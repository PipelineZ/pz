using System.Net;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Proto.Collector.Metrics.V1;
using OpenTelemetry.Proto.Collector.Trace.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Trace.V1;

namespace Pz.PackageManagement.Tests.Otlp;

/// <summary>A loopback OTLP/grpc collector for tests: accepts trace and metric exports and keeps what
/// arrived. Waiting is gate-based (a TCS reset per export), never a poll-and-sleep.</summary>
internal sealed class OtlpReceiver : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly Store _store;

    private OtlpReceiver(WebApplication app, Store store, Uri endpoint)
    {
        _app = app;
        _store = store;
        Endpoint = endpoint;
    }

    public Uri Endpoint { get; }

    public IReadOnlyList<ResourceSpans> ResourceSpans => _store.SnapshotSpans();

    public IReadOnlyList<Span> Spans => ResourceSpans.SelectMany(r => r.ScopeSpans).SelectMany(s => s.Spans).ToArray();

    public IReadOnlyList<ResourceMetrics> ResourceMetrics => _store.SnapshotMetrics();

    public static async Task<OtlpReceiver> StartAsync()
    {
        var store = new Store();
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(k => k.Listen(IPAddress.Loopback, 0, l => l.Protocols = HttpProtocols.Http2));
        builder.Services.AddGrpc();
        builder.Services.AddSingleton(store);
        var app = builder.Build();
        app.MapGrpcService<TraceSink>();
        app.MapGrpcService<MetricsSink>();
        await app.StartAsync();
        var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return new OtlpReceiver(app, store, new Uri(address));
    }

    /// <summary>Waits until <paramref name="ready"/> accepts what has arrived. A timeout throws a
    /// <see cref="TimeoutException"/> naming every span received so far plus whatever
    /// <paramref name="diagnostics"/> returns (a test passes the connector's stderr tail), so a failure
    /// says what the child actually did rather than only that it never exported.</summary>
    public async Task<IReadOnlyList<Span>> WaitForSpansAsync(
        Func<IReadOnlyList<Span>, bool> ready, TimeSpan timeout, Func<string>? diagnostics = null)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (true)
        {
            var changed = _store.Changed;
            var spans = Spans;
            if (ready(spans))
            {
                return spans;
            }

            try
            {
                await changed.WaitAsync(cts.Token);
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException(
                    $"no matching span export within {timeout}; spans so far: [{string.Join(", ", Spans.Select(s => s.Name))}]"
                    + Describe(diagnostics));
            }
        }
    }

    public async Task<IReadOnlyList<ResourceMetrics>> WaitForMetricsAsync(
        Func<IReadOnlyList<ResourceMetrics>, bool> ready, TimeSpan timeout, Func<string>? diagnostics = null)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (true)
        {
            var changed = _store.Changed;
            var metrics = ResourceMetrics;
            if (ready(metrics))
            {
                return metrics;
            }

            try
            {
                await changed.WaitAsync(cts.Token);
            }
            catch (OperationCanceledException)
            {
                var names = ResourceMetrics.SelectMany(r => r.ScopeMetrics).SelectMany(s => s.Metrics).Select(m => m.Name);
                throw new TimeoutException(
                    $"no matching metric export within {timeout}; instruments so far: [{string.Join(", ", names)}]"
                    + Describe(diagnostics));
            }
        }
    }

    private static string Describe(Func<string>? diagnostics)
    {
        var text = diagnostics?.Invoke();
        return string.IsNullOrWhiteSpace(text) ? string.Empty : Environment.NewLine + "connector stderr:" + Environment.NewLine + text;
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }

    internal sealed class Store
    {
        private readonly Lock _gate = new();
        private readonly List<ResourceSpans> _spans = [];
        private readonly List<ResourceMetrics> _metrics = [];
        private TaskCompletionSource _changed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Changed { get { lock (_gate) { return _changed.Task; } } }

        public void Add(IEnumerable<ResourceSpans> spans)
        {
            lock (_gate)
            {
                _spans.AddRange(spans);
                Signal();
            }
        }

        public void Add(IEnumerable<ResourceMetrics> metrics)
        {
            lock (_gate)
            {
                _metrics.AddRange(metrics);
                Signal();
            }
        }

        public IReadOnlyList<ResourceSpans> SnapshotSpans() { lock (_gate) { return [.. _spans]; } }

        public IReadOnlyList<ResourceMetrics> SnapshotMetrics() { lock (_gate) { return [.. _metrics]; } }

        private void Signal()
        {
            var previous = _changed;
            _changed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            previous.TrySetResult();
        }
    }

    private sealed class TraceSink(Store store) : TraceService.TraceServiceBase
    {
        public override Task<ExportTraceServiceResponse> Export(ExportTraceServiceRequest request, ServerCallContext context)
        {
            store.Add(request.ResourceSpans);
            return Task.FromResult(new ExportTraceServiceResponse());
        }
    }

    private sealed class MetricsSink(Store store) : MetricsService.MetricsServiceBase
    {
        public override Task<ExportMetricsServiceResponse> Export(ExportMetricsServiceRequest request, ServerCallContext context)
        {
            store.Add(request.ResourceMetrics);
            return Task.FromResult(new ExportMetricsServiceResponse());
        }
    }
}
