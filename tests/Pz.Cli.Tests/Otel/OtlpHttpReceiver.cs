using System.IO.Compression;
using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Proto.Collector.Metrics.V1;
using OpenTelemetry.Proto.Collector.Trace.V1;

namespace Pz.Cli.Tests.Otel;

/// <summary>A loopback OTLP/HTTP (protobuf) receiver for tests: <c>POST /traces</c> and <c>POST /metrics</c>, each
/// recorded with its Authorization header, Content-Encoding and on-the-wire size, the body gunzipped when needed and
/// parsed. <see cref="StatusCode"/> lets a test answer with an error (413, for example).</summary>
internal sealed class OtlpHttpReceiver : IAsyncDisposable
{
    internal sealed record Received(
        string Path, string? Authorization, string? Encoding, long BodyBytes, long? ContentLength,
        ExportTraceServiceRequest? Traces, ExportMetricsServiceRequest? Metrics);

    private readonly Lock _gate = new();
    private readonly List<Received> _requests = [];
    private WebApplication? _app;

    public Uri TracesUrl { get; private set; } = null!;

    public Uri MetricsUrl { get; private set; } = null!;

    public Func<int> StatusCode { get; set; } = () => 200;

    /// <summary>When set, every request waits on this before answering, so a test can play a backend that never does.</summary>
    public Task? Hold { get; set; }

    public IReadOnlyList<Received> Requests { get { lock (_gate) { return [.. _requests]; } } }

    public async Task StartAsync()
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(k => k.Listen(IPAddress.Loopback, 0));
        _app = builder.Build();
        _app.MapPost("/traces", context => Accept(context, traces: true));
        _app.MapPost("/metrics", context => Accept(context, traces: false));
        await _app.StartAsync();
        var address = new Uri(_app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First());
        TracesUrl = new Uri(address, "/traces");
        MetricsUrl = new Uri(address, "/metrics");
    }

    private async Task Accept(HttpContext context, bool traces)
    {
        if (Hold is { } hold) await hold.WaitAsync(context.RequestAborted);
        using var raw = new MemoryStream();
        await context.Request.Body.CopyToAsync(raw);
        var encoding = context.Request.Headers.ContentEncoding.ToString();
        var body = raw.ToArray();
        if (encoding == "gzip")
        {
            using var unzipped = new MemoryStream();
            using (var gzip = new GZipStream(new MemoryStream(body), CompressionMode.Decompress)) gzip.CopyTo(unzipped);
            body = unzipped.ToArray();
        }

        var received = new Received(
            context.Request.Path,
            context.Request.Headers.Authorization.Count == 0 ? null : context.Request.Headers.Authorization.ToString(),
            encoding.Length == 0 ? null : encoding,
            raw.Length,
            context.Request.ContentLength,
            traces ? ExportTraceServiceRequest.Parser.ParseFrom(body) : null,
            traces ? null : ExportMetricsServiceRequest.Parser.ParseFrom(body));
        lock (_gate) _requests.Add(received);
        context.Response.StatusCode = StatusCode();
    }

    public async ValueTask DisposeAsync()
    {
        if (_app is null) return;
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
