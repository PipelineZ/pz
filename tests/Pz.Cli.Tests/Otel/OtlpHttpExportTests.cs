using Pz.Cli.Otel;
using Pz.Diagnostics.Otel;

namespace Pz.Cli.Tests.Otel;

/// <summary>ActivitySource/Meter listeners are process-global, so OTel export tests never run beside each other.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class OtelGlobalCollection
{
    public const string Name = "OtelGlobal";
}

[Collection(OtelGlobalCollection.Name)]
public sealed class OtlpHttpExportTests : IAsyncLifetime
{
    private readonly OtlpHttpReceiver _receiver = new();
    private readonly string _headers = Path.GetTempFileName();

    public Task InitializeAsync() => _receiver.StartAsync();

    public async Task DisposeAsync()
    {
        await _receiver.DisposeAsync();
        File.Delete(_headers);
    }

    private OtelOptions Options(bool traces = true, bool metrics = true) => new(
        OtelProtocol.HttpProtobuf, null, traces ? _receiver.TracesUrl : null, metrics ? _receiver.MetricsUrl : null, _headers);

    [Fact]
    public async Task Traces_and_metrics_go_to_their_own_urls_with_the_files_header()
    {
        await File.WriteAllTextAsync(_headers, "Authorization=Bearer abc\n");
        await using (OtelProviders.Create(Options(), _ => { }))
        {
            using (PzActivitySource.Instance.StartActivity("run")) { }
            PzMeters.RowsMoved.Add(5);
        }

        var traces = Assert.Single(_receiver.Requests, r => r.Path == "/traces");
        Assert.Equal("Bearer abc", traces.Authorization);
        Assert.Equal("gzip", traces.Encoding);
        Assert.Equal(traces.BodyBytes, traces.ContentLength);
        Assert.Contains(traces.Traces!.ResourceSpans.SelectMany(r => r.ScopeSpans).SelectMany(s => s.Spans), s => s.Name == "run");
        var metrics = Assert.Single(_receiver.Requests, r => r.Path == "/metrics");
        Assert.Equal("Bearer abc", metrics.Authorization);
    }

    [Fact]
    public async Task Http_metrics_are_delta_with_an_exponential_node_duration()
    {
        await using (OtelProviders.Create(Options(traces: false), _ => { }))
        {
            PzMeters.RowsMoved.Add(5);
            PzMeters.NodeDuration.Record(12.5);
        }

        var all = _receiver.Requests.Where(r => r.Metrics is not null)
            .SelectMany(r => r.Metrics!.ResourceMetrics).SelectMany(r => r.ScopeMetrics).SelectMany(s => s.Metrics).ToList();
        var rows = all.Single(m => m.Name == "pz.rows_moved");
        Assert.Equal(OpenTelemetry.Proto.Metrics.V1.AggregationTemporality.Delta, rows.Sum.AggregationTemporality);
        Assert.NotNull(all.Single(m => m.Name == "pz.node.duration").ExponentialHistogram);
    }

    [Fact]
    public async Task A_traces_only_run_sends_no_metrics()
    {
        await using (OtelProviders.Create(Options(metrics: false), _ => { }))
        {
            using (PzActivitySource.Instance.StartActivity("run")) { }
            PzMeters.RowsMoved.Add(5);
        }
        Assert.DoesNotContain(_receiver.Requests, r => r.Path == "/metrics");
    }

    [Fact]
    public async Task Many_spans_never_make_a_request_over_one_megabyte()
    {
        await using (OtelProviders.Create(Options(metrics: false), _ => { }))
        {
            // 2000 stays under the batch processor's default 2048-span queue, so nothing is dropped and the
            // export takes eight requests of at most 256 spans.
            for (var i = 0; i < 2000; i++)
                using (var a = PzActivitySource.Instance.StartActivity("node.SourceLoad")) a?.SetTag("pad", new string('x', 1024));
        }
        Assert.All(_receiver.Requests, r => Assert.True(r.BodyBytes <= 1_048_576, $"{r.BodyBytes} bytes"));
        Assert.True(_receiver.Requests.Count >= 8, $"{_receiver.Requests.Count} requests");
        Assert.Equal(2000, _receiver.Requests.Sum(r => r.Traces!.ResourceSpans.SelectMany(x => x.ScopeSpans).Sum(s => s.Spans.Count)));
    }

    [Fact]
    public async Task A_413_does_not_stall_the_flush()
    {
        _receiver.StatusCode = () => 413;
        var watch = System.Diagnostics.Stopwatch.StartNew();
        await using (OtelProviders.Create(Options(), _ => { }))
        {
            using (PzActivitySource.Instance.StartActivity("run")) { }
            PzMeters.RowsMoved.Add(5);
        }
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(15), $"flush took {watch.Elapsed}");
        Assert.Contains(_receiver.Requests, r => r.Path == "/traces");
    }

    [Fact]
    public async Task A_backend_that_never_answers_cannot_stall_the_flush_past_the_export_timeout()
    {
        var never = new TaskCompletionSource();
        _receiver.Hold = never.Task;
        var watch = System.Diagnostics.Stopwatch.StartNew();
        var flush = Task.Run(async () =>
        {
            await using (OtelProviders.Create(Options(), _ => { }, exportTimeout: TimeSpan.FromMilliseconds(500)))
            {
                using (PzActivitySource.Instance.StartActivity("run")) { }
                PzMeters.RowsMoved.Add(5);
            }
        });
        await flush.WaitAsync(TimeSpan.FromSeconds(30));
        never.TrySetResult();
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(10), $"flush took {watch.Elapsed}");
    }

    [Fact]
    public async Task A_missing_headers_file_is_noted_once_per_run_not_once_per_signal()
    {
        File.Delete(_headers);
        var notes = new List<string>();
        await using (OtelProviders.Create(Options(), notes.Add))
        {
            using (PzActivitySource.Instance.StartActivity("run")) { }
            PzMeters.RowsMoved.Add(5);
        }
        Assert.Single(notes);
    }

    [Fact]
    public async Task Grpc_keeps_the_sdk_default_span_batch()
    {
        // The 256 cap is for Azure Monitor's 1 MB HTTP limit; gRPC stays as before 0.9.1.
        Assert.Equal(512, OtelProviders.SpanBatchSize(OtelProtocol.Grpc));
        Assert.Equal(OtelProviders.MaxSpanBatch, OtelProviders.SpanBatchSize(OtelProtocol.HttpProtobuf));
        await Task.CompletedTask;
    }
}
