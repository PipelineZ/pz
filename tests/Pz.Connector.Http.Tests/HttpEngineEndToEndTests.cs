using System.Text.Json.Nodes;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Resilience;

namespace Pz.Connector.Http.Tests;

/// <summary>The connector-through-engine tier: the REAL HttpConnector
/// sink driven by the REAL SinkWriteExecutor/KindDispatchingExecutor over a real staging DuckDB,
/// with StubHttpServer as the destination. Engine unit tests prove these paths against synthetic
/// sinks; the HTTP suite proves the connector against the stub in isolation; this file is where
/// the two compose.</summary>
public sealed class HttpEngineEndToEndTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-http-e2e-tests", Guid.NewGuid().ToString("N"));
    private readonly StubHttpServer _server = new();
    private DuckSession _duck = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        // Unique ids => `order by all` is a total order => the checkpoint drain is deterministic.
        await _duck.ExecuteAsync("create table staging.stg_orders as select range as id, 'row-' || range as name from range(200)");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        await _server.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    private RunContext Context() => new(
        _duck, Registry(), new RunPaths(_dir, "test-run"), NullRunEvents.Instance,
        // MaxRowsPerBatch is inert for SinkWriteExecutor's drain (it batches by
        // EffectiveBatch.TargetBatchBytes, not row count -- 200 tiny rows land in one DuckDB
        // batch); set anyway to document intent. What actually matters for the chunk-level
        // assertions below is rows_per_request: 5 on the output, which HttpWriteSession applies
        // per WriteBatchAsync call regardless of how many rows that call carries.
        Batch: new BatchOptions(MaxRowsPerBatch: 100));

    private ConnectorRegistry Registry()
    {
        var reg = new ConnectorRegistry();
        reg.AddSink("http", new HttpConnector());
        return reg;
    }

    private DagNode SinkNode()
    {
        var sink = new ConnectionDef("api", "http",
            new Dictionary<string, object?> { ["base_url"] = _server.BaseUrl.ToString() }, [],
            "sinks/api.yml") { Outputs = [new OutputDef("out", "stg_orders", "append", "fail_on_change",
                new Dictionary<string, object?> { ["path"] = "/ingest", ["rows_per_request"] = 5 })] };
        return new DagNode(new NodeId("ffffffffffffffff"), NodeKind.SinkWrite, "api.out",
            [], null, new SinkOutputDef(sink, sink.Outputs[0]));
    }

    private static KindDispatchingExecutor TwoAttempts() =>
        new(new RetryPolicy(MaxAttempts: 2, BaseDelay: TimeSpan.Zero, MaxDelay: TimeSpan.Zero),
            delay: (_, _) => Task.CompletedTask);

    /// <summary>Every id confirmed by a 2xx POST, in confirmation order. Only valid when every
    /// mapped response is 2xx (the stub records the request body regardless of the status the
    /// handler returns) -- see <see cref="Retry_resumes_past_the_acknowledged_prefix_end_to_end"/>
    /// for the failure-mixed case, which tracks confirmed ids itself instead.</summary>
    private List<long> ConfirmedIds() =>
        _server.Requests.Where(r => r.Method == "POST" && !string.IsNullOrEmpty(r.Body))
            .SelectMany(r => ((JsonArray)JsonNode.Parse(r.Body)!).Select(row => row!["id"]!.GetValue<long>()))
            .ToList();

    [Fact]
    public async Task Append_delivers_all_rows_through_the_engine()
    {
        _server.Map("/ingest", _ => new StubResponse(200, "{}"));
        var ctx = Context();

        var result = await TwoAttempts().ExecuteAsync(SinkNode(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(200, result.RowsMoved);
        Assert.Equal(Enumerable.Range(0, 200).Select(i => (long)i), ConfirmedIds());
    }

    [Fact]
    public async Task Failed_attempt_reports_chunk_level_acknowledgment()
    {
        // End-to-end: HTTP confirms rows chunk-by-chunk INSIDE a batch
        // (5-row requests). The stub confirms 3 chunks (rows 0..14) then fails every later POST
        // starting with request #4 (rows 15..19); honesty must report 15 rows visible (a
        // mid-batch count no batch-boundary poll could produce) and the ledger must persist the
        // same 15.
        var posts = 0;
        _server.Map("/ingest", _ => ++posts <= 3 ? new StubResponse(200, "{}") : new StubResponse(500, "{}"));
        var ctx = Context();
        var node = SinkNode();

        await Assert.ThrowsAsync<PzConnectorException>(
            () => new SinkWriteExecutor().ExecuteAsync(node, ctx, default));

        Assert.True(ctx.DeliveryFailures.TryGetValue(node.Id, out var stats));
        Assert.Equal(15, stats!.RowsVisible);
        var persisted = await SinkDeliveryLedger.ReadAsync(_duck, node.Id.Value, default);
        Assert.NotNull(persisted);
        Assert.Equal(15, persisted!.AcknowledgedRows);
    }

    [Fact]
    public async Task Retry_resumes_past_the_acknowledged_prefix_end_to_end()
    {
        // One 500 on the 10th POST (rows 45..49), then healthy. Attempt 1 dies with 45 rows
        // acknowledged (chunks 1..9, 5 rows each); attempt 2 must resume at row 45 -- the 200
        // confirmed rows arrive exactly once each (the failed, unconfirmed chunk is re-sent; the
        // acknowledged prefix is not).
        //
        // Confirmed ids are tracked from inside the stub handler itself, not by re-parsing every
        // captured request after the fact: StubRequest records a request's body the moment it
        // arrives, regardless of what status the handler goes on to return, so the 10th request
        // (rows 45..49, which THIS handler fails) would double-count those five ids if "confirmed"
        // were derived by scanning _server.Requests for POSTs with a body -- it cannot distinguish
        // "sent" from "acknowledged" after the fact. Recording only on the success branch, at the
        // point the handler decides to return 200, is what makes "confirmed" mean "acknowledged".
        var posts = 0;
        var confirmedIds = new List<long>();
        _server.Map("/ingest", req =>
        {
            if (++posts == 10)
            {
                return new StubResponse(500, "{}");
            }

            var ids = ((JsonArray)JsonNode.Parse(req.Body)!).Select(row => row!["id"]!.GetValue<long>());
            confirmedIds.AddRange(ids);
            return new StubResponse(200, "{}");
        });
        var ctx = Context();
        var node = SinkNode();

        var result = await TwoAttempts().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(200, result.RowsMoved);
        Assert.Equal(200, confirmedIds.Count); // no duplicates: the acked prefix was never re-sent
        Assert.Equal(Enumerable.Range(0, 200).Select(i => (long)i), confirmedIds.OrderBy(i => i));
        Assert.NotNull(result.Delivery);
        Assert.Equal(45, result.Delivery!.ResumedRows);
        Assert.Null(await SinkDeliveryLedger.ReadAsync(_duck, node.Id.Value, default)); // cleared post-commit
    }
}
