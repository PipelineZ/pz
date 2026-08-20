using System.Text.Json.Nodes;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary>Runs the TestKit sink acceptance contract against the HTTP sink, with a
/// StubHttpServer standing in for the destination. "Committed" for an HTTP destination means
/// "requests the server confirmed": append outputs replay every confirmed POST row in order;
/// merge outputs apply last-writer-wins per key over the confirmed PUTs (exactly the
/// idempotent-destination semantics the guarantee matrix assigns keyed merge).</summary>
public sealed class HttpSinkAcceptance : SinkConnectorAcceptanceTests, IDisposable
{
    private readonly StubHttpServer _server = new();

    public HttpSinkAcceptance()
    {
        _server.Map("/ingest", _ => new StubResponse(200, "{}"));
        _server.MapPrefix("/items/", _ => new StubResponse(200, "{}"));
    }

    public void Dispose() => _server.DisposeAsync().AsTask().GetAwaiter().GetResult();

    protected override ISinkConnector CreateSink() => new HttpConnector();

    protected override ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["base_url"] = _server.BaseUrl.ToString(),
    });

    protected override OutputSpec SmallOutput => new("api", "small", "append", "fail_on_change",
        new Dictionary<string, object?> { ["path"] = "/ingest", ["rows_per_request"] = 25 });

    protected override OutputSpec? MergeOutput => new("api", "merge-out", "merge", "fail_on_change",
        new Dictionary<string, object?> { ["path"] = "/items/{key}" }) { Keys = ["id"] };

    protected override OutputSpec? CheckpointOutput => new("api", "ckpt", "append", "fail_on_change",
        new Dictionary<string, object?> { ["path"] = "/ingest", ["rows_per_request"] = 25 });

    protected override ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec)
    {
        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: false),
        ], null);

        var batches = new List<RecordBatch>();
        if (spec.Mode == "merge")
        {
            var lastByKey = new Dictionary<long, (long Id, string Name)>();
            foreach (var request in _server.Requests.Where(r => r.Method == "PUT"))
            {
                var row = JsonNode.Parse(request.Body)!;
                var id = row["id"]!.GetValue<long>();
                lastByKey[id] = (id, row["name"]!.GetValue<string>());
            }

            var builder = new ArrowBatchBuilder(schema);
            foreach (var (id, name) in lastByKey.Values.OrderBy(v => v.Id))
            {
                builder.AppendRow([id, name]);
            }

            if (builder.Flush() is { } merged)
            {
                batches.Add(merged);
            }
        }
        else
        {
            foreach (var request in _server.Requests.Where(r => r.Method == "POST"))
            {
                var rows = (JsonArray)JsonNode.Parse(request.Body)!;
                if (rows.Count == 0)
                {
                    continue;
                }

                var builder = new ArrowBatchBuilder(schema);
                foreach (var row in rows)
                {
                    builder.AppendRow([row!["id"]!.GetValue<long>(), row["name"]!.GetValue<string>()]);
                }

                batches.Add(builder.Flush()!);
            }
        }

        return ValueTask.FromResult<IReadOnlyList<RecordBatch>>(batches);
    }
}
