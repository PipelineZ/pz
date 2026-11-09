using System.IO.Compression;
using System.Text;
using System.Text.Json.Nodes;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

public sealed class HttpForEachFileTests : IAsyncLifetime
{
    private readonly StubHttpServer _server = new();
    private readonly List<string> _notices = [];

    // This class's own scratch root: other test classes download into the shared temp dir in parallel.
    private readonly string _scratchRoot =
        Path.Combine(Path.GetTempPath(), "pz-for-each-file-tests", Guid.NewGuid().ToString("N"));

    public HttpForEachFileTests() => Directory.CreateDirectory(_scratchRoot);

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await _server.DisposeAsync();
        Directory.Delete(_scratchRoot, recursive: true);
    }

    private sealed class RecordingGate : IOperationGate
    {
        public List<(string Label, bool Idempotent)> Calls { get; } = [];

        public Task<T> ExecuteAsync<T>(string opLabel, bool idempotent, Func<CancellationToken, Task<T>> op,
            CancellationToken ct)
        {
            lock (Calls)
            {
                Calls.Add((opLabel, idempotent));
            }

            return op(ct);
        }

        public void ReportBudget(int remaining, DateTimeOffset resetAt)
        {
        }
    }

    private HttpSource Source()
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = _server.BaseUrl.ToString(),
        }), errors);
        Assert.Empty(errors);
        var source = new HttpSource(connection!, TimeProvider.System);
        source.UseNotice(_notices.Add);
        return source;
    }

    /// <summary>Reads the dataset's single partition with scratch under this class's root;
    /// <paramref name="onBatch"/> runs after each batch's rows are taken.</summary>
    private async Task<List<object?[]>> Read(DatasetSpec spec, Action? onBatch = null, IOperationGate? gate = null,
        CancellationToken ct = default)
    {
        await using var source = Source();
        if (gate is not null)
        {
            source.UseOperationGate(gate);
        }

        var schema = (await source.GetSchemaAsync(spec, CancellationToken.None)).Schema;
        var partition = Assert.IsType<HttpForEachPartition>(
            Assert.Single(await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None)));
        partition.ScratchRoot = _scratchRoot;
        var rows = new List<object?[]>();
        await foreach (var batch in partition.ReadAsync(new BatchOptions(TargetBatchBytes: 1, MaxRowsPerBatch: 1), ct))
        {
            using (batch)
            {
                Assert.Equal(schema.FieldsList.Select(f => f.Name), batch.Schema.FieldsList.Select(f => f.Name));
                for (var r = 0; r < batch.Length; r++)
                {
                    var row = new object?[batch.ColumnCount];
                    for (var c = 0; c < batch.ColumnCount; c++)
                    {
                        row[c] = FileReaderRows.Value(batch.Column(c), r);
                    }

                    rows.Add(row);
                }
            }

            onBatch?.Invoke();
        }

        return rows;
    }

    private static DatasetSpec Spec(Dictionary<string, object?> options) => new("s", "d", options);

    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    /// <summary>The spec's list-of-exports example: list /exports, download /exports/{id}/file as CSV.</summary>
    private static Dictionary<string, object?> ExportsOptions(Dictionary<string, object?> file,
        Dictionary<string, object?>? forEachExtra = null)
    {
        var forEach = Map(("path", "/exports"), ("items", "/exports"), ("fields", Map(("export_id", "/id"))));
        foreach (var (k, v) in forEachExtra ?? [])
        {
            forEach[k] = v;
        }

        return Map(("for_each", forEach), ("path", "/exports/{{ item.export_id }}/file"), ("file", file),
            ("columns", new Dictionary<string, string> { ["export_id"] = "varchar", ["name"] = "varchar", ["n"] = "int" }));
    }

    private void MapExports(params string[] ids)
        => _server.Map("/exports", _ => new StubResponse(200,
            new JsonObject { ["exports"] = new JsonArray([.. ids.Select(id => (JsonNode)new JsonObject { ["id"] = id })]) }
                .ToJsonString()));

    private static StubResponse File(byte[] bytes, string contentType = "application/octet-stream")
        => new(200, "", new Dictionary<string, string> { ["Content-Type"] = contentType }) { RawBody = bytes };

    private static StubResponse File(string text) => File(Encoding.UTF8.GetBytes(text));

    private static byte[] Zip(params (string Name, string Text)[] entries)
    {
        var buffer = new MemoryStream();
        using (var zip = new ZipArchive(buffer, ZipArchiveMode.Create, leaveOpen: true))
        {
            foreach (var (name, text) in entries)
            {
                using var w = new StreamWriter(zip.CreateEntry(name).Open());
                w.Write(text);
            }
        }

        return buffer.ToArray();
    }

    private string[] ScratchDirs() => Directory.GetDirectories(_scratchRoot, "pz-http-*");

    [Fact]
    public async Task Exports_list_then_download_each()
    {
        MapExports("e1", "e2");
        _server.MapPrefix("/exports/", r => r.Url.AbsolutePath switch
        {
            "/exports/e1/file" => File("name,n\na,1\nb,2\n"),
            "/exports/e2/file" => File("name,n\nc,3\n"),
            _ => new StubResponse(500, "unexpected"),
        });

        var rows = await Read(Spec(ExportsOptions(Map(("format", "csv")))));

        Assert.Equal(
            [["e1", "a", 1], ["e1", "b", 2], ["e2", "c", 3]],
            rows.Select(r => r.Take(3).ToArray()).ToList());
        Assert.Equal(["/exports", "/exports/e1/file", "/exports/e2/file"],
            _server.Requests.Select(r => r.Url.AbsolutePath).ToList());
        Assert.Empty(_notices);
    }

    [Fact]
    public async Task Zip_items_with_members()
    {
        MapExports("e1", "e2");
        _server.MapPrefix("/exports/", r => r.Url.AbsolutePath switch
        {
            "/exports/e1/file" => File(Zip(("part-1.csv", "name,n\na,1\n"), ("readme.txt", "ignore me"),
                ("part-2.csv", "name,n\nb,2\n")), "application/zip"),
            "/exports/e2/file" => File(Zip(("part-1.csv", "name,n\nc,3\n"), ("other.txt", "x")), "application/zip"),
            _ => new StubResponse(500, "unexpected"),
        });

        var options = ExportsOptions(Map(("format", "csv"), ("archive", "zip"), ("members", "part-*.csv"),
            ("file_column", "src")));
        var rows = await Read(Spec(options));

        // file_column stays per downloaded member, item columns per item.
        Assert.Equal(
            [["e1", "a", 1, "part-1.csv"], ["e1", "b", 2, "part-2.csv"], ["e2", "c", 3, "part-1.csv"]],
            rows.Select(r => r.Take(4).ToArray()).ToList());
    }

    [Fact]
    public async Task Item_download_404_skipped()
    {
        MapExports("e1", "gone", "e3");
        _server.MapPrefix("/exports/", r => r.Url.AbsolutePath switch
        {
            "/exports/e1/file" => File("name,n\na,1\n"),
            "/exports/gone/file" => new StubResponse(404, """{"error":"no such export"}"""),
            "/exports/e3/file" => File("name,n\nc,3\n"),
            _ => new StubResponse(500, "unexpected"),
        });

        var rows = await Read(Spec(ExportsOptions(Map(("format", "csv")), Map(("on_missing", "skip")))));

        Assert.Equal([["e1", "a", 1], ["e3", "c", 3]], rows.Select(r => r.Take(3).ToArray()).ToList());
        Assert.Equal(["http source 's.d': skipped 1 items that returned 404/410 (first: item export_id=gone)"],
            _notices);
        Assert.Empty(ScratchDirs());
    }

    [Fact]
    public async Task Item_download_404_fails_by_default_with_item_identity()
    {
        MapExports("gone");
        _server.MapPrefix("/exports/", _ => new StubResponse(404, "nope"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => Read(Spec(ExportsOptions(Map(("format", "csv"))))));

        Assert.False(ex.IsTransient);
        Assert.Contains("404", ex.Message);
        Assert.EndsWith("(item export_id=gone)", ex.Message);
        Assert.Empty(ScratchDirs());
    }

    [Fact]
    public async Task Temp_dirs_removed_per_item()
    {
        MapExports("e1", "e2", "bad");
        _server.MapPrefix("/exports/", r => r.Url.AbsolutePath switch
        {
            "/exports/e1/file" => File("name,n\na,1\nb,2\n"),
            "/exports/e2/file" => File("name,n\nc,3\n"),
            "/exports/bad/file" => File("name,n\nd,4\ne,not-a-number\n"),
            _ => new StubResponse(500, "unexpected"),
        });

        // At every batch at most one scratch dir exists, the current item's; earlier items' are gone. An
        // item's last batch is yielded after the item's read has ended (the for_each read looks one batch
        // ahead to place the item-boundary checkpoint), so its dir is already gone by then, and the bad
        // item's first row is held back by that look-ahead and fails with it.
        var seen = new List<string[]>();
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(ExportsOptions(Map(("format", "csv")))),
            onBatch: () => seen.Add(ScratchDirs())));

        Assert.EndsWith("(item export_id=bad)", ex.Message);
        Assert.Equal([1, 0, 0], seen.Select(dirs => dirs.Length));
        Assert.Empty(ScratchDirs());
    }

    [Fact]
    public async Task Raw_file_items_carry_pz_item_and_pz_file_per_item()
    {
        MapExports("e1", "e2");
        _server.MapPrefix("/exports/", r => r.Url.AbsolutePath switch
        {
            "/exports/e1/file" => File(Zip(("a.csv", "name\nx\ny\n")), "application/zip"),
            "/exports/e2/file" => File(Zip(("b.csv", "name\nz\n")), "application/zip"),
            _ => new StubResponse(500, "unexpected"),
        });
        var options = ExportsOptions(Map(("format", "csv"), ("archive", "zip"), ("members", "*.csv")));
        options.Remove("columns");

        var rows = await Read(Spec(options));

        // payload, pz_file, pz_row, pz_fetched_at, pz_item
        Assert.Equal(
            [
                ("a.csv", """{"export_id":"e1"}"""),
                ("a.csv", """{"export_id":"e1"}"""),
                ("b.csv", """{"export_id":"e2"}"""),
            ],
            rows.Select(r => ((string)r[1]!, (string)r[4]!)).ToList());
        Assert.Equal(["x", "y", "z"], rows.Select(r => JsonNode.Parse((string)r[0]!)!["name"]!.GetValue<string>()));
    }

    [Fact]
    public async Task Each_item_download_goes_through_the_gate()
    {
        MapExports("e1", "e2");
        _server.MapPrefix("/exports/", _ => File("name,n\na,1\n"));
        var gate = new RecordingGate();

        await Read(Spec(ExportsOptions(Map(("format", "csv")))), gate: gate);

        Assert.Equal([("http.get_page", true), ("http.download", true), ("http.download", true)], gate.Calls);
    }

    [Fact]
    public async Task Cancelled_mid_file_item_removes_its_scratch()
    {
        MapExports("e1", "e2");
        _server.MapPrefix("/exports/", _ => File("name,n\na,1\nb,2\nc,3\nd,4\n"));
        using var cts = new CancellationTokenSource();
        var seen = new List<int>();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Read(Spec(ExportsOptions(Map(("format", "csv")))),
            onBatch: () =>
            {
                seen.Add(ScratchDirs().Length);
                cts.Cancel();
            },
            ct: cts.Token));

        // Cancelled while e1's file was still open, and nothing of it is left behind.
        Assert.Equal([1], seen);
        Assert.Empty(ScratchDirs());
        Assert.DoesNotContain(_server.Requests, r => r.Url.AbsolutePath.Contains("e2"));
    }
}
