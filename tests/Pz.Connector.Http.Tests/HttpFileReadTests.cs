using System.IO.Compression;
using System.Text;
using Apache.Arrow;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Xunit;

namespace Pz.Connector.Http.Tests;

public sealed class HttpFileReadTests : IAsyncLifetime
{
    private const int MiB = 1024 * 1024;
    private static readonly DateTimeOffset Now = new(2026, 11, 8, 6, 0, 0, TimeSpan.Zero);

    private readonly StubHttpServer _server = new();
    private readonly ManualClock _clock = new();
    private readonly List<string> _notices = [];

    public HttpFileReadTests() => _clock.Advance(Now - _clock.GetUtcNow());

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync() => await _server.DisposeAsync();

    private HttpSource Source(Dictionary<string, object?>? extra = null)
    {
        var values = new Dictionary<string, object?> { ["base_url"] = _server.BaseUrl.ToString() };
        foreach (var (key, value) in extra ?? [])
        {
            values[key] = value;
        }

        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(values), errors);
        Assert.Empty(errors);
        var source = new HttpSource(connection!, _clock);
        source.UseNotice(_notices.Add);
        return source;
    }

    private static DatasetSpec Spec(string dataset, Dictionary<string, object?> options) => new("stub", dataset, options);

    private static Dictionary<string, object?> FileOptions(string path, Dictionary<string, object?> file,
        Dictionary<string, string>? columns = null)
    {
        var options = new Dictionary<string, object?> { ["path"] = path, ["file"] = file };
        if (columns is not null)
        {
            options["columns"] = columns;
        }

        return options;
    }

    private async Task<List<object?[]>> Read(DatasetSpec spec, HttpSource? source = null, BatchOptions? options = null)
    {
        var owned = source is null;
        source ??= Source();
        try
        {
            var schema = (await source.GetSchemaAsync(spec, CancellationToken.None)).Schema;
            var rows = new List<object?[]>();
            var partition = Assert.Single(await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));
            await foreach (var batch in partition.ReadAsync(options ?? new BatchOptions(), CancellationToken.None))
            {
                using (batch)
                {
                    AssertSameSchema(schema, batch.Schema);
                    rows.AddRange(Rows(batch));
                }
            }

            return rows;
        }
        finally
        {
            if (owned)
            {
                await source.DisposeAsync();
            }
        }
    }

    private static void AssertSameSchema(Schema expected, Schema actual)
    {
        Assert.Equal(expected.FieldsList.Select(f => f.Name), actual.FieldsList.Select(f => f.Name));
        Assert.Equal(expected.FieldsList.Select(f => f.DataType.TypeId), actual.FieldsList.Select(f => f.DataType.TypeId));
    }

    private static IEnumerable<object?[]> Rows(RecordBatch batch)
    {
        for (var r = 0; r < batch.Length; r++)
        {
            var row = new object?[batch.ColumnCount];
            for (var c = 0; c < batch.ColumnCount; c++)
            {
                row[c] = FileReaderRows.Value(batch.Column(c), r);
            }

            yield return row;
        }
    }

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

    private static byte[] Gzip(string text)
    {
        var buffer = new MemoryStream();
        using (var gz = new GZipStream(buffer, CompressionLevel.Fastest, leaveOpen: true))
        {
            gz.Write(Encoding.UTF8.GetBytes(text));
        }

        return buffer.ToArray();
    }

    private static StubResponse File(byte[] bytes, string contentType = "application/octet-stream")
        => new(200, "", new Dictionary<string, string> { ["Content-Type"] = contentType }) { RawBody = bytes };

    private static StubResponse File(string text, string contentType = "application/octet-stream")
        => File(Encoding.UTF8.GetBytes(text), contentType);

    private static HashSet<string> TempDirs()
        => [.. Directory.GetDirectories(Path.GetTempPath(), "pz-http-*")];

    [Fact]
    public async Task Users_daily_zip_three_datasets()
    {
        var zip = Zip(
            ("sessions_2026-11-07.csv",
                "day,session_id,started_at,duration_s\n" +
                "2026-11-07,s1,2026-11-07T10:00:00Z,30\n" +
                "2026-11-07,s2,2026-11-07T11:30:00Z,45\n"),
            ("events.jsonl", "{\"e\":\"click\",\"n\":1}\n{\"e\":\"view\",\"n\":2}\n"),
            ("campaigns.xml",
                "<report><campaigns>" +
                "<campaign id=\"7\"><name>Autumn</name><budget>12.50</budget></campaign>" +
                "<campaign id=\"8\"><name>Winter</name><budget>3</budget></campaign>" +
                "</campaigns></report>"));
        _server.Map("/exports/daily/2026-11-07.zip", _ => File(zip, "application/zip"));
        _server.Map("/exports/daily", _ => File(zip, "application/zip"));

        // Windowed: the engine resolved the (2026-11-06, 2026-11-07] day window.
        var sessions = Spec("daily_sessions", FileOptions("/exports/daily/{{ window_upper }}.zip",
            new() { ["format"] = "csv", ["members"] = "sessions_*.csv" },
            new() { ["day"] = "date", ["session_id"] = "varchar", ["started_at"] = "timestamp", ["duration_s"] = "int" }))
        with
        {
            WatermarkCursor = "day",
            WatermarkValue = "2026-11-06",
            WatermarkUpperBound = "2026-11-07",
        };

        // The original example's form: the date in the query.
        var eventsOptions = FileOptions("/exports/daily", new()
        {
            ["format"] = "json",
            ["layout"] = "lines",
            ["members"] = "events.jsonl",
        });
        eventsOptions["query"] = new Dictionary<string, object?> { ["date"] = "{{ run_date }}" };
        eventsOptions["run_date"] = new Dictionary<string, object?> { ["offset_days"] = "-1" };
        var events = Spec("daily_events", eventsOptions);

        var campaignsOptions = FileOptions("/exports/daily/{{ run_date }}.zip", new()
        {
            ["format"] = "xml",
            ["members"] = "campaigns.xml",
            ["records"] = "/report/campaigns/campaign",
            ["if_missing"] = "empty",
        }, new() { ["id"] = "bigint", ["name"] = "varchar", ["budget"] = "decimal" });
        campaignsOptions["run_date"] = new Dictionary<string, object?> { ["offset_days"] = "-1" };
        var campaigns = Spec("daily_campaigns", campaignsOptions);

        var sessionRows = await Read(sessions);
        Assert.Equal(
        [
            [new DateOnly(2026, 11, 7), "s1", new DateTimeOffset(2026, 11, 7, 10, 0, 0, TimeSpan.Zero), 30],
            [new DateOnly(2026, 11, 7), "s2", new DateTimeOffset(2026, 11, 7, 11, 30, 0, TimeSpan.Zero), 45],
        ], sessionRows);

        var eventRows = await Read(events);
        Assert.Equal(["""{"e":"click","n":1}""", """{"e":"view","n":2}"""], eventRows.Select(r => r[0]));
        Assert.All(eventRows, r => Assert.Equal("events.jsonl", r[1]));
        Assert.Equal([1L, 2L], eventRows.Select(r => r[2]));
        Assert.All(eventRows, r => Assert.Equal(Now, r[3]));

        var campaignRows = await Read(campaigns);
        Assert.Equal([[7L, "Autumn", 12.50m], [8L, "Winter", 3m]], campaignRows);

        Assert.Equal(2, _server.Requests.Count(r => r.Url.AbsolutePath == "/exports/daily/2026-11-07.zip"));
        var eventsRequest = Assert.Single(_server.Requests, r => r.Url.AbsolutePath == "/exports/daily");
        Assert.Equal("?date=2026-11-07", eventsRequest.Url.Query);
        Assert.Empty(_notices);
    }

    [Fact]
    public async Task Plain_csv_and_gzip_csv()
    {
        _server.Map("/files/plain.csv", _ => File("id,name\n1,a\n2,b\n"));
        _server.Map("/files/data.csv.gz", _ => File(Gzip("id,name\n3,c\n"), "application/gzip"));
        var columns = new Dictionary<string, string> { ["id"] = "int", ["name"] = "varchar" };
        Dictionary<string, object?> Csv() => new() { ["format"] = "csv", ["file_column"] = "src" };

        Assert.Equal([[1, "a", "plain.csv"], [2, "b", "plain.csv"]],
            await Read(Spec("plain", FileOptions("/files/plain.csv", Csv(), columns))));
        Assert.Equal([[3, "c", "data.csv"]],
            await Read(Spec("gz", FileOptions("/files/data.csv.gz", Csv(), columns))));
    }

    [Fact]
    public async Task If_missing_empty_yields_no_rows_and_notice()
    {
        _server.Map("/quiet.zip", _ => File(Zip(("sessions.csv", "id\n1\n")), "application/zip"));

        var rows = await Read(Spec("daily_campaigns", FileOptions("/quiet.zip", new()
        {
            ["format"] = "xml",
            ["members"] = "campaigns.xml",
            ["records"] = "/report/campaigns/campaign",
            ["if_missing"] = "empty",
        }, new() { ["id"] = "bigint" })));

        Assert.Empty(rows);
        var notice = Assert.Single(_notices);
        Assert.Contains("stub.daily_campaigns", notice);
        Assert.Contains("campaigns.xml", notice);
    }

    [Fact]
    public async Task Schema_matches_batches_in_both_modes()
    {
        _server.Map("/a.csv", _ => File("id,name\n1,a\n"));
        var contract = Spec("contract", FileOptions("/a.csv", new() { ["format"] = "csv", ["file_column"] = "src" },
            new() { ["id"] = "int", ["name"] = "varchar" }));
        var raw = Spec("raw", FileOptions("/a.csv", new() { ["format"] = "csv" }));

        await using var source = Source();
        var contractSchema = (await source.GetSchemaAsync(contract, CancellationToken.None)).Schema;
        Assert.Equal(["id", "name", "src"], contractSchema.FieldsList.Select(f => f.Name));
        var rawSchema = (await source.GetSchemaAsync(raw, CancellationToken.None)).Schema;
        Assert.Equal(["payload", "pz_file", "pz_row", "pz_fetched_at"], rawSchema.FieldsList.Select(f => f.Name));

        // Read asserts every batch carries GetSchemaAsync's schema.
        Assert.Single(await Read(contract, source));
        Assert.Single(await Read(raw, source));
    }

    [Fact]
    public async Task Error_page_with_200_is_refused()
    {
        _server.Map("/report.csv", _ => new StubResponse(200,
            """{"error":"session expired, see https://api.example/login?sig=s3cret"}""",
            new Dictionary<string, string> { ["Content-Type"] = "Application/JSON; charset=utf-8" }));
        _server.Map("/report.json", _ => new StubResponse(200, "<html>sign in</html>",
            new Dictionary<string, string> { ["Content-Type"] = "text/html" }));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => Read(Spec("csv", FileOptions("/report.csv", new() { ["format"] = "csv" }))));
        Assert.False(ex.IsTransient);
        Assert.Contains("expected a csv file", ex.Message);
        Assert.Contains("application/json", ex.Message);
        Assert.Contains("session expired", ex.Message);
        Assert.DoesNotContain("s3cret", ex.Message);

        var json = await Assert.ThrowsAsync<PzConnectorException>(
            () => Read(Spec("json", FileOptions("/report.json", new() { ["format"] = "json" }))));
        Assert.False(json.IsTransient);
        Assert.Contains("expected a json file", json.Message);
        Assert.Contains("sign in", json.Message);
    }

    [Fact]
    public async Task Http_status_classification_unchanged()
    {
        _server.Map("/busy.csv", _ => new StubResponse(503, "busy",
            new Dictionary<string, string> { ["Retry-After"] = "7" }));
        _server.Map("/gone.csv", _ => new StubResponse(404, "not here"));

        var busy = await Assert.ThrowsAsync<PzConnectorException>(
            () => Read(Spec("busy", FileOptions("/busy.csv", new() { ["format"] = "csv" }))));
        Assert.True(busy.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(7), busy.RetryAfter);
        Assert.Contains("HTTP 503", busy.Message);

        var gone = await Assert.ThrowsAsync<PzConnectorException>(
            () => Read(Spec("gone", FileOptions("/gone.csv", new() { ["format"] = "csv" }))));
        Assert.False(gone.IsTransient);
        Assert.Contains("HTTP 404", gone.Message);
        Assert.Contains("not here", gone.Message);
    }

    [Fact]
    public async Task Download_cap_enforced()
    {
        _server.Map("/big.csv", _ => File(new byte[2 * MiB]));
        await using var source = Source(new() { ["max_download_mb"] = "1" });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => Read(Spec("big", FileOptions("/big.csv", new() { ["format"] = "csv" })), source));

        Assert.False(ex.IsTransient);
        Assert.Contains("max_download_mb", ex.Message);
    }

    [Fact]
    public async Task Temp_dir_removed_on_success_failure_and_cancel()
    {
        var good = new StringBuilder("id\n");
        for (var i = 1; i <= 10; i++)
        {
            good.Append(i).Append('\n');
        }

        _server.Map("/good.csv", _ => File(good.ToString()));
        _server.Map("/bad.csv", _ => File("id\n1\n2\nnot-a-number\n"));
        var columns = new Dictionary<string, string> { ["id"] = "int" };
        var options = new BatchOptions(TargetBatchBytes: 1, MaxRowsPerBatch: 1);
        await using var source = Source();

        // Each read pauses after its first batch; the directory that appeared is this read's own.
        async Task<(HashSet<string> Created, Exception? Error)> ReadPausing(string path, bool cancelAfterFirst)
        {
            using var cts = new CancellationTokenSource();
            var before = TempDirs();
            var spec = Spec("tmp", FileOptions(path, new() { ["format"] = "csv" }, columns));
            var partition = Assert.Single(await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));
            HashSet<string>? created = null;
            try
            {
                await foreach (var batch in partition.ReadAsync(options, cts.Token))
                {
                    batch.Dispose();
                    if (created is null)
                    {
                        created = [.. TempDirs().Except(before)];
                        Assert.Single(created);
                        Assert.All(created, d => Assert.True(Directory.Exists(d)));
                        if (cancelAfterFirst)
                        {
                            await cts.CancelAsync();
                        }
                    }
                }

                return (created!, null);
            }
            catch (Exception ex)
            {
                return (created!, ex);
            }
        }

        var success = await ReadPausing("/good.csv", cancelAfterFirst: false);
        Assert.Null(success.Error);
        Assert.All(success.Created, d => Assert.False(Directory.Exists(d)));

        var failure = await ReadPausing("/bad.csv", cancelAfterFirst: false);
        Assert.IsType<PzConnectorException>(failure.Error);
        Assert.All(failure.Created, d => Assert.False(Directory.Exists(d)));

        var cancelled = await ReadPausing("/good.csv", cancelAfterFirst: true);
        Assert.IsAssignableFrom<OperationCanceledException>(cancelled.Error);
        Assert.All(cancelled.Created, d => Assert.False(Directory.Exists(d)));
    }

    [Fact]
    public async Task No_checkpoint_in_file_mode()
    {
        _server.Map("/a.csv", _ => File("id\n1\n"));
        var spec = Spec("ds", FileOptions("/a.csv", new() { ["format"] = "csv" }));
        await using var source = Source();
        var partition = Assert.IsAssignableFrom<ICheckpointingPartition>(
            Assert.Single(await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None)));

        Assert.Equal("stub.ds", partition.PartitionId);
        Assert.False(partition.TryResumeFrom("anything"));
        await foreach (var batch in partition.ReadAsync(new BatchOptions(), CancellationToken.None))
        {
            batch.Dispose();
            Assert.False(partition.TryGetCheckpoint(out var checkpoint));
            Assert.Null(checkpoint);
        }

        Assert.False(partition.TryGetCheckpoint(out _));
    }

    [Fact]
    public async Task Json_page_datasets_still_use_HttpPartition()
    {
        await using var source = Source();

        var page = await source.PlanReadAsync(Spec("page", new() { ["path"] = "/items" }), ReadHints.None,
            CancellationToken.None);
        Assert.IsType<HttpPartition>(Assert.Single(page));

        var file = await source.PlanReadAsync(Spec("file", FileOptions("/a.csv", new() { ["format"] = "csv" })),
            ReadHints.None, CancellationToken.None);
        Assert.IsType<HttpFilePartition>(Assert.Single(file));
    }
}
