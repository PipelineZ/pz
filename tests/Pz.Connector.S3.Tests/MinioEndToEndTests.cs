using System.Text.Json;
using Pz.Cli;
using Pz.DuckDb;

namespace Pz.Connector.S3.Tests;

/// <summary>MinIO Testcontainers e2e (docker+network gated -- see <see cref="MinioFixture"/>): a real
/// `pz run` writes a parquet output via the S3 native COPY path against a live MinIO instance, then a
/// second, independent DuckDB session reads it back via <c>read_parquet('s3://...')</c> through the same
/// kind of secret the sink itself generates -- proving the whole round trip, not just statement shape.</summary>
[Collection("minio")]
public sealed class MinioEndToEndTests(MinioFixture fixture)
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-s3-e2e", Guid.NewGuid().ToString("N"));

    [SkippableFact]
    public async Task S3_native_copy_roundtrips()
    {
        WriteProject(fixture.SecretKey);

        var readbackPath = Path.Combine(Path.GetTempPath(), $"pz-s3-readback-{Guid.NewGuid():N}.duckdb");
        try
        {
            var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
            Assert.Equal(ExitCodes.Ok, exit);

            var planPath = Path.Combine(_work, ".pz", "target", "plan.json");
            using var plan = JsonDocument.Parse(await File.ReadAllTextAsync(planPath));
            var sinkNode = plan.RootElement.GetProperty("nodes").EnumerateArray()
                .Single(n => n.GetProperty("kind").GetString() == "SinkWrite");
            Assert.Equal("native_copy", sinkNode.GetProperty("strategy").GetString());

            await using var duck = DuckSession.Open(readbackPath);
            await duck.ExecuteAsync("install httpfs");
            await duck.ExecuteAsync("load httpfs");
            await duck.ExecuteAsync(
                $"create or replace secret pz_s3_verify (type s3, key_id '{fixture.AccessKey}', " +
                $"secret '{fixture.SecretKey}', region 'us-east-1', endpoint '{fixture.Endpoint}', " +
                "url_style 'path', use_ssl false)");

            var rowCount = await duck.ScalarAsync<long>(
                $"select count(*) from read_parquet('s3://{MinioFixture.Bucket}/out/data.parquet')");

            Assert.Equal(3, rowCount);
        }
        finally
        {
            try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
            try { File.Delete(readbackPath); } catch { /* best-effort cleanup */ }
        }
    }

    [SkippableFact]
    public async Task S3_native_copy_roundtrips_json()
    {
        // The json (NDJSON) COPY writes an object a fresh session can
        // read back with read_json(..., format = 'newline_delimited') — the same round-trip proof the
        // parquet test above runs.
        WriteProject(fixture.SecretKey, format: "json");

        var readbackPath = Path.Combine(Path.GetTempPath(), $"pz-s3-readback-{Guid.NewGuid():N}.duckdb");
        try
        {
            var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
            Assert.Equal(ExitCodes.Ok, exit);

            await using var duck = DuckSession.Open(readbackPath);
            await duck.ExecuteAsync("install httpfs");
            await duck.ExecuteAsync("load httpfs");
            await duck.ExecuteAsync(
                $"create or replace secret pz_s3_verify (type s3, key_id '{fixture.AccessKey}', " +
                $"secret '{fixture.SecretKey}', region 'us-east-1', endpoint '{fixture.Endpoint}', " +
                "url_style 'path', use_ssl false)");

            var rowCount = await duck.ScalarAsync<long>(
                $"select count(*) from read_json('s3://{MinioFixture.Bucket}/out/data.json', format = 'newline_delimited')");
            Assert.Equal(3, rowCount);

            var name = await duck.ScalarAsync<string>(
                $"select customer from read_json('s3://{MinioFixture.Bucket}/out/data.json', format = 'newline_delimited') where id = 2");
            Assert.Equal("bob", name);
        }
        finally
        {
            try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
            try { File.Delete(readbackPath); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>A native COPY that fails against the REAL MinIO endpoint because
    /// the credentials are wrong (not a syntax error -- the CREATE SECRET statement itself is
    /// well-formed and succeeds; the S3 PUT inside the COPY is what MinIO rejects) must still never leak
    /// the wrong secret value into the run's recorded error. This exercises SinkWriteExecutor's OTHER
    /// sanitization site (the CopySql failure catch), distinct from NativeSetup's (already covered by
    /// SecretRedactionTests.Malformed_secret_setup_statement_never_leaks_the_secret).</summary>
    [SkippableFact]
    public async Task Wrong_credentials_produce_redacted_error()
    {
        const string WrongSecret = "WRONG_S3CRET_VALUE_ON_PURPOSE";
        WriteProject(WrongSecret);

        try
        {
            var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
            Assert.Equal(ExitCodes.NodeFailures, exit);

            var runsDir = Path.Combine(_work, ".pz", "runs");
            var runDir = Directory.EnumerateDirectories(runsDir).Single();
            var runResultsJson = await File.ReadAllTextAsync(Path.Combine(runDir, "run_results.json"));
            using var runResults = JsonDocument.Parse(runResultsJson);

            var sinkNode = runResults.RootElement.GetProperty("nodes").EnumerateArray()
                .Single(n => n.GetProperty("kind").GetString() == "SinkWrite");
            Assert.Equal("failed", sinkNode.GetProperty("status").GetString());

            var errorMessage = sinkNode.GetProperty("error").GetProperty("message").GetString()!;
            Assert.DoesNotContain(WrongSecret, errorMessage, StringComparison.Ordinal);
            Assert.DoesNotContain(fixture.AccessKey, errorMessage, StringComparison.Ordinal);
            Assert.DoesNotContain(WrongSecret, runResultsJson, StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    // --- reads through a real `pz run` ---

    /// <summary>Seeds one object in MinIO through a scratch DuckDB COPY — the same mechanism the sink
    /// uses, but hand-rolled here so the READ under test is exercised independently of S3Sink.</summary>
    private async Task SeedObjectAsync(string key, string selectSql, string formatClause)
    {
        var seedPath = Path.Combine(Path.GetTempPath(), $"pz-s3-seed-{Guid.NewGuid():N}.duckdb");
        try
        {
            await using var duck = DuckSession.Open(seedPath);
            await duck.ExecuteAsync("install httpfs");
            await duck.ExecuteAsync("load httpfs");
            await duck.ExecuteAsync(
                $"create or replace secret pz_s3_seed (type s3, key_id '{fixture.AccessKey}', " +
                $"secret '{fixture.SecretKey}', region 'us-east-1', endpoint '{fixture.Endpoint}', " +
                "url_style 'path', use_ssl false)");
            await duck.ExecuteAsync(
                $"copy ({selectSql}) to 's3://{MinioFixture.Bucket}/{key}' ({formatClause})");
        }
        finally
        {
            try { File.Delete(seedPath); } catch { /* best-effort cleanup */ }
        }
    }

    private void WriteReadProject(string readBlock, string pipelineSql)
    {
        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));
        Directory.CreateDirectory(Path.Combine(_work, "out"));

        File.WriteAllText(Path.Combine(_work, "project.yml"), """
            name: s3_read_e2e
            version: 0.1.0

            connectors:
              - package: Pz.Connector.LocalFiles
                version: 0.1.0
              - package: Pz.Connector.S3
                version: 0.1.0
            """);

        File.WriteAllText(Path.Combine(_work, "connections.yml"), $"""
            lake:
              connector: s3
              root: {MinioFixture.Bucket}
              access_key: "{fixture.AccessKey}"
              secret_key: "{fixture.SecretKey}"
              endpoint: "{fixture.Endpoint}"
              region: us-east-1
              url_style: path
              use_ssl: false
              entities:
            {readBlock}

            outfiles:
              connector: localfiles
              root: out
            """);

        File.WriteAllText(Path.Combine(_work, "pipelines", "result.sql"), pipelineSql);
    }

    [SkippableFact]
    public async Task S3_native_scan_reads_parquet_through_a_real_run()
    {
        await SeedObjectAsync("in/orders.parquet",
            "select * from (values (1, 'alice', 10.25), (2, 'bob', 20.50)) t(id, customer, amount)",
            "format parquet");
        WriteReadProject(
            """
                orders:
                  read:
                    path: in/orders.parquet
                    format: parquet
            """,
            "INSERT INTO {{ sink('outfiles', 'result', format: 'csv', strategy: 'replace') }}\n" +
            "select * from {{ source('lake', 'orders') }} order by id\n");

        try
        {
            var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
            Assert.Equal(ExitCodes.Ok, exit);

            var csv = await File.ReadAllTextAsync(Path.Combine(_work, "out", "result", "result.csv"));
            Assert.Contains("1,alice,10.25", csv, StringComparison.Ordinal);
            Assert.Contains("2,bob,20.5", csv, StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [SkippableFact]
    public async Task Contract_less_csv_read_infers_the_schema_over_a_glob()
    {
        // Two objects under one glob, no columns: contract — DuckDB auto-detects as part of the scan.
        await SeedObjectAsync("logs/a.csv", "select 1 as id, 'x' as tag", "format csv, header");
        await SeedObjectAsync("logs/b.csv", "select 2 as id, 'y' as tag", "format csv, header");
        WriteReadProject(
            """
                logs:
                  read:
                    path: logs/*.csv
                    format: csv
            """,
            "INSERT INTO {{ sink('outfiles', 'result', format: 'csv', strategy: 'replace') }}\n" +
            "select * from {{ source('lake', 'logs') }} order by id\n");

        try
        {
            var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
            Assert.Equal(ExitCodes.Ok, exit);

            var csv = await File.ReadAllTextAsync(Path.Combine(_work, "out", "result", "result.csv"));
            Assert.Contains("1,x", csv, StringComparison.Ordinal);
            Assert.Contains("2,y", csv, StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [SkippableFact]
    public async Task S3_to_s3_run_reads_one_object_and_writes_another()
    {
        await SeedObjectAsync("in2/src.json",
            "select * from (values (1, 'p'), (2, 'q')) t(id, name)", "format json");
        WriteReadProject(
            """
                src:
                  read:
                    path: in2/src.json
                    format: json
            """,
            "INSERT INTO {{ sink('lake', 'roundtrip', strategy: 'replace', format: 'parquet', path: 'out2') }}\n" +
            "select * from {{ source('lake', 'src') }}\n");

        var readbackPath = Path.Combine(Path.GetTempPath(), $"pz-s3-readback-{Guid.NewGuid():N}.duckdb");
        try
        {
            var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
            Assert.Equal(ExitCodes.Ok, exit);

            await using var duck = DuckSession.Open(readbackPath);
            await duck.ExecuteAsync("install httpfs");
            await duck.ExecuteAsync("load httpfs");
            await duck.ExecuteAsync(
                $"create or replace secret pz_s3_verify (type s3, key_id '{fixture.AccessKey}', " +
                $"secret '{fixture.SecretKey}', region 'us-east-1', endpoint '{fixture.Endpoint}', " +
                "url_style 'path', use_ssl false)");
            var rows = await duck.ScalarAsync<long>(
                $"select count(*) from read_parquet('s3://{MinioFixture.Bucket}/out2/roundtrip.parquet')");
            Assert.Equal(2, rows);
        }
        finally
        {
            try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
            try { File.Delete(readbackPath); } catch { /* best-effort cleanup */ }
        }
    }

    private void WriteProject(string secretKey, string format = "parquet")
    {
        Directory.CreateDirectory(Path.Combine(_work, "data"));
        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));

        File.WriteAllText(Path.Combine(_work, "project.yml"), """
            name: s3_e2e
            version: 0.1.0

            connectors:
              - package: Pz.Connector.LocalFiles
                version: 0.1.0
              - package: Pz.Connector.S3
                version: 0.1.0

            engine:
              threads: 2
            """);

        File.WriteAllText(Path.Combine(_work, "data", "orders.csv"), """
            id,customer,amount
            1,alice,10.25
            2,bob,20.50
            3,carol,30.75
            """);

        File.WriteAllText(Path.Combine(_work, "connections.yml"), """
            files:
              connector: localfiles
              entities:
                orders:
                  read:
                    path: data/orders.csv
                    format: csv
                    columns:
                      id: bigint
                      customer: varchar
                      amount: double
            """);

        File.WriteAllText(Path.Combine(_work, "pipelines", "passthrough.sql"),
            $"INSERT INTO {{{{ sink('lake', 'data', strategy: 'replace', format: '{format}', "
            + $"bucket: '{MinioFixture.Bucket}', path: 'out') }}}}\nselect * from {{{{ source('files', 'orders') }}}}\n");

        File.AppendAllText(Path.Combine(_work, "connections.yml"), $"""

            lake:
              connector: s3
              access_key: "{fixture.AccessKey}"
              secret_key: "{secretKey}"
              endpoint: "{fixture.Endpoint}"
              region: us-east-1
              url_style: path
              use_ssl: false
            """);
    }
}
