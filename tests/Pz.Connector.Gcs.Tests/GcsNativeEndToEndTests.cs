using System.Text.Json;
using Pz.Cli;
using Pz.DuckDb;

namespace Pz.Connector.Gcs.Tests;

/// <summary>MinIO Testcontainers e2e for the NATIVE tier (docker+network gated -- see
/// <see cref="GcsMinioFixture"/>): a real `pz run` writes a parquet output via the gcs native COPY
/// path (<c>type gcs</c> secret, <c>gs://</c> URL, endpoint override) against a live s3-interop
/// server, reads one back through a native scan, and a second, independent DuckDB session verifies
/// the landed object -- proving the whole round trip, not just statement shape.</summary>
[Collection("gcs-minio")]
public sealed class GcsNativeEndToEndTests(GcsMinioFixture fixture)
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-gcs-e2e", Guid.NewGuid().ToString("N"));

    [SkippableFact]
    public async Task Gcs_native_copy_roundtrips()
    {
        WriteWriteProject();

        var readbackPath = Path.Combine(Path.GetTempPath(), $"pz-gcs-readback-{Guid.NewGuid():N}.duckdb");
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
            await duck.ExecuteAsync(VerifySecretSql());

            var rowCount = await duck.ScalarAsync<long>(
                $"select count(*) from read_parquet('gs://{GcsMinioFixture.Bucket}/out/data.parquet')");

            Assert.Equal(3, rowCount);
        }
        finally
        {
            try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
            try { File.Delete(readbackPath); } catch { /* best-effort cleanup */ }
        }
    }

    [SkippableFact]
    public async Task Gcs_native_scan_reads_parquet_through_a_real_run()
    {
        await SeedObjectAsync("in/orders.parquet",
            "select * from (values (1, 'alice', 10.25), (2, 'bob', 20.50)) t(id, customer, amount)",
            "format parquet");
        WriteReadProject();

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

    private string VerifySecretSql() =>
        $"create or replace secret pz_gcs_verify (type gcs, key_id '{fixture.AccessKey}', " +
        $"secret '{fixture.SecretKey}', endpoint '{fixture.Endpoint}', url_style 'path', use_ssl false)";

    /// <summary>Seeds one object through a scratch DuckDB COPY — the same mechanism the sink uses,
    /// but hand-rolled here so the READ under test is exercised independently of GcsSink.</summary>
    private async Task SeedObjectAsync(string key, string selectSql, string formatClause)
    {
        var seedPath = Path.Combine(Path.GetTempPath(), $"pz-gcs-seed-{Guid.NewGuid():N}.duckdb");
        try
        {
            await using var duck = DuckSession.Open(seedPath);
            await duck.ExecuteAsync("install httpfs");
            await duck.ExecuteAsync("load httpfs");
            await duck.ExecuteAsync(VerifySecretSql());
            await duck.ExecuteAsync(
                $"copy ({selectSql}) to 'gs://{GcsMinioFixture.Bucket}/{key}' ({formatClause})");
        }
        finally
        {
            try { File.Delete(seedPath); } catch { /* best-effort cleanup */ }
        }
    }

    private string ConnectionBlock() => $"""
        lake:
          connector: gcs
          auth: hmac
          key_id: "{fixture.AccessKey}"
          secret: "{fixture.SecretKey}"
          endpoint: "{fixture.Endpoint}"
          url_style: path
          use_ssl: false
        """;

    private void WriteWriteProject()
    {
        Directory.CreateDirectory(Path.Combine(_work, "data"));
        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));

        File.WriteAllText(Path.Combine(_work, "project.yml"), """
            name: gcs_e2e
            version: 0.1.0

            connectors:
              - package: Pz.Connector.LocalFiles
                version: 0.1.0
              - package: Pz.Connector.Gcs
                version: 0.1.0
            """);

        File.WriteAllText(Path.Combine(_work, "data", "orders.csv"), """
            id,customer,amount
            1,alice,10.25
            2,bob,20.50
            3,carol,30.75
            """);

        File.WriteAllText(Path.Combine(_work, "connections.yml"), $"""
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

            {ConnectionBlock()}
            """);

        File.WriteAllText(Path.Combine(_work, "pipelines", "passthrough.sql"),
            "INSERT INTO {{ sink('lake', 'data', strategy: 'replace', format: 'parquet', " +
            $"bucket: '{GcsMinioFixture.Bucket}', path: 'out') }}}}\n" +
            "select * from {{ source('files', 'orders') }}\n");
    }

    private void WriteReadProject()
    {
        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));
        Directory.CreateDirectory(Path.Combine(_work, "out"));

        File.WriteAllText(Path.Combine(_work, "project.yml"), """
            name: gcs_read_e2e
            version: 0.1.0

            connectors:
              - package: Pz.Connector.LocalFiles
                version: 0.1.0
              - package: Pz.Connector.Gcs
                version: 0.1.0
            """);

        File.WriteAllText(Path.Combine(_work, "connections.yml"), $"""
            {ConnectionBlock()}
              root: {GcsMinioFixture.Bucket}
              entities:
                orders:
                  read:
                    path: in/orders.parquet
                    format: parquet

            outfiles:
              connector: localfiles
              root: out
            """);

        File.WriteAllText(Path.Combine(_work, "pipelines", "result.sql"),
            "INSERT INTO {{ sink('outfiles', 'result', format: 'csv', strategy: 'replace') }}\n" +
            "select * from {{ source('lake', 'orders') }} order by id\n");
    }
}
