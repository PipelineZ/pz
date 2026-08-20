using System.Text.Json;
using Parquet;
using Parquet.Schema;
using Pz.Mcp;
using Pz.Mcp.Handlers;

namespace Pz.Mcp.Tests;

/// <summary>pz_entity_schema tests. The live-fetch happy path uses a <b>parquet</b> entity, not a csv
/// one: <see cref="Pz.Connector.LocalFiles"/>'s CsvSource.GetSchemaAsync unconditionally requires a
/// declared `columns:` contract (see CsvSource.cs), and
/// <c>ConnectivityValidator.ProbeDatasetSchemaAsync</c> only ever populates <c>FetchedSchemas</c> for a
/// dataset with NO declared contract — so a contract-less csv entity can never reach
/// <c>FetchedSchemas</c> (it always throws PZ0330 first) and a contract-bearing one is only
/// drift-checked, never recorded there either. Parquet is self-describing (no contract required either
/// way — <c>ParquetSourceTests.Parquet_schema_read_from_footer_without_contract</c> pins this for the
/// connector itself), so it is the one offline, no-docker localfiles format that can actually exercise
/// live-fetched columns end to end through <see cref="IntrospectTools.EntitySchemaAsync"/>. The
/// declared-contract fallback path (source: "declared_contract") is exercised separately below with a
/// contract-bearing csv entity, which is exactly the case <c>FetchedSchemas</c> can never carry.</summary>
public sealed class EntitySchemaTests
{
    private static CliServices RealServices() => new()
    {
        CreateRegistryAsync = (project, dir, ct) =>
            Pz.Cli.ConnectorRegistryFactory.CreateAsync(project, dir, noLockCheck: false, ct),
        CreateStateStores = (_, _) => throw new InvalidOperationException("not needed for entity schema"),
        InitProject = (_, _, _) => throw new InvalidOperationException("not needed for entity schema"),
        RunAsync = (_, _) => throw new InvalidOperationException("not needed for entity schema"),
        RetryAsync = (_, _, _) => throw new InvalidOperationException("not needed for entity schema"),
    };

    [Fact]
    public async Task Entity_schema_fetches_live_columns_for_a_contract_less_parquet_entity()
    {
        using var p = new ParquetProject();
        var doc = JsonDocument.Parse(await IntrospectTools.EntitySchemaAsync(
            p.Dir, "raw", "orders", RealServices(), CancellationToken.None));
        var result = doc.RootElement.GetProperty("result");
        Assert.Contains(result.GetProperty("columns").EnumerateArray(),
            c => c.GetProperty("name").GetString() == "id");
        Assert.Equal("fetched", result.GetProperty("source").GetString());
    }

    [Fact]
    public async Task Entity_schema_falls_back_to_the_declared_contract_for_a_csv_entity()
    {
        using var p = new CsvWithContractProject();
        var doc = JsonDocument.Parse(await IntrospectTools.EntitySchemaAsync(
            p.Dir, "raw", "orders", RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        var result = doc.RootElement.GetProperty("result");
        Assert.Contains(result.GetProperty("columns").EnumerateArray(),
            c => c.GetProperty("name").GetString() == "id");
        Assert.Equal("declared_contract", result.GetProperty("source").GetString());
    }

    [Fact]
    public async Task Unknown_connection_is_an_enveloped_error_not_a_throw()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await IntrospectTools.EntitySchemaAsync(
            p.Dir, "nope", "orders", RealServices(), CancellationToken.None));
        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
    }

    [Fact]
    public async Task Unknown_entity_on_a_real_connection_is_an_enveloped_error_not_a_throw()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await IntrospectTools.EntitySchemaAsync(
            p.Dir, "raw", "no_such_entity", RealServices(), CancellationToken.None));
        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal("PZ0330", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
    }

    /// <summary>A minimal self-contained project (no docker, no network) with one contract-less
    /// `raw.orders` PARQUET entity — no pipeline reads it, which is fine: <c>DagCompiler</c> seeds
    /// <c>CompiledDag.Connections</c> from every YAML-declared connection's own <c>Datasets</c> up
    /// front, pipeline-readership only ever ADDS call-site-only entities on top.</summary>
    private sealed class ParquetProject : IDisposable
    {
        public string Dir { get; } = Path.Combine(Path.GetTempPath(), "pz-mcp-pq-" + Guid.NewGuid().ToString("N"));

        public ParquetProject()
        {
            Directory.CreateDirectory(Path.Combine(Dir, "pipelines"));
            Directory.CreateDirectory(Path.Combine(Dir, "data"));
            File.WriteAllText(Path.Combine(Dir, "project.yml"), "name: mcp_test\nversion: \"0.1.0\"\n");
            File.WriteAllText(Path.Combine(Dir, "connections.yml"),
                """
                raw:
                  connector: localfiles
                  entities:
                    orders:
                      read:
                        path: data/orders.parquet
                        format: parquet
                """ + "\n");
            WriteOrdersParquetAsync(Path.Combine(Dir, "data", "orders.parquet")).GetAwaiter().GetResult();
        }

        private static async Task WriteOrdersParquetAsync(string path)
        {
            var id = new DataField("id", typeof(int?));
            var amount = new DataField("amount", typeof(double?));
            var schema = new ParquetSchema(id, amount);

            await using var stream = new FileStream(path, FileMode.Create, FileAccess.Write);
            await using var writer = await ParquetWriter.CreateAsync(schema, stream);
            using var rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(id, new int?[] { 1, 2 }, cancellationToken: default);
            await rowGroup.WriteAsync<double>(amount, new double?[] { 10.0, 20.0 }, cancellationToken: default);
        }

        public void Dispose()
        {
            try { Directory.Delete(Dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>A minimal self-contained project (no docker, no network) with one `raw.orders` CSV
    /// entity that DOES declare a `columns:` contract — the case <see cref="ConnectivityValidator"/>
    /// only ever drift-checks, never records into <c>FetchedSchemas</c>, so this is what exercises
    /// <see cref="IntrospectTools.EntitySchemaAsync"/>'s declared-contract fallback.</summary>
    private sealed class CsvWithContractProject : IDisposable
    {
        public string Dir { get; } = Path.Combine(Path.GetTempPath(), "pz-mcp-csv-" + Guid.NewGuid().ToString("N"));

        public CsvWithContractProject()
        {
            Directory.CreateDirectory(Path.Combine(Dir, "pipelines"));
            Directory.CreateDirectory(Path.Combine(Dir, "data"));
            File.WriteAllText(Path.Combine(Dir, "project.yml"), "name: mcp_test\nversion: \"0.1.0\"\n");
            File.WriteAllText(Path.Combine(Dir, "data", "orders.csv"), "id,amount\n1,10\n2,20\n");
            File.WriteAllText(Path.Combine(Dir, "connections.yml"),
                """
                raw:
                  connector: localfiles
                  entities:
                    orders:
                      read:
                        path: data/orders.csv
                        format: csv
                        columns:
                          id: bigint
                          amount: double
                """ + "\n");
        }

        public void Dispose()
        {
            try { Directory.Delete(Dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
