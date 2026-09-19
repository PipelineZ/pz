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
            p.Dir, "raw", "orders", read: null, RealServices(), CancellationToken.None));
        var result = doc.RootElement.GetProperty("result");
        Assert.Contains(result.GetProperty("columns").EnumerateArray(),
            c => c.GetProperty("name").GetString() == "id");
        Assert.Equal("fetched", result.GetProperty("source").GetString());
    }

    // The declared read is the one that applies to a declared entity. Options passed on top of it are
    // not used, and the caller -- who would otherwise trust a schema shaped by options it never got --
    // is told so.
    [Fact]
    public async Task Read_options_for_an_already_declared_entity_are_reported_as_unused()
    {
        using var p = new ParquetProject();
        var doc = JsonDocument.Parse(await IntrospectTools.EntitySchemaAsync(
            p.Dir, "raw", "orders", read: new Dictionary<string, object?> { ["format"] = "csv" },
            RealServices(), CancellationToken.None));

        var result = doc.RootElement.GetProperty("result");
        Assert.Equal("fetched", result.GetProperty("source").GetString());
        Assert.Contains("already declared", result.GetProperty("note").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task No_note_when_no_read_options_were_passed()
    {
        using var p = new ParquetProject();
        var doc = JsonDocument.Parse(await IntrospectTools.EntitySchemaAsync(
            p.Dir, "raw", "orders", read: null, RealServices(), CancellationToken.None));

        Assert.False(doc.RootElement.GetProperty("result").TryGetProperty("note", out _));
    }

    [Fact]
    public async Task Entity_schema_falls_back_to_the_declared_contract_for_a_csv_entity()
    {
        using var p = new CsvWithContractProject();
        var doc = JsonDocument.Parse(await IntrospectTools.EntitySchemaAsync(
            p.Dir, "raw", "orders", read: null, RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        var result = doc.RootElement.GetProperty("result");
        Assert.Contains(result.GetProperty("columns").EnumerateArray(),
            c => c.GetProperty("name").GetString() == "id");
        Assert.Equal("declared_contract", result.GetProperty("source").GetString());
        // Nothing lives beyond the declared columns for this csv entity -- additive field, but present
        // and false rather than omitted, since we DID check.
        Assert.False(result.GetProperty("differs_from_contract").GetBoolean());
    }

    // A parquet file is self-describing, so a `columns:` contract narrower than the file's real shape
    // is exactly the case ConnectivityValidator only ever drift-checks (never errors on an EXTRA fetched
    // column -- contracts prune on read, they don't widen). pz_entity_schema must surface that the live
    // table grew, additively, without changing what `columns`/`source` already report for every other
    // contract-bearing entity.
    [Fact]
    public async Task Entity_schema_flags_a_declared_contract_that_is_narrower_than_the_live_schema()
    {
        using var p = new ParquetWithNarrowContractProject();
        var doc = JsonDocument.Parse(await IntrospectTools.EntitySchemaAsync(
            p.Dir, "raw", "orders", read: null, RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        var result = doc.RootElement.GetProperty("result");
        Assert.Equal("declared_contract", result.GetProperty("source").GetString());
        Assert.True(result.GetProperty("differs_from_contract").GetBoolean());
        Assert.Contains(result.GetProperty("extra_columns").EnumerateArray(),
            c => c.GetString() == "extra");
        // The declared contract's own two columns still ride `columns` unchanged -- additive, not a
        // field swap.
        Assert.Equal(2, result.GetProperty("columns").GetArrayLength());
    }

    // The natural authoring order: look at the table, then write the pipeline. An entity not yet
    // declared under connections.yml's `entities:` block is still just a name in that place -- the
    // connector only needs the connection + entity name (+ optional read options this call supplies)
    // to discover a schema, exactly what a bare `source()` call site already gets away with. `read`
    // carries `format: parquet` because localfiles defaults every undeclared entity to csv, which would
    // otherwise require the very columns: contract this call is trying to avoid pre-declaring.
    [Fact]
    public async Task Entity_schema_fetches_live_columns_for_an_undeclared_entity_of_a_declared_connection()
    {
        using var p = new ParquetProject();
        await ParquetProject.WriteParquetAsync(Path.Combine(p.Dir, "customers.parquet"), "id", "name");
        var doc = JsonDocument.Parse(await IntrospectTools.EntitySchemaAsync(
            p.Dir, "raw", "customers", read: new() { ["format"] = "parquet" },
            RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        var result = doc.RootElement.GetProperty("result");
        Assert.Equal("fetched", result.GetProperty("source").GetString());
        Assert.Contains(result.GetProperty("columns").EnumerateArray(),
            c => c.GetProperty("name").GetString() == "id");
        Assert.Contains(result.GetProperty("columns").EnumerateArray(),
            c => c.GetProperty("name").GetString() == "name");
    }

    [Fact]
    public async Task Unknown_connection_is_an_enveloped_error_not_a_throw()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await IntrospectTools.EntitySchemaAsync(
            p.Dir, "nope", "orders", read: null, RealServices(), CancellationToken.None));
        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        var error = doc.RootElement.GetProperty("errors")[0];
        Assert.Equal("PZ0330", error.GetProperty("code").GetString());
        // Project-relative, like every other PzError -- never the machine's absolute temp path.
        Assert.Equal("connections.yml", error.GetProperty("file").GetString());
    }

    // "no such table/file" is a genuinely unprobable entity, even on a declared connection -- this must
    // still fail cleanly (unlike an UNDECLARED entity, which the two tests above now resolve).
    [Fact]
    public async Task Unknown_entity_on_a_real_connection_is_an_enveloped_error_not_a_throw()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await IntrospectTools.EntitySchemaAsync(
            p.Dir, "raw", "no_such_entity", read: null, RealServices(), CancellationToken.None));
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

        /// <summary>A minimal self-describing parquet file with the given column names, each written as
        /// nullable string -- only column NAMES are asserted on by the tests that use this, never types.</summary>
        internal static async Task WriteParquetAsync(string path, params string[] columnNames)
        {
            var fields = columnNames.Select(n => new DataField(n, typeof(string))).ToArray();
            var schema = new ParquetSchema(fields);

            await using var stream = new FileStream(path, FileMode.Create, FileAccess.Write);
            await using var writer = await ParquetWriter.CreateAsync(schema, stream);
            using var rowGroup = writer.CreateRowGroup();
            foreach (var field in fields)
            {
                await rowGroup.WriteAsync(field, new List<string?> { "a", "b" });
            }
        }

        public void Dispose()
        {
            try { Directory.Delete(Dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>A minimal self-contained project (no docker, no network) with one `raw.orders` PARQUET
    /// entity whose declared `columns:` contract (id, amount) is narrower than the file's real shape
    /// (id, amount, extra) -- parquet is self-describing, so the live fetch always sees `extra` even
    /// though the contract never declared it. Exercises <c>differs_from_contract</c>/<c>extra_columns</c>.</summary>
    private sealed class ParquetWithNarrowContractProject : IDisposable
    {
        public string Dir { get; } = Path.Combine(Path.GetTempPath(), "pz-mcp-pqx-" + Guid.NewGuid().ToString("N"));

        public ParquetWithNarrowContractProject()
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
                        columns:
                          id: bigint
                          amount: double
                """ + "\n");
            WriteOrdersParquetAsync(Path.Combine(Dir, "data", "orders.parquet")).GetAwaiter().GetResult();
        }

        // id/amount match the declared contract's types exactly (int64/double) -- a type MISMATCH is a
        // drift error (PZ0331) this test is not about; only `extra`, absent from the contract entirely,
        // is the divergence under test.
        private static async Task WriteOrdersParquetAsync(string path)
        {
            // id is int64 (long), matching the declared "bigint" contract's exact Arrow expectation --
            // ContractTypes.ToArrowExpectation("bigint") is Int64Type, and int32 would mismatch it,
            // which is a real drift error (PZ0331) this test is not about.
            var id = new DataField("id", typeof(long?));
            var amount = new DataField("amount", typeof(double?));
            var extra = new DataField("extra", typeof(string));
            var schema = new ParquetSchema(id, amount, extra);

            await using var stream = new FileStream(path, FileMode.Create, FileAccess.Write);
            await using var writer = await ParquetWriter.CreateAsync(schema, stream);
            using var rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<long>(id, new long?[] { 1, 2 }, cancellationToken: default);
            await rowGroup.WriteAsync<double>(amount, new double?[] { 10.0, 20.0 }, cancellationToken: default);
            await rowGroup.WriteAsync(extra, new List<string?> { "x", "y" });
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
