using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>Stored-procedure extraction via query: mode. EXEC flows through the same
/// verbatim-query path as any user SQL: schema probe via CommandBehavior.SchemaOnly (SqlClient's
/// legacy SET FMTONLY ON semantics, not sp_describe_first_result_set), reads via the typed Arrow
/// reader. A proc using dynamic SQL (sp_executesql/EXEC(string)) against real tables IS statically
/// describable under
/// FMTONLY -- no WITH RESULT SETS hint needed, contrary to a common assumption. The genuine caveat
/// is temp tables: FMTONLY skips DDL, so a proc that stages its result in a #temp table fails the
/// probe with "Invalid object name", and WITH RESULT SETS does not rescue it (that clause targets
/// the sp_describe_first_result_set API, which this probe doesn't use). Query mode never receives
/// watermark/predicate pushdown (the connector never rewrites user SQL), but partition_column/
/// partitions DO apply -- the query is wrapped as a derived table, parity with Postgres; none of
/// the specs below set partition options, so each expects the single-partition case.</summary>
[Collection("sqlserver")]
public sealed class StoredProcedureTests(MsSqlContainerFixture fixture)
{
    private ConnectorConfig Config => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host, ["port"] = fixture.Port, ["database"] = fixture.Database,
        ["user"] = fixture.User, ["password"] = fixture.Password,
        ["trust_server_certificate"] = true,
    });

    private async Task<(Schema Schema, long Rows)> ReadAsync(string query)
    {
        ISourceConnector connector = new SqlServerConnector();
        await using var source = await connector.OpenAsync(Config, CancellationToken.None);
        var spec = new DatasetSpec("ms", "proc", new Dictionary<string, object?> { ["query"] = query });
        var schema = (await source.GetSchemaAsync(spec, CancellationToken.None)).Schema;
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var partition = Assert.Single(partitions); // the specs in this file set no partition options, so a single partition is expected
        var rows = 0L;
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return (schema, rows);
    }

    private async Task<PzConnectorException> SchemaProbeFailsAsync(string query)
    {
        ISourceConnector connector = new SqlServerConnector();
        await using var source = await connector.OpenAsync(Config, CancellationToken.None);
        var spec = new DatasetSpec("ms", "proc", new Dictionary<string, object?> { ["query"] = query });
        return await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.GetSchemaAsync(spec, CancellationToken.None));
    }

    [SkippableFact]
    public async Task Describable_proc_probes_and_reads_via_query_mode()
    {
        DockerFacts.SkipUnlessDocker();
        var (schema, rows) = await ReadAsync("exec dbo.orders_since @min_id = 99");
        Assert.Equal(["id", "name", "amount", "flag", "created"],
            schema.FieldsList.Select(f => f.Name).ToArray());
        Assert.Equal(50, rows); // ids 100..149
    }

    [SkippableFact]
    public async Task Dynamic_sql_proc_probes_and_reads_without_a_result_sets_hint()
    {
        // CommandBehavior.SchemaOnly is SqlClient's legacy SET FMTONLY ON, which runs the
        // sp_executesql batch too (suppressing rows, not skipping it) -- so a dynamic-SQL proc
        // against a real table is already statically describable. No WITH RESULT SETS is needed
        // here; see the class doc comment.
        DockerFacts.SkipUnlessDocker();
        var (schema, rows) = await ReadAsync("exec dbo.orders_dynamic");
        Assert.Equal(["id", "name"], schema.FieldsList.Select(f => f.Name).ToArray());
        Assert.Equal(150, rows);
    }

    [SkippableFact]
    public async Task Temp_table_proc_fails_schema_probe_even_with_a_result_sets_hint()
    {
        // The real "not statically describable" case: FMTONLY skips the CREATE TABLE #tmp, so the
        // later reference to #tmp fails. WITH RESULT SETS does not fix this under FMTONLY semantics
        // (verified directly against the server, not just through the connector); the binding
        // requirement is a clean classified error, with no recovery path.
        DockerFacts.SkipUnlessDocker();
        var withoutHint = await SchemaProbeFailsAsync("exec dbo.orders_temp");
        Assert.False(withoutHint.IsTransient);
        Assert.False(string.IsNullOrWhiteSpace(withoutHint.Message));
        Assert.Contains("proc", withoutHint.Message, StringComparison.OrdinalIgnoreCase); // names the dataset

        var withHint = await SchemaProbeFailsAsync(
            "exec dbo.orders_temp with result sets ((id int, name nvarchar(50)))");
        Assert.False(withHint.IsTransient);
        Assert.False(string.IsNullOrWhiteSpace(withHint.Message));
    }

    // First-class `procedure:` dataset mode. Unlike the query: EXEC path above, these go
    // through DatasetSpec.Options["procedure"]/["parameters"] and CommandType.StoredProcedure with
    // typed SqlParameters -- ProcedureDataset.BuildCommand, never a hand-built EXEC string.
    private async Task<(Schema Schema, long Rows)> ReadProcedureAsync(
        string procedure, Dictionary<string, object?>? parameters = null, Dictionary<string, object?>? columns = null,
        string? watermarkCursor = null, string? watermarkValue = null, string? watermarkUpperBound = null)
    {
        ISourceConnector connector = new SqlServerConnector();
        await using var source = await connector.OpenAsync(Config, CancellationToken.None);
        var options = new Dictionary<string, object?> { ["procedure"] = procedure };
        if (parameters is not null)
        {
            options["parameters"] = parameters;
        }

        if (columns is not null)
        {
            options["columns"] = columns;
        }

        var spec = new DatasetSpec("ms", "proc", options) with
        {
            WatermarkCursor = watermarkCursor, WatermarkValue = watermarkValue, WatermarkUpperBound = watermarkUpperBound,
        };
        var schema = (await source.GetSchemaAsync(spec, CancellationToken.None)).Schema;
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var partition = Assert.Single(partitions); // procedure mode: always single partition
        var rows = 0L;
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return (schema, rows);
    }

    [SkippableFact]
    public async Task Procedure_mode_reads_full_result()
    {
        DockerFacts.SkipUnlessDocker();
        var (schema, rows) = await ReadProcedureAsync("dbo.orders_page");
        Assert.Equal(["id", "name", "amount", "flag", "created"],
            schema.FieldsList.Select(f => f.Name).ToArray());
        Assert.Equal(150, rows);
    }

    [SkippableFact]
    public async Task Procedure_mode_binds_watermark_parameter()
    {
        DockerFacts.SkipUnlessDocker();
        var (_, rows) = await ReadProcedureAsync("dbo.orders_page",
            parameters: new Dictionary<string, object?> { ["min_id"] = "$watermark" },
            watermarkCursor: "id", watermarkValue: "99");
        Assert.Equal(50, rows); // ids 100..149
    }

    [SkippableFact]
    public async Task Procedure_mode_binds_window_parameters()
    {
        DockerFacts.SkipUnlessDocker();
        var (_, rows) = await ReadProcedureAsync("dbo.orders_page",
            parameters: new Dictionary<string, object?> { ["min_id"] = "$watermark", ["max_id"] = "$watermark_upper" },
            watermarkCursor: "id", watermarkValue: "99", watermarkUpperBound: "119");
        Assert.Equal(20, rows); // ids 100..119
    }

    [SkippableFact]
    public async Task Procedure_mode_columns_contract_bypasses_probe_for_temp_proc()
    {
        DockerFacts.SkipUnlessDocker();
        // dbo.orders_temp stages its result in a #temp table -- Temp_table_proc_fails_schema_probe_even_
        // with_a_result_sets_hint above proves FMTONLY cannot describe it. GetSchemaAsync succeeding here
        // is only possible via the columns: contract bypass (no server-describe probe at all).
        var (schema, rows) = await ReadProcedureAsync("dbo.orders_temp",
            columns: new Dictionary<string, object?> { ["id"] = "int", ["name"] = "varchar" });
        Assert.Equal(["id", "name"], schema.FieldsList.Select(f => f.Name).ToArray());
        Assert.Equal(150, rows);
    }

    [SkippableFact]
    public async Task Procedure_mode_contract_mismatch_errors()
    {
        DockerFacts.SkipUnlessDocker();
        // orders_temp's actual 'id' column is int, not bigint -- the mismatch surfaces at read time
        // (GetSchemaAsync itself never probes the server when a columns: contract is declared).
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await ReadProcedureAsync(
            "dbo.orders_temp", columns: new Dictionary<string, object?> { ["id"] = "bigint", ["name"] = "varchar" }));
        Assert.False(ex.IsTransient);
        Assert.Contains("column 'id'", ex.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task Procedure_mode_schema_probe_with_null_watermark_sentinel_succeeds()
    {
        DockerFacts.SkipUnlessDocker();
        // The exact production scenario the planner runs: a schema probe with NO watermark set on
        // the spec, so the "$watermark" sentinel binds
        // DBNull.Value into GetSchemaAsync's own SchemaOnly probe command -- not just the read path.
        ISourceConnector connector = new SqlServerConnector();
        await using var source = await connector.OpenAsync(Config, CancellationToken.None);
        var spec = new DatasetSpec("ms", "proc", new Dictionary<string, object?>
        {
            ["procedure"] = "dbo.orders_page",
            ["parameters"] = new Dictionary<string, object?> { ["min_id"] = "$watermark" },
        });
        var schema = (await source.GetSchemaAsync(spec, CancellationToken.None)).Schema;
        Assert.Equal(["id", "name", "amount", "flag", "created"],
            schema.FieldsList.Select(f => f.Name).ToArray());
    }
}
