using System.Data;
using System.Globalization;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.SqlServer;

/// <summary>SQL Server source: T-SQL SELECT generation with column pruning, engine predicate
/// pushdown, watermark lower bound and bounded-window upper bound. Watermark/window
/// literals stay untyped and unquoted-by-cast: T-SQL data-type precedence converts the literal to
/// the COLUMN's type, so the comparison stays sargable. PredicateSql is engine-generated and
/// trusted raw (documented ABI trust boundary); identifiers are never trusted and go through
/// MsDdl.Quote.</summary>
public sealed partial class SqlServerSource(string connectionString) : ISource, IChangeCaptureAdmin
{
    internal static string BuildSelect(DatasetSpec spec, ReadHints hints)
    {
        var query = spec.Options.TryGetValue("query", out var q) ? q?.ToString() : null;
        var procedure = spec.Options.TryGetValue("procedure", out var p) ? p?.ToString() : null;
        if (query is not null && procedure is not null)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': 'query' and 'procedure' are mutually exclusive", isTransient: false);
        }

        if (procedure is not null)
        {
            // Unreachable via GetSchemaAsync/PlanReadAsync: SqlServerSource branches to ProcedureDataset
            // before ever calling BuildSelect. Reachable only from a direct unit test of this pure guard.
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': procedure mode does not build a SELECT statement", isTransient: false);
        }

        if (query is not null)
        {
            return query; // query mode: user SQL verbatim, hints not applied
        }

        // There is no `table:`/`schema:` option -- the dataset name is the object name, qualified by
        // its own dot.
        var (schema, table) = MsDdl.SplitEntity(spec.Dataset);
        var columns = hints.Columns is { Count: > 0 }
            ? string.Join(", ", hints.Columns.Select(MsDdl.Quote))
            : "*";
        var sql = $"select {columns} from {MsDdl.Quote(schema)}.{MsDdl.Quote(table)}";

        var predicates = new List<string>(3);
        if (hints.PredicateSql is { Length: > 0 } predicate)
        {
            predicates.Add($"({predicate})");
        }

        // Gated on WatermarkValue (not WatermarkCursor alone), per DatasetSpec.WatermarkCursor's doc
        // comment ("when set, alongside WatermarkValue"): SpecBuilder stamps WatermarkCursor on every
        // incremental dataset's spec, even on a first run with no stored watermark yet, so a
        // cursor-set/value-null spec is a real, expected shape here -- it must fall through to the
        // same unfiltered SELECT as a watermark-free spec, not dereference a null WatermarkValue.
        if (spec is { WatermarkCursor: not null, WatermarkValue: not null })
        {
            var op = spec.WatermarkLowerInclusive ? ">=" : ">";
            predicates.Add($"{MsDdl.Quote(spec.WatermarkCursor)} {op} '{spec.WatermarkValue!.Replace("'", "''")}'");
        }

        if (spec.WatermarkCursor is not null && spec.WatermarkUpperBound is not null)
        {
            predicates.Add($"{MsDdl.Quote(spec.WatermarkCursor)} <= '{spec.WatermarkUpperBound.Replace("'", "''")}'");
        }

        // Each term self-parenthesized before the AND-join: a disjunctive engine pushdown must not
        // let the watermark's AND bind into the middle of its OR.
        return predicates.Count > 0
            ? $"{sql} where {string.Join(" and ", predicates.Select(p => $"({p})"))}"
            : sql;
    }

    private const int MinPartitions = 1;
    private const int MaxPartitions = 16;

    public async ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        if (spec.ChangeCapture)
        {
            // Offline-safe: plain SQL against the BASE table (no prerequisite/capture-instance work --
            // that runs at read open), so a dataset can be validated before cdc is even enabled server-side.
            var (cdcSchema, cdcTable) = SqlServerCdc.SchemaAndTable(spec);
            await using var cdcConnection = new SqlConnection(connectionString);
            try
            {
                await cdcConnection.OpenAsync(ct).ConfigureAwait(false);
                await using var cdcCommand = new SqlCommand(SqlServerCdc.SnapshotSelect(cdcSchema, cdcTable), cdcConnection);
                await using var cdcReader = await cdcCommand.ExecuteReaderAsync(CommandBehavior.SchemaOnly, ct).ConfigureAwait(false);
                return new DatasetSchema(SqlServerArrowReader.BuildSchema(cdcReader, $"dataset '{spec.Dataset}'"));
            }
            catch (SqlException ex)
            {
                throw new PzConnectorException(
                    $"dataset '{spec.Dataset}': schema probe failed: {ex.Message}", ex.IsTransient, innerException: ex);
            }
        }

        if (ProcedureDataset.IsProcedure(spec))
        {
            // Declared columns: contract bypasses the probe entirely: FMTONLY cannot
            // describe procs that stage their result in a #temp table, so this is the escape hatch.
            if (ProcedureDataset.BuildContractSchema(spec) is { } contractSchema)
            {
                return new DatasetSchema(contractSchema);
            }

            await using var procConnection = new SqlConnection(connectionString);
            try
            {
                await procConnection.OpenAsync(ct).ConfigureAwait(false);
                await using var command = ProcedureDataset.BuildCommand(procConnection, spec);
                // SchemaOnly: works for procs whose shape FMTONLY can compute; parameters are bound
                // ($watermark sentinels bind DBNull on planning probes, which never carry a watermark).
                await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SchemaOnly, ct).ConfigureAwait(false);
                return new DatasetSchema(SqlServerArrowReader.BuildSchema(reader, $"dataset '{spec.Dataset}'"));
            }
            catch (SqlException ex)
            {
                throw new PzConnectorException(
                    $"dataset '{spec.Dataset}': schema probe failed: {ex.Message}", ex.IsTransient, innerException: ex);
            }
        }

        var select = BuildSelect(spec, ReadHints.None);
        await using var connection = new SqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await using var command = new SqlCommand(select, connection);
            // SchemaOnly: SqlClient's legacy SET FMTONLY ON, not a true describe -- it EXECUTES the
            // batch with row output suppressed (no row-fetch cost), but #temp DDL is skipped, which
            // breaks probing procs that stage into #temp (see MsSqlContainerFixture's seed comments
            // and ProcedureDataset's columns: contract escape hatch).
            await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SchemaOnly, ct).ConfigureAwait(false);
            return new DatasetSchema(SqlServerArrowReader.BuildSchema(reader, $"dataset '{spec.Dataset}'"));
        }
        catch (SqlException ex)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': schema probe failed: {ex.Message}", ex.IsTransient, innerException: ex);
        }
    }

    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        // Universal path only: the community mssql DuckDB extension is the designated
        // future native tier; decline quietly so the universal path always applies.
        scan = null;
        return false;
    }

    // Table-mode options that are meaningless (and so forbidden) on a cdc dataset: cdc reads a single
    // table directly under a snapshot / the change-table window, not a custom query, procedure, or a
    // range-partitioned scan.
    private static readonly string[] CdcForbiddenOptions = ["query", "procedure", "partition_column", "partitions"];

    public async ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        if (spec.ChangeCapture)
        {
            foreach (var forbidden in CdcForbiddenOptions)
            {
                if (spec.Options.ContainsKey(forbidden))
                {
                    throw new PzConnectorException(
                        $"dataset '{spec.Dataset}': option '{forbidden}' is not allowed with change capture " +
                        "-- a cdc dataset reads its 'table' directly; remove it",
                        isTransient: false);
                }
            }

            // Validates 'table' presence (throws non-transient otherwise). One partition: cdc is
            // inherently sequential (a single bounded LSN window per run).
            _ = SqlServerCdc.SchemaAndTable(spec);
            return [new SqlServerCdcPartition(connectionString, spec)];
        }

        if (ProcedureDataset.IsProcedure(spec))
        {
            // Checked before any connection is opened: re-running a proc per partition would violate
            // the union-equals-one-read invariant for non-deterministic procs.
            if (spec.Options.ContainsKey("partition_column") || spec.Options.ContainsKey("partitions"))
            {
                throw new PzConnectorException(
                    $"dataset '{spec.Dataset}': partitioned reads are not supported for procedure datasets -- " +
                    "hint: expose the underlying query via 'table' or 'query', or remove 'partition_column'",
                    isTransient: false);
            }

            return [new SqlServerProcedurePartition(connectionString, spec)];
        }

        var select = BuildSelect(spec, hints);
        var partitionColumn = spec.Options.TryGetValue("partition_column", out var pc) ? pc?.ToString() : null;
        if (partitionColumn is null)
        {
            return [new SqlServerPartition(connectionString, select)];
        }

        var partitionCount = ParsePartitionCount(spec);
        if (partitionCount == MinPartitions)
        {
            return [new SqlServerPartition(connectionString, select)];
        }

        var (min, max) = await ProbeRangeAsync(spec, select, partitionColumn, ct).ConfigureAwait(false);
        if (min is null || max is null || min.Equals(max))
        {
            return [new SqlServerPartition(connectionString, select)];
        }

        var boundaries = RangeBoundaries.ComputeLiterals(min, max, partitionCount);
        return BuildPartitionSelects(select, partitionColumn, boundaries)
            .Select(s => (IDatasetPartition)new SqlServerPartition(connectionString, s))
            .ToArray();
    }

    internal static string[] BuildPartitionSelects(string select, string partitionColumn, string[] boundaries)
    {
        var quoted = MsDdl.Quote(partitionColumn);
        var n = boundaries.Length - 1;
        var selects = new string[n];
        for (var i = 0; i < n; i++)
        {
            var range = i == n - 1
                ? $"{quoted} >= {boundaries[i]} and {quoted} <= {boundaries[i + 1]}" // inclusive tail
                : $"{quoted} >= {boundaries[i]} and {quoted} < {boundaries[i + 1]}"; // [lo, hi)
            var predicate = i == 0
                ? $"({quoted} is null or ({range}))" // NULL bucket rides partition 0
                : $"({range})";
            selects[i] = $"select * from ({select}) q where ({predicate})";
        }

        return selects;
    }

    private static int ParsePartitionCount(DatasetSpec spec)
    {
        if (!spec.Options.TryGetValue("partitions", out var raw) || raw is null)
        {
            return MinPartitions;
        }

        int n;
        try
        {
            n = Convert.ToInt32(raw, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is FormatException or OverflowException or InvalidCastException)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': 'partitions' must be an integer, got '{raw}'", isTransient: false);
        }

        if (n < MinPartitions || n > MaxPartitions)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': 'partitions' must be between {MinPartitions} and {MaxPartitions}, got {n}",
                isTransient: false);
        }

        return n;
    }

    private async Task<(object? Min, object? Max)> ProbeRangeAsync(
        DatasetSpec spec, string select, string column, CancellationToken ct)
    {
        var quoted = MsDdl.Quote(column);
        await using var connection = new SqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await using var command = new SqlCommand($"select min({quoted}), max({quoted}) from ({select}) q", connection);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);

            var fieldType = reader.GetFieldType(0);
            var isDate = string.Equals(reader.GetDataTypeName(0), "date", StringComparison.OrdinalIgnoreCase);
            if (!isDate && !RangeBoundaries.IsOrderable(fieldType))
            {
                throw new PzConnectorException(
                    $"dataset '{spec.Dataset}': partition_column '{column}' has type '{fieldType.Name}', which is " +
                    "not orderable -- partition_column must be numeric (int, bigint, float, decimal) or temporal " +
                    "(date, datetime2, datetimeoffset)",
                    isTransient: false);
            }

            await reader.ReadAsync(ct).ConfigureAwait(false);
            // SqlClient reports `date` as CLR DateTime; fetch as DateOnly so boundary literals render
            // as date casts, not datetime2.
            object? Get(int i) => reader.IsDBNull(i) ? null
                : isDate ? reader.GetFieldValue<DateOnly>(i)
                : reader.GetValue(i);
            return (Get(0), Get(1));
        }
        catch (SqlException ex)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': partition_column '{column}' probe failed: {ex.Message}",
                ex.IsTransient, innerException: ex);
        }
    }

    public ValueTask DisposeAsync() => default;

    /// <summary>`pz cdc status`: the capture instance's min LSN (null = never enabled/primed)
    /// plus, when unhealthy, the same remediation lines <see cref="SqlServerCdc.ValidatePrerequisitesAsync"/>
    /// raises at read time (this already covers capture-job presence, so no separate job query is
    /// needed here). <see cref="ChangeCaptureStatus.RetainedBytes"/> is always null: SQL Server
    /// retention is governed by the cleanup job's window, not a byte count pz can query. Checks
    /// <see cref="SqlServerCdc.IsDbCdcEnabledAsync"/> FIRST and skips the min-lsn probe entirely when
    /// db-level cdc isn't enabled yet -- the `cdc` schema (and the function `fn_cdc_get_min_lsn` lives
    /// in) doesn't exist until `sp_cdc_enable_db` runs, so calling it in that state would fail for a
    /// reason unrelated to the query itself; any other <see cref="SqlException"/> here is a real
    /// connector failure and propagates as <see cref="PzConnectorException"/>, not a benign status.</summary>
    public async ValueTask<ChangeCaptureStatus> GetChangeCaptureStatusAsync(DatasetSpec spec, CancellationToken ct)
    {
        var instance = SqlServerCdc.CaptureInstance(spec);
        await using var connection = new SqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);

            var dbEnabled = await SqlServerCdc.IsDbCdcEnabledAsync(connection, ct).ConfigureAwait(false);
            var minLsn = dbEnabled
                ? await SqlServerCdc.GetMinLsnAsync(connection, instance, ct).ConfigureAwait(false)
                : null;

            var unmet = await SqlServerCdc.ValidatePrerequisitesAsync(connection, spec, ct).ConfigureAwait(false);

            var detail = new List<string>();
            if (minLsn is null)
            {
                detail.Add("capture instance not created yet -- first run creates it");
            }

            detail.AddRange(unmet);

            return new ChangeCaptureStatus(minLsn is not null && unmet.Count == 0, instance, null, detail);
        }
        catch (SqlException ex)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': cdc status query failed: {ex.Message}", ex.IsTransient, innerException: ex);
        }
    }

    /// <summary>`pz cdc drop`: deliberately a no-op. Disabling cdc on a SQL Server table
    /// (<c>sp_cdc_disable_table</c>) is a DBA-level, database-wide decision pz never makes unilaterally
    /// -- the CLI's `pz cdc drop` command prints the exact remediation statement instead and clears
    /// only pz's own sync-state entry.</summary>
    public ValueTask DropChangeCaptureStateAsync(DatasetSpec spec, CancellationToken ct) => default;
}

/// <summary>One independently readable slice: opens its own connection (sibling partitions drain
/// concurrently). SequentialAccess + SingleResult: the typed column plans read ordinals strictly in
/// order, letting SqlClient stream wide nvarchar values without whole-row buffering.
/// Manual enumerator: yield cannot live inside try/catch, so MoveNextAsync is wrapped alone —
/// mid-stream SqlExceptions surface classified, and `await using` disposes the connection on every
/// exit path including abandoned enumeration.</summary>
internal sealed class SqlServerPartition(string connectionString, string selectSql) : IDatasetPartition
{
    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        SqlDataReader reader;
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            var command = new SqlCommand(selectSql, connection);
            reader = await command.ExecuteReaderAsync(
                System.Data.CommandBehavior.SequentialAccess | System.Data.CommandBehavior.SingleResult, ct)
                .ConfigureAwait(false);
        }
        catch (SqlException ex)
        {
            throw new PzConnectorException($"sqlserver read failed: {ex.Message}", ex.IsTransient, innerException: ex);
        }

        var enumerator = SqlServerArrowReader.ReadBatchesAsync(reader, options, ct).GetAsyncEnumerator(ct);
        try
        {
            while (true)
            {
                bool moved;
                try
                {
                    moved = await enumerator.MoveNextAsync().ConfigureAwait(false);
                }
                catch (SqlException ex)
                {
                    throw new PzConnectorException(
                        $"sqlserver read failed mid-stream: {ex.Message}", ex.IsTransient, innerException: ex);
                }

                if (!moved)
                {
                    yield break;
                }

                yield return enumerator.Current;
            }
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
            await reader.DisposeAsync().ConfigureAwait(false);
        }
    }
}
