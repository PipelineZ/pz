using System.Globalization;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Npgsql;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Shared.Partitioning;

namespace Pz.Connector.Postgres;

/// <summary>Postgres source: universal path only -- <see cref="TryGetNativeScan"/> always declines,
/// since no postgres_scanner native scan is wired up yet.
/// The entity name is the object name (there is no <c>table:</c> or <c>schema:</c> option); an
/// optional <c>query:</c> replaces the generated SELECT wholesale (see <see cref="BuildSelect"/>).
/// A real ADO.NET connection + <see cref="DataReaderSource"/> do the rest.
/// <see cref="PlanReadAsync"/> returns 1 partition unless the dataset declares <c>partition_column</c>
/// (<see cref="PostgresConnector.Capabilities"/> declares <see
/// cref="ConnectorCapabilities.PartitionedRead"/>). The partition-math contract:
/// a single min/max probe, N equal-width <c>[lo,hi)</c> ranges
/// with an inclusive final tail, a NULL bucket folded into partition 0, and degenerate ranges (min ==
/// max, or an empty/all-NULL column) collapsing to 1 partition.</summary>
internal sealed class PostgresSource(string connectionString) : ISource, IChangeCaptureAdmin
{
    private const int MinPartitions = 1;
    private const int MaxPartitions = 16;

    // The DataReaderSource type matrix's orderable CLR shapes (see PgTypeMap): every numeric and
    // temporal type postgres can surface via Npgsql. string/bool (text/boolean) are deliberately
    // excluded -- there is no meaningful equal-width range over them.
    private static readonly HashSet<Type> OrderablePartitionColumnTypes =
    [
        typeof(int), typeof(long), typeof(double), typeof(decimal),
        typeof(DateOnly), typeof(DateTime), typeof(DateTimeOffset),
    ];

    public async ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        // CDC: probe the snapshot projection's shape -- the three _pz_ header columns
        // prepended to the table's own columns -- so the schema matches exactly what the snapshot read
        // produces. Offline-safe: plain SQL, no prerequisite/slot work (that runs at read open).
        string select;
        if (spec.ChangeCapture)
        {
            var (cdcSchema, cdcTable) = PostgresCdc.SchemaAndTable(spec);
            select = PostgresCdc.SnapshotSelect(cdcSchema, cdcTable);
        }
        else
        {
            select = BuildSelect(spec, ReadHints.None);
        }

        await using var connection = new NpgsqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await using var command = new NpgsqlCommand($"select * from ({select}) q limit 0", connection);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            return new DatasetSchema(DataReaderSource.BuildArrowSchema(reader));
        }
        catch (NpgsqlException ex)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': schema query failed: {ex.Message}", ex.IsTransient, innerException: ex);
        }
    }

    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        // Universal-path only: no postgres_scanner native scan is wired up yet, so decline quietly
        // (never throw) so the universal path always applies, per the same contract CsvSource follows.
        scan = null;
        return false;
    }

    // Table-mode options that are meaningless (and so forbidden) on a cdc dataset: cdc reads a single
    // table directly under a snapshot / the pgoutput feed, not a custom query or a range-partitioned scan.
    private static readonly string[] CdcForbiddenOptions = ["query", "partition_column", "partitions"];

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
                        "-- a cdc dataset reads its entity's table directly; remove it",
                        isTransient: false, code: PzConnectorErrorCodes.Config);
                }
            }

            // Rejects a malformed entity name up front (SplitEntity throws). One partition: cdc is
            // inherently sequential (a single slot / snapshot cut).
            _ = PostgresCdc.SchemaAndTable(spec);
            return [new PostgresCdcPartition(connectionString, spec)];
        }

        var select = BuildSelect(spec, hints);

        var partitionColumn = spec.Options.TryGetValue("partition_column", out var pc) ? pc?.ToString() : null;
        if (partitionColumn is null)
        {
            return [new PostgresPartition(connectionString, select)];
        }

        var partitionCount = ParsePartitionCount(spec);
        if (partitionCount == MinPartitions)
        {
            return [new PostgresPartition(connectionString, select)];
        }

        var (min, max) = await ProbeRangeAsync(spec, select, partitionColumn, ct).ConfigureAwait(false);
        if (min is null || max is null || min.Equals(max))
        {
            // Degenerate range (min == max) or an empty/all-NULL partition_column: a single partition
            // reads every row and no boundary math is needed -- no row can be lost or duplicated.
            return [new PostgresPartition(connectionString, select)];
        }

        var boundaries = ComputeBoundaryLiterals(min, max, partitionCount);
        var quotedColumn = PgDdl.Quote(partitionColumn);
        var partitions = new List<IDatasetPartition>(partitionCount);
        for (var i = 0; i < partitionCount; i++)
        {
            // boundaries[i]/boundaries[i+1] are each computed exactly once and shared between adjacent
            // partitions (partition i's hi literal IS partition i+1's lo literal) -- the [lo, hi) vs
            // [lo, hi] split below is what makes a boundary value belong to exactly one partition.
            var lo = boundaries[i];
            var hi = boundaries[i + 1];
            var range = i == partitionCount - 1
                ? $"{quotedColumn} >= {lo} and {quotedColumn} <= {hi}" // inclusive tail
                : $"{quotedColumn} >= {lo} and {quotedColumn} < {hi}"; // [lo, hi)
            var predicate = i == 0
                ? $"({quotedColumn} is null or ({range}))" // NULL bucket rides partition 0 -- no row lost
                : range;
            partitions.Add(new PostgresPartition(connectionString, $"select * from ({select}) q where ({predicate})"));
        }

        return partitions;
    }

    private static int ParsePartitionCount(DatasetSpec spec)
    {
        if (!spec.Options.TryGetValue("partitions", out var raw) || raw is null)
        {
            return MinPartitions;
        }

        if (!TryParseInt32(raw, out var n))
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': 'partitions' must be an integer, got '{raw}'", isTransient: false, code: PzConnectorErrorCodes.Config);
        }

        if (n < MinPartitions || n > MaxPartitions)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': 'partitions' must be between {MinPartitions} and {MaxPartitions}, got {n}",
                isTransient: false, code: PzConnectorErrorCodes.Config);
        }

        return n;
    }

    // TryParse pattern (not Convert.ToInt32, which throws a raw, unnamed FormatException for a
    // non-integer-shaped string like "four") -- every failure mode below is folded into a single `false`
    // so the caller can surface one named PzConnectorException instead.
    private static bool TryParseInt32(object raw, out int value)
    {
        switch (raw)
        {
            case int i:
                value = i;
                return true;
            case long l when l is >= int.MinValue and <= int.MaxValue:
                value = (int)l;
                return true;
            case long or double or decimal:
                value = 0;
                return false;
            default:
                return int.TryParse(
                    Convert.ToString(raw, CultureInfo.InvariantCulture), NumberStyles.Integer,
                    CultureInfo.InvariantCulture, out value);
        }
    }

    private async Task<(object? Min, object? Max)> ProbeRangeAsync(
        DatasetSpec spec, string select, string column, CancellationToken ct)
    {
        var quoted = PgDdl.Quote(column);
        await using var connection = new NpgsqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await using var command = new NpgsqlCommand($"select min({quoted}), max({quoted}) from ({select}) q", connection);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);

            var fieldType = reader.GetFieldType(0);
            if (!OrderablePartitionColumnTypes.Contains(fieldType))
            {
                throw new PzConnectorException(
                    $"dataset '{spec.Dataset}': partition_column '{column}' has type '{fieldType.Name}', which is " +
                    "not orderable -- partition_column must be numeric (integer, bigint, double precision, " +
                    "numeric) or temporal (date, timestamp, timestamptz)",
                    isTransient: false);
            }

            await reader.ReadAsync(ct).ConfigureAwait(false);
            object? min = reader.IsDBNull(0) ? null : reader.GetValue(0);
            object? max = reader.IsDBNull(1) ? null : reader.GetValue(1);
            return (min, max);
        }
        catch (NpgsqlException ex)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': partition_column '{column}' probe failed: {ex.Message}",
                ex.IsTransient, innerException: ex);
        }
    }

    /// <summary>Returns <paramref name="n"/>+1 SQL literal strings <c>boundaries[0..n]</c>:
    /// <c>boundaries[0]</c> is the exact min, <c>boundaries[n]</c> is the exact max, interior values are
    /// equal-width interpolations. Dispatches on the min/max value's runtime CLR type (already validated
    /// orderable by <see cref="ProbeRangeAsync"/>).</summary>
    internal static string[] ComputeBoundaryLiterals(object min, object max, int n) => min switch
    {
        int or long => BuildIntegerBoundaries(
            Convert.ToInt64(min, CultureInfo.InvariantCulture), Convert.ToInt64(max, CultureInfo.InvariantCulture), n),
        double => BuildDoubleBoundaries((double)min, (double)max, n),
        decimal => BuildDecimalBoundaries((decimal)min, (decimal)max, n),
        DateOnly => BuildDateBoundaries((DateOnly)min, (DateOnly)max, n),
        DateTime => BuildTimestampBoundaries((DateTime)min, (DateTime)max, n),
        DateTimeOffset => BuildTimestamptzBoundaries((DateTimeOffset)min, (DateTimeOffset)max, n),
        _ => throw new InvalidOperationException(
            $"unreachable: {min.GetType()} should already have been rejected as non-orderable"),
    };

    private static string[] BuildIntegerBoundaries(long min, long max, int n) =>
        EqualWidth.Longs(min, max, n).Select(b => b.ToString(CultureInfo.InvariantCulture)).ToArray();

    private static string[] BuildDoubleBoundaries(double min, double max, int n) =>
        EqualWidth.Doubles(min, max, n).Select(b => b.ToString("G17", CultureInfo.InvariantCulture)).ToArray();

    private static string[] BuildDecimalBoundaries(decimal min, decimal max, int n) =>
        EqualWidth.Decimals(min, max, n).Select(b => b.ToString(CultureInfo.InvariantCulture)).ToArray();

    private static string[] BuildDateBoundaries(DateOnly min, DateOnly max, int n) =>
        EqualWidth.Longs(min.DayNumber, max.DayNumber, n)
            .Select(day => $"date '{DateOnly.FromDayNumber((int)day).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)}'")
            .ToArray();

    // pg `timestamp` (no time zone) is trusted-UTC per PgTypeMap -- ticks arithmetic needs no
    // time zone conversion, only the two endpoints' wall-clock values.
    private static string[] BuildTimestampBoundaries(DateTime min, DateTime max, int n) =>
        EqualWidth.Longs(min.Ticks, max.Ticks, n)
            .Select(ticks => $"timestamp '{new DateTime(ticks, DateTimeKind.Unspecified)
                .ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)}'")
            .ToArray();

    // Explicit UTC conversion (not merely trusting Npgsql's default session time zone) -- probe and
    // boundary math both operate in UTC regardless of the connection's configured time zone.
    private static string[] BuildTimestamptzBoundaries(DateTimeOffset min, DateTimeOffset max, int n) =>
        EqualWidth.Longs(min.UtcDateTime.Ticks, max.UtcDateTime.Ticks, n)
            .Select(ticks => $"timestamptz '{new DateTime(ticks, DateTimeKind.Utc)
                .ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)}+00'")
            .ToArray();

    public ValueTask DisposeAsync() => default;

    /// <summary>`pz cdc status`: the slot row plus, when either the slot
    /// is missing or a prerequisite has regressed since the last healthy run, the same remediation lines
    /// <see cref="PostgresCdc.ValidatePrerequisitesAsync"/> raises at read time -- so an operator sees
    /// exactly what to fix without having to trigger a real run first.</summary>
    public async ValueTask<ChangeCaptureStatus> GetChangeCaptureStatusAsync(DatasetSpec spec, CancellationToken ct)
    {
        var slot = PostgresCdc.SlotName(spec);
        await using var connection = new NpgsqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            var unmet = await PostgresCdc.ValidatePrerequisitesAsync(connection, spec, ct).ConfigureAwait(false);

            await using var command = new NpgsqlCommand(
                "select confirmed_flush_lsn, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) as retained_bytes " +
                "from pg_replication_slots where slot_name = @name", connection);
            command.Parameters.AddWithValue("name", slot);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            var exists = await reader.ReadAsync(ct).ConfigureAwait(false);
            long? retainedBytes = exists && !reader.IsDBNull(1)
                ? Convert.ToInt64(reader.GetDecimal(1), CultureInfo.InvariantCulture)
                : null;
            await reader.DisposeAsync().ConfigureAwait(false);

            var detail = new List<string>();
            if (!exists)
            {
                // A missing slot means opposite things either side of the first run: before it, the slot is
                // simply not created yet; after it, the slot pz was resuming from is gone and every change
                // it held WAL for is unrecoverable. The stored token is what distinguishes them.
                detail.Add(spec.PriorSyncState is null
                    ? "slot not created yet -- first run creates it"
                    : $"slot '{slot}' is GONE but pz still holds a sync token -- changes since that token " +
                      $"are unrecoverable; run `pz cdc drop {spec.Source}.{spec.Dataset}` then " +
                      "`pz run --full-refresh` to re-snapshot");
            }

            detail.AddRange(unmet);

            return new ChangeCaptureStatus(exists && unmet.Count == 0, slot, retainedBytes, detail);
        }
        catch (NpgsqlException ex)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': cdc status query failed: {ex.Message}", ex.IsTransient, innerException: ex);
        }
    }

    /// <summary>`pz cdc drop`: drops the replication slot server-side (a missing slot is a
    /// no-op, per <see cref="PostgresCdc.DropSlotIfExistsAsync"/>). Clearing pz's own sync-state entry
    /// is the CLI's job, not this connector's -- this call only tears down server-side state.</summary>
    public async ValueTask DropChangeCaptureStateAsync(DatasetSpec spec, CancellationToken ct)
    {
        var slot = PostgresCdc.SlotName(spec);
        await using var connection = new NpgsqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await PostgresCdc.DropSlotIfExistsAsync(connection, slot, ct).ConfigureAwait(false);
        }
        catch (NpgsqlException ex)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': cdc drop failed: {ex.Message}", ex.IsTransient, innerException: ex);
        }
    }

    internal static string BuildSelect(DatasetSpec spec, ReadHints hints)
    {
        var query = spec.Options.TryGetValue("query", out var q) ? q?.ToString() : null;
        if (query is not null)
        {
            return query; // query mode: hints deliberately ignored
        }

        // No `table:`/`schema:` option -- the dataset name is the object name, qualified by its own dot.
        var (schema, table) = PgDdl.SplitEntity(spec.Dataset);
        var columns = hints.Columns is { Count: > 0 }
            ? string.Join(", ", hints.Columns.Select(PgDdl.Quote))
            : "*";
        var sql = $"select {columns} from {PgDdl.Quote(schema)}.{PgDdl.Quote(table)}";
        // Trust boundary (mirrors ReadHints' own doc comment): PredicateSql is engine-generated SQL, not
        // end-user/attacker input -- the planner builds it from the pipeline's own predicate pushdown
        // logic, so it is concatenated raw rather than parameterized. Identifiers (schema/table/columns)
        // are NOT similarly trusted and go through Quote above.
        var predicates = new List<string>(2);
        if (hints.PredicateSql is { Length: > 0 } predicate)
        {
            predicates.Add(predicate);
        }

        // Watermark pushdown joins the SAME AND-chain as the pushdown
        // predicate above -- a single-quoted (quote-doubled) literal, untyped: postgres infers the
        // literal's type from the column (digits -> numeric, ISO date/timestamp string -> date/timestamp/
        // timestamptz), which is exact for every canonical form WatermarkValue can take. Values are
        // already engine-canonicalized (digits/ISO only); the escaping here is defensive depth, per the
        // same injection-safety discipline as the rest of this file.
        // The lower bound operator is dynamic, controlled by WatermarkLowerInclusive.
        // Gated on WatermarkValue (not WatermarkCursor alone), per DatasetSpec.WatermarkCursor's doc
        // comment ("when set, alongside WatermarkValue"): SpecBuilder stamps WatermarkCursor on every
        // incremental dataset's spec, even on a first run with no stored watermark yet, so a
        // cursor-set/value-null spec is a real, expected shape here -- it must fall through to the
        // same unfiltered SELECT as a watermark-free spec, not dereference a null WatermarkValue.
        if (spec is { WatermarkCursor: not null, WatermarkValue: not null })
        {
            var op = spec.WatermarkLowerInclusive ? ">=" : ">";
            predicates.Add($"{PgDdl.Quote(spec.WatermarkCursor)} {op} '{spec.WatermarkValue!.Replace("'", "''")}'");
        }

        // The bounded-window upper bound joins the SAME AND-chain as
        // the lower-bound predicate above -- same quote-doubled untyped-literal discipline, same
        // BoundedWindow-capable-connector contract (DatasetSpec.WatermarkUpperBound's doc comment).
        if (spec.WatermarkCursor is not null && spec.WatermarkUpperBound is not null)
        {
            predicates.Add($"{PgDdl.Quote(spec.WatermarkCursor)} <= '{spec.WatermarkUpperBound.Replace("'", "''")}'");
        }

        // Each term is individually parenthesized before the AND-join (SQL's AND binds tighter than OR):
        // a term carrying a top-level OR (e.g. a disjunctive PredicateSql pushdown) must not let the
        // watermark's `cursor > 'v'` AND bind into the middle of the OR -- `(a or b) and (cursor > 'v')`,
        // not `a or (b and cursor > 'v')`. With every term self-wrapped, the outer `where (...)` paren
        // pair is redundant and is deliberately omitted (so a single predicate stays `where (id > 10)`,
        // not `where ((id > 10))`).
        return predicates.Count > 0
            ? $"{sql} where {string.Join(" and ", predicates.Select(p => $"({p})"))}"
            : sql;
    }
}

/// <summary>One independently readable slice of a postgres dataset (the whole dataset when
/// unpartitioned, or one range-narrowed slice of it otherwise): opens its own connection so the
/// partition is independently readable/disposable per the <see cref="IDatasetPartition"/> contract --
/// this is what lets sibling partitions be read concurrently.</summary>
internal sealed class PostgresPartition(string connectionString, string selectSql) : IDatasetPartition
{
    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        // `await using` here (mirroring GetSchemaAsync's pattern) guarantees the connection is disposed on
        // EVERY exit path -- normal completion, a wrapped NpgsqlException below, an *unwrapped* exception
        // (e.g. OperationCanceledException from `ct` firing mid-OpenAsync, which is not an NpgsqlException,
        // so a catch-and-dispose-only-on-NpgsqlException block would leak the connection there), or the
        // caller abandoning enumeration early. The compiler lowers `await using` in an
        // iterator into a finally block on the generated state machine's Dispose, so this holds even
        // though the method yields.
        await using var connection = new NpgsqlConnection(connectionString);
        NpgsqlDataReader reader;
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            var command = new NpgsqlCommand(selectSql, connection);
            reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        }
        catch (NpgsqlException ex)
        {
            throw new PzConnectorException($"postgres read failed: {ex.Message}", ex.IsTransient, innerException: ex);
        }

        await foreach (var batch in PgBatchStream.ReadAsync(
            reader, options.TargetBatchBytes, "postgres read failed mid-stream", ct).ConfigureAwait(false))
        {
            yield return batch;
        }
    }
}

/// <summary>Drains an open <see cref="NpgsqlDataReader"/> into Arrow batches and owns the reader from
/// here on (disposed on every exit path). CS1626 forbids a <c>yield return</c> inside a try/catch, but
/// not one that merely follows it, so the inner enumerator is driven by hand: only
/// <c>MoveNextAsync</c> sits inside the catch, which lets a fault Npgsql raises mid-stream surface as a
/// classified <see cref="PzConnectorException"/> (same <c>IsTransient</c> rule as the connection-open
/// catches) instead of escaping as a raw <see cref="NpgsqlException"/>.</summary>
internal static class PgBatchStream
{
    public static async IAsyncEnumerable<RecordBatch> ReadAsync(
        NpgsqlDataReader reader, int targetBatchBytes, string midStreamFailure,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var enumerator = DataReaderSource.ReadBatchesAsync(reader, targetBatchBytes, ct).GetAsyncEnumerator(ct);
        try
        {
            while (true)
            {
                bool moved;
                try
                {
                    moved = await enumerator.MoveNextAsync().ConfigureAwait(false);
                }
                catch (NpgsqlException ex)
                {
                    throw new PzConnectorException($"{midStreamFailure}: {ex.Message}", ex.IsTransient, innerException: ex);
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
