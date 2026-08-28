using System.Data.Common;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Snowflake.Data.Client;

namespace Pz.Connector.Snowflake;

/// <summary>Snowflake source: SELECT generation with column pruning, engine predicate pushdown,
/// watermark lower bound and bounded-window upper bound. PredicateSql is engine-generated and
/// trusted raw (documented ABI trust boundary); identifiers are never trusted and go through
/// SfDdl.Quote.</summary>
internal sealed class SnowflakeSource(string connectionString) : ISource
{
    internal static string BuildSelect(DatasetSpec spec, ReadHints hints)
    {
        var query = spec.Options.TryGetValue("query", out var q) ? q?.ToString() : null;
        if (query is not null)
        {
            return query; // query mode: user SQL verbatim, hints not applied
        }

        // There is no `table:`/`schema:` option -- the dataset name is the object name, qualified by
        // its own dot.
        var (schema, table) = SfDdl.SplitEntity(spec.Dataset);
        var columns = hints.Columns is { Count: > 0 }
            ? string.Join(", ", hints.Columns.Select(SfDdl.Quote))
            : "*";
        var sql = $"select {columns} from {SfDdl.Quote(schema)}.{SfDdl.Quote(table)}";

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
            predicates.Add($"({SfDdl.Quote(spec.WatermarkCursor)} {op} '{spec.WatermarkValue!.Replace("'", "''")}')");
        }

        if (spec.WatermarkCursor is not null && spec.WatermarkUpperBound is not null)
        {
            predicates.Add($"({SfDdl.Quote(spec.WatermarkCursor)} <= '{spec.WatermarkUpperBound.Replace("'", "''")}')");
        }

        // Each term self-parenthesized before the AND-join: a disjunctive engine pushdown must not
        // let the watermark's AND bind into the middle of its OR.
        return predicates.Count > 0
            ? $"{sql} where {string.Join(" and ", predicates.Select(p => $"({p})"))}"
            : sql;
    }

    public async ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        // limit 0 in a wrapper: the driver executes server-side but fetches no rows; wrapping keeps a
        // user query:'s own LIMIT/ORDER BY intact.
        var probe = $"select * from ({BuildSelect(spec, ReadHints.None)}) as pz_probe limit 0";
        await using var connection = new SnowflakeDbConnection { ConnectionString = connectionString };
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = probe;
            await using var reader = (DbDataReader)await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            return new DatasetSchema(SnowflakeArrowReader.BuildSchema(reader, $"dataset '{spec.Dataset}'"));
        }
        catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': schema probe failed: {ex.Message}", SfErrors.IsTransient(ex), innerException: ex);
        }
    }

    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        new([new SnowflakePartition(connectionString, BuildSelect(spec, hints), spec.Dataset)]);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>One independently readable slice: opens its own connection per read (single-partition --
/// Snowflake native scan/range partitioning is future work), wraps SnowflakeArrowReader.ReadBatchesAsync,
/// and disposes the connection on every exit path including abandoned enumeration.</summary>
internal sealed class SnowflakePartition(string connectionString, string selectSql, string dataset) : IDatasetPartition
{
    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        await using var connection = new SnowflakeDbConnection { ConnectionString = connectionString };
        DbDataReader reader;
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            var command = connection.CreateCommand();
            command.CommandText = selectSql;
            reader = (DbDataReader)await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
        {
            throw new PzConnectorException(
                $"dataset '{dataset}': read failed: {ex.Message}", SfErrors.IsTransient(ex), innerException: ex);
        }

        var enumerator = SnowflakeArrowReader.ReadBatchesAsync(reader, options, ct).GetAsyncEnumerator(ct);
        try
        {
            while (true)
            {
                bool moved;
                try
                {
                    moved = await enumerator.MoveNextAsync().ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
                {
                    throw new PzConnectorException(
                        $"dataset '{dataset}': read failed mid-stream: {ex.Message}", SfErrors.IsTransient(ex), innerException: ex);
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
