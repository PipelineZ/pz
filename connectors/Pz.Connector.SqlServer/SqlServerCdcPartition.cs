using System.Data;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.SqlServer;

/// <summary>The single cdc partition for one SQL Server dataset. Runs
/// either the first-run snapshot path (<see cref="DatasetSpec.PriorSyncState"/> null = first run or
/// --full-refresh) or the LSN-windowed poll path (PriorSyncState non-null) against the native
/// <c>cdc.fn_cdc_get_all_changes_&lt;instance&gt;</c> table function. Both emit the SAME change-row
/// contract (<c>_pz_op</c>/<c>_pz_lsn</c>/<c>_pz_changed_at</c> then the table columns) read through
/// the same <see cref="SqlServerArrowReader"/> path <see cref="SqlServerPartition"/> uses -- no bespoke
/// decode. Unlike Postgres's pgoutput poll (a live stream bounded by an idle timer), a SQL Server poll
/// is one bounded SELECT over <c>[@from, @to]</c>: @to is captured once at read start and the whole
/// window is read to completion, so there is no idle-timeout/partial-window concern.</summary>
internal sealed class SqlServerCdcPartition(string connectionString, DatasetSpec spec)
    : IDatasetPartition, ISyncStatePartition, IChangeCapturePartition
{
    private IReadOnlyList<string>? _keyColumns;
    private string? _syncCandidate;

    public IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, CancellationToken ct) =>
        spec.PriorSyncState is null ? SnapshotReadAsync(options, ct) : PollReadAsync(options, ct);

    public bool TryGetChangeKeyColumns(out IReadOnlyList<string>? keyColumns)
    {
        keyColumns = _keyColumns;
        return _keyColumns is { Count: > 0 };
    }

    public bool TryGetSyncStateCandidate(out string? candidate)
    {
        candidate = _syncCandidate;
        return _syncCandidate is not null;
    }

    // ---- first run / --full-refresh: snapshot the table, token = to_lsn captured before the read ----

    private async IAsyncEnumerable<RecordBatch> SnapshotReadAsync(
        BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var (schema, table) = SqlServerCdc.SchemaAndTable(spec);
        var instance = SqlServerCdc.CaptureInstance(spec);

        var connection = new SqlConnection(connectionString);
        SqlDataReader reader;
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await ValidatePrerequisitesOrThrowAsync(connection, ct).ConfigureAwait(false);

            // Captured FIRST (before the snapshot SELECT): the token this run persists, so the next
            // run's poll resumes exactly here -- rows committed after this instant are picked up by
            // that poll, never lost, and the snapshot possibly re-reading a row already inside
            // [snapshot, to] is harmless (merge idempotency).
            var toLsn = await SqlServerCdc.GetMaxLsnAsync(connection, ct).ConfigureAwait(false);
            if (toLsn is null || toLsn.All(b => b == 0))
            {
                throw new PzConnectorException(
                    $"dataset '{spec.Dataset}': sqlserver cdc: change data capture has not produced a max lsn " +
                    "yet -- ensure SQL Server Agent is running (MSSQL_AGENT_ENABLED=true in containers) and the " +
                    "capture job has run at least once",
                    isTransient: false);
            }

            _keyColumns = await SqlServerCdc.DiscoverKeyColumnsAsync(connection, schema, table, instance, ct)
                .ConfigureAwait(false);
            _syncCandidate = SqlServerCdc.FormatLsn(toLsn);

            var command = new SqlCommand(SqlServerCdc.SnapshotSelect(schema, table), connection);
            reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, ct).ConfigureAwait(false);
        }
        catch (SqlException ex)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw Wrap(ex);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        await foreach (var batch in DrainAsync(reader, connection, options, ct).ConfigureAwait(false))
        {
            yield return batch;
        }
    }

    // ---- poll: [@from, @to] window through the change table, retention-gap checked before reading ----

    private async IAsyncEnumerable<RecordBatch> PollReadAsync(
        BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var (schema, table) = SqlServerCdc.SchemaAndTable(spec);
        var instance = SqlServerCdc.CaptureInstance(spec);
        var priorLsn = SqlServerCdc.ParseLsn(spec.PriorSyncState!);

        var connection = new SqlConnection(connectionString);
        SqlDataReader? reader = null;
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await ValidatePrerequisitesOrThrowAsync(connection, ct).ConfigureAwait(false);

            // Retention check BEFORE reading, against the PRIOR token itself (not @from): if the
            // capture instance's oldest retained change is already past what we last consumed, the gap
            // in between is gone for good -- never silently skip it.
            var minLsn = await SqlServerCdc.GetMinLsnAsync(connection, instance, ct).ConfigureAwait(false);
            if (minLsn is not null && SqlServerCdc.CompareLsn(minLsn, priorLsn) > 0)
            {
                throw new PzConnectorException(
                    $"dataset '{spec.Dataset}': sqlserver cdc: retention gap -- changes were lost; run with " +
                    "--full-refresh to re-snapshot",
                    isTransient: false);
            }

            // @from is EXCLUSIVE of what was already consumed: fn_cdc_increment_lsn(prior) moves past
            // the prior token, since the change-window functions treat their from-bound as inclusive
            // and prior was already fully read last run.
            var fromLsn = await SqlServerCdc.IncrementLsnAsync(connection, priorLsn, ct).ConfigureAwait(false);
            // @to captured once, here, at read start -- the whole bounded window is read to completion
            // in one SELECT (no live-stream idle timer needed, unlike postgres's pgoutput poll).
            var toLsn = await SqlServerCdc.GetMaxLsnAsync(connection, ct).ConfigureAwait(false)
                ?? throw new PzConnectorException(
                    $"dataset '{spec.Dataset}': sqlserver cdc: change data capture has not produced a max lsn " +
                    "yet -- ensure SQL Server Agent is running (MSSQL_AGENT_ENABLED=true in containers)",
                    isTransient: false);

            _keyColumns = await SqlServerCdc.DiscoverKeyColumnsAsync(connection, schema, table, instance, ct)
                .ConfigureAwait(false);
            // Candidate always advances to @to, even for an empty window: nothing between (from, to]
            // was consumed because nothing exists there, so advancing skips no change -- watermark-free
            // progress is safe exactly like an empty incremental-extract window.
            _syncCandidate = SqlServerCdc.FormatLsn(toLsn);

            if (SqlServerCdc.CompareLsn(fromLsn, toLsn) > 0)
            {
                await connection.DisposeAsync().ConfigureAwait(false);
                yield break;
            }

            var dataColumns = await SqlServerCdc.ProbeBaseColumnsAsync(connection, schema, table, ct)
                .ConfigureAwait(false);
            var command = new SqlCommand(SqlServerCdc.BuildWindowSelect(instance, dataColumns), connection);
            command.Parameters.Add(new SqlParameter("@from", SqlDbType.Binary, 10) { Value = fromLsn });
            command.Parameters.Add(new SqlParameter("@to", SqlDbType.Binary, 10) { Value = toLsn });
            reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, ct).ConfigureAwait(false);
        }
        catch (SqlException ex)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw Wrap(ex);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        await foreach (var batch in DrainAsync(reader, connection, options, ct).ConfigureAwait(false))
        {
            yield return batch;
        }
    }

    // Manual-enumerator pattern (CS1626: no yield inside try/catch) -- mirrors SqlServerPartition and
    // PostgresCdcPartition: a mid-stream SqlException classifies as PzConnectorException, connection and
    // reader are disposed on every exit path (normal completion, mid-stream fault, or abandoned enumeration).
    private async IAsyncEnumerable<RecordBatch> DrainAsync(
        SqlDataReader reader, SqlConnection connection, BatchOptions options,
        [EnumeratorCancellation] CancellationToken ct)
    {
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
                    throw Wrap(ex);
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
            await connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task ValidatePrerequisitesOrThrowAsync(SqlConnection connection, CancellationToken ct)
    {
        var unmet = await SqlServerCdc.ValidatePrerequisitesAsync(connection, spec, ct).ConfigureAwait(false);
        if (unmet.Count > 0)
        {
            throw new PzConnectorException(
                $"sqlserver cdc: prerequisites not met for dataset '{spec.Dataset}' -- run:\n" +
                string.Join("\n", unmet),
                isTransient: false);
        }
    }

    private PzConnectorException Wrap(SqlException ex) =>
        new($"dataset '{spec.Dataset}': sqlserver cdc failed: {ex.Message}", ex.IsTransient, innerException: ex);
}
