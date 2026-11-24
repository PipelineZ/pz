using System.Globalization;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Paths;
using Pz.DuckDb;

namespace Pz.Engine.Execution;

/// <summary>The spool tier of a SinkWrite: DuckDB COPYs the staged output to local files under the run directory
/// (one per partition folder when the plan names a partition column), the sink delivers them
/// (<see cref="ISpoolingSink.CommitSpoolAsync"/>), and the files are deleted whether delivery succeeded or not.
/// A folder is the plan's calendar template rendered from the column's UTC value, exactly as the Arrow path's
/// per-row rendering would; a null partition value fails the write, as it does there.</summary>
internal static class SpoolWrite
{
    private const char RecordSeparator = '\u001e';
    private const char UnitSeparator = '\u001f';

    public static async Task<WriteResult> RunAsync(RunContext ctx, string nodeId, string relation, SpoolPlan plan,
        Func<IReadOnlyList<SpooledFile>, Task<WriteResult>> commit, CancellationToken ct)
    {
        foreach (var statement in plan.SetupStatements)
        {
            await ctx.SetupLedger.ExecuteOnceAsync(statement, ct).ConfigureAwait(false);
        }

        var source = plan.NestedAsJson ? await StagedColumns.NestedAsJsonAsync(ctx.Duck, relation, ct).ConfigureAwait(false) : relation;
        var directory = Path.Combine(ctx.Paths.RunDir, "spool", nodeId);
        DeleteDirectory(directory);
        Directory.CreateDirectory(directory);
        try
        {
            var files = new List<SpooledFile>();
            foreach (var (folder, predicate, rows) in await FoldersAsync(ctx.Duck, source, plan, ct).ConfigureAwait(false))
            {
                var file = Path.Combine(directory, files.Count.ToString(CultureInfo.InvariantCulture) + "." + plan.Extension);
                await CopyAsync(ctx.Duck,
                    $"copy (select * from {source}{predicate}) to '{file.Replace("'", "''", StringComparison.Ordinal)}' ({plan.CopyOptions})",
                    ct).ConfigureAwait(false);
                files.Add(new SpooledFile(folder, file, rows));
            }

            return await commit(files).ConfigureAwait(false);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    /// <summary>One entry per file to write: its folder, the WHERE clause selecting its rows, and their count.
    /// Unpartitioned, one file holds every row, even none (an empty output still replaces the destination).</summary>
    private static async Task<IReadOnlyList<(string Folder, string Predicate, long Rows)>> FoldersAsync(
        IDuckSession duck, string source, SpoolPlan plan, CancellationToken ct)
    {
        if (plan.PartitionColumn is not { } column)
        {
            return [("", "", await duck.ScalarAsync<long>($"select count(*) from {source}", ct).ConfigureAwait(false))];
        }

        var quoted = ArrowInterop.QuoteQualified(column);
        var type = await duck.ScalarAsync<string>(
            $"select coalesce(max(column_type), '') from (describe select * from {source}) where column_name = '{Esc(column)}'",
            ct).ConfigureAwait(false);
        var instant = type switch
        {
            "" => throw new PzConnectorException(
                $"partition_by column '{column}' is not present in the write schema", isTransient: false),
            "TIMESTAMP WITH TIME ZONE" => $"timezone('UTC', {quoted})",
            "DATE" => quoted,
            _ when type.StartsWith("TIMESTAMP", StringComparison.Ordinal) => quoted,
            _ => throw new PzConnectorException(
                $"partition_by column '{column}' is not a timestamp/date column (got '{type}')", isTransient: false),
        };

        var nulls = await duck.ScalarAsync<long>($"select count(*) from {source} where {quoted} is null", ct).ConfigureAwait(false);
        if (nulls > 0)
        {
            throw new PzConnectorException(
                $"partition_by column '{column}' has a null value in {nulls} row(s); a null partition value cannot be routed to a partition folder",
                isTransient: false);
        }

        var folder = $"strftime({instant}, '{Esc(PathTemplate.ToStrftime(plan.PathTemplate ?? ""))}')";
        var packed = await duck.ScalarAsync<string>(
            $"select coalesce(string_agg(f || chr(31) || n, chr(30) order by f), '') from " +
            $"(select {folder} as f, count(*) as n from {source} group by f)", ct).ConfigureAwait(false);
        return packed.Length == 0
            ? []
            : packed.Split(RecordSeparator).Select(entry =>
            {
                var parts = entry.Split(UnitSeparator);
                return (parts[0], $" where {folder} = '{Esc(parts[0])}'", long.Parse(parts[1], CultureInfo.InvariantCulture));
            }).ToList();
    }

    /// <summary>Runs one COPY; a failure is reported without the statement (DuckDB's error context can echo it).</summary>
    private static async Task CopyAsync(IDuckSession duck, string sql, CancellationToken ct)
    {
        try
        {
            await duck.ExecuteAsync(sql, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException and not PzConnectorException)
        {
            throw new PzConnectorException(
                $"spool copy failed: {NativeStatementRedactor.SanitizeEngineMessage(ex.Message)}",
                isTransient: DuckTransientErrors.IsTransient(ex.Message), innerException: ex);
        }
    }

    private static string Esc(string text) => text.Replace("'", "''", StringComparison.Ordinal);

    private static void DeleteDirectory(string directory)
    {
        try
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
        catch (IOException)
        {
            // Best effort: a leftover spool lives under the run directory, never in the destination.
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
