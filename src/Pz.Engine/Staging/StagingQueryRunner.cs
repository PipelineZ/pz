using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Preview;

namespace Pz.Engine.Staging;

/// <summary>What <c>pz staging query</c> prints, in the field order of its JSON contract.
/// <see cref="Truncated"/> is true when the query had more rows than <see cref="Limit"/>.</summary>
public sealed record StagingQueryResult(string? RunId, IReadOnlyList<PreviewColumn> Columns,
    IReadOnlyList<IReadOnlyList<PreviewCell>> Rows, int Limit, bool Truncated);

/// <summary><c>pz staging query</c>: one read-only SELECT against a finished run's <c>staging.duckdb</c>,
/// read from the run directory alone. The session is the one <c>pz preview --run</c> reads with: the file
/// attached <c>READ_ONLY</c>, filesystem and network access off and the configuration locked before any
/// query runs, so the SQL can read the run's tables and nothing else. Staged relations are
/// <c>staging.&lt;name&gt;</c>.
///
/// <c>redact: true</c> marks are not applied: they belong to a node, and a query can join, rename and
/// compute columns, so there is no column to attach a node's mark to.</summary>
public static class StagingQueryRunner
{
    public const int MaxLimit = 500;

    public static async Task<StagingQueryResult> RunAsync(string runDir, string? runId, string sql, int limit,
        CancellationToken ct)
    {
        if (!Directory.Exists(runDir))
        {
            throw Unavailable($"run directory '{runDir}' does not exist");
        }

        var staging = Path.Combine(runDir, "staging.duckdb");
        if (!File.Exists(staging))
        {
            throw Unavailable($"run directory '{runDir}' has no staging.duckdb (it was cleaned)");
        }

        // Held for the whole query: retention cannot delete the file under us, and a run still writing it
        // is refused at once instead of blocking on DuckDB's own file lock.
        using var held = RunDirLock.TryAcquire(runDir)
            ?? throw Unavailable($"the run in '{runDir}' is still in progress");

        DuckSession duck;
        try
        {
            duck = DuckSession.OpenReadOnly(staging);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw Unavailable($"the run in '{runDir}' is still in progress or its staging.duckdb cannot be opened");
        }

        await using (duck)
        {
            IReadOnlyList<PreviewColumn> columns;
            IReadOnlyList<IReadOnlyList<PreviewCell>> rows;
            try
            {
                (columns, rows) = await PreviewSampler.SampleQueryAsync(duck, Normalize(sql), limit + 1, ct)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // The query is the inside of a subquery, so anything but one SELECT (a write, DDL, several
                // statements) surfaces as a parse error at its first keyword; say so, not just where.
                var reason = FirstLine(ex.Message);
                var what = reason.StartsWith("Parser Error", StringComparison.Ordinal)
                    ? "it is not a single SELECT, or it has a syntax error"
                    : "it did not run";
                throw new PzValidationException([new PzError(PzErrorCode.StagingQueryFailed,
                    $"pz staging query: {what}: {reason}", null, null,
                    "write one SELECT over the run's tables, named staging.<name>; " +
                    "`select * from duckdb_tables()` lists them")]);
            }

            var truncated = rows.Count > limit;
            return new StagingQueryResult(runId, columns, truncated ? [.. rows.Take(limit)] : rows, limit, truncated);
        }
    }

    /// <summary>The query as the inside of a subquery: surrounding whitespace and trailing semicolons
    /// removed, since a terminated statement is still one statement to the person who wrote it.</summary>
    public static string Normalize(string sql) => sql.Trim().TrimEnd(';', ' ', '\t', '\r', '\n');

    private static string FirstLine(string message) => message.Split('\n')[0].Trim();

    private static PzValidationException Unavailable(string reason) =>
        new([new PzError(PzErrorCode.StagingQueryUnavailable, $"pz staging query: {reason}.", null, null,
            "pick a finished run from `pz runs`")]);
}
