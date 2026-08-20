namespace Pz.Engine.Execution;

/// <summary>Translates a terminal node failure's message into a next step
/// phrased in pz's own configuration vocabulary.
///
/// PZ0501 is the engine's catch-all wrap for any foreign exception, so its message is whatever the
/// underlying library said — and the libraries pz embeds hand out remediation advice naming THEIR knobs:
/// DuckDB suggests "SET threads=X" and "SET preserve_insertion_order=false", Sylvan suggests "increasing
/// the MaxBufferSize setting". None of those is a key a pz user can set, so such an error satisfies the
/// letter of "say what went wrong" while leaving the reader with no reachable move. Worse, DuckDB's
/// preserve_insertion_order suggestion measurably does NOT fix the wide-schema OOM it accompanies, so
/// passing it through unqualified actively misleads.
///
/// Deliberately a short, closed list of known signatures. An unrecognized
/// failure returns null and the message is left as-is: a wrong next step costs more than an absent one,
/// and this sits on the path every foreign exception in the engine takes.</summary>
internal static class NodeFailureGuidance
{
    /// <summary>Appends the guidance to the message itself, rather than filling PzError's NextStep. A node
    /// failure has no next-step channel to fill: <c>RunResultsWriter</c> writes only <c>code</c> and
    /// <c>message</c> for a node error, and <c>node_completed</c> carries only <c>errorCode</c>/
    /// <c>errorMessage</c> — so a populated NextStep would be silently dropped by every reader. Appending
    /// reaches the console, run_results.json, the NDJSON stream and the MCP envelope at once without
    /// touching either the events.md contract or the run-results artifact shape.
    ///
    /// The `pz:` prefix marks the sentence as pz's own voice: these messages already carry a wall of
    /// third-party text (DuckDB prints its own "Possible solutions:" list), and guidance that named pz
    /// keys while blending into that wall would read as more of the same library's advice.</summary>
    internal static string Annotate(string message) =>
        NextStepFor(message) is { } guidance ? $"{message}\n\npz: {guidance}" : message;

    internal static string? NextStepFor(string message)
    {
        if (message.Contains("Out of Memory Error", StringComparison.Ordinal))
        {
            // DuckDB's own memory floor for materialising a table scales with the table's column count
            // times its thread count, so a wide schema can exhaust a limit that looks generous next to
            // the data's size. engine.duckdb.threads is unset by default, which means DuckDB picks the
            // machine's core count -- naming it is the difference between a fix and a shrug.
            return "raise engine.duckdb.memory_limit, or lower engine.duckdb.threads (unset, DuckDB uses " +
                "the machine's core count; its memory floor scales with a table's columns times threads, " +
                "so wide schemas exhaust a limit that looks generous for the row count)";
        }

        if (message.Contains("was too large", StringComparison.Ordinal) &&
            message.Contains("MaxBufferSize", StringComparison.Ordinal))
        {
            return "a single csv row exceeded the reader's maximum row size; reduce the row's largest " +
                "column, or split the value across rows";
        }

        if (message.Contains("Parallel CSV Reader", StringComparison.Ordinal))
        {
            return "this csv has rows DuckDB's parallel reader will not scan; set engine.force_universal: " +
                "true to read it through pz's own csv reader, which handles far larger rows";
        }

        return null;
    }
}
