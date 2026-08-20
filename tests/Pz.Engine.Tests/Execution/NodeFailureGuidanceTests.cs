using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary>PZ0501 is the engine's terminal catch-all, so its message is whatever the underlying
/// library said. The libraries pz embeds give remediation advice in their OWN vocabulary — DuckDB says
/// "SET threads=X", Sylvan says "increase the MaxBufferSize setting" — naming knobs a pz user cannot
/// reach. These map known failure signatures onto pz's own configuration surface.</summary>
public class NodeFailureGuidanceTests
{
    /// <summary>The wide-schema OOM. DuckDB's own advice is "SET threads=X" /
    /// "SET preserve_insertion_order=false"; neither is a pz key, and the second is measurably no help.
    /// The next step must name engine.duckdb.threads, which IS a pz key and which does fix it.</summary>
    [Fact]
    public void Out_of_memory_points_at_the_duckdb_memory_and_thread_keys()
    {
        var next = NodeFailureGuidance.NextStepFor(
            "Out of Memory Error: could not allocate block of size 256.0 KiB (1023.9 MiB/1.0 GiB used)");

        Assert.NotNull(next);
        Assert.Contains("engine.duckdb.memory_limit", next);
        Assert.Contains("engine.duckdb.threads", next);
    }

    /// <summary>A csv row past even the universal tier's raised ceiling, which is not infinite.
    /// Sylvan's own text names MaxBufferSize, which is not a pz key.</summary>
    [Fact]
    public void Row_too_large_describes_the_row_rather_than_the_library_knob()
    {
        var next = NodeFailureGuidance.NextStepFor("Row 1 was too large. Try increasing the MaxBufferSize setting.");

        Assert.NotNull(next);
        Assert.DoesNotContain("MaxBufferSize", next);
    }

    /// <summary>DuckDB's parallel csv reader refuses some very wide rows and advises "parallel = false",
    /// which pz does not expose. The universal tier reads far larger rows, so the actionable pz-side
    /// move is to route this dataset through it.</summary>
    [Fact]
    public void Parallel_csv_reader_refusal_points_at_the_universal_tier()
    {
        var next = NodeFailureGuidance.NextStepFor(
            "Not implemented Error: The Parallel CSV Reader currently does not support a full read on this file.");

        Assert.NotNull(next);
        Assert.Contains("force_universal", next);
    }

    /// <summary>An unrecognized failure gets no invented advice — a wrong next step is worse than none,
    /// and PZ0501 wraps every foreign exception in the engine.</summary>
    [Fact]
    public void Unrecognized_failure_gets_no_next_step()
    {
        Assert.Null(NodeFailureGuidance.NextStepFor("some connector blew up in a way pz has never seen"));
    }

    /// <summary>The guidance rides in the message itself rather than a separate field: node errors have
    /// no next-step channel (run_results.json writes only code+message, and node_completed carries only
    /// errorCode/errorMessage), so appending is what actually gets it to the console, the artifact, the
    /// NDJSON stream and the MCP envelope at once. Prefixed `pz:` so it is unmistakably pz's own voice
    /// and not more of the underlying library's text.</summary>
    [Fact]
    public void Annotate_appends_pz_guidance_below_the_original_message()
    {
        var annotated = NodeFailureGuidance.Annotate("Out of Memory Error: could not allocate block");

        Assert.StartsWith("Out of Memory Error: could not allocate block", annotated);
        Assert.Contains("\n\npz: ", annotated);
        Assert.Contains("engine.duckdb.threads", annotated);
    }

    /// <summary>An unrecognized message must come through byte-identical — this sits on the path every
    /// foreign exception in the engine takes, so it must not reshape errors it has nothing to say about.</summary>
    [Fact]
    public void Annotate_leaves_an_unrecognized_message_untouched()
    {
        const string message = "some connector blew up in a way pz has never seen";

        Assert.Equal(message, NodeFailureGuidance.Annotate(message));
    }
}
