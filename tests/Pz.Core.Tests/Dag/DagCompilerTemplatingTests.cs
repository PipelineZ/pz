using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

/// <summary>DagCompiler compile-time validation of
/// date-templated dataset paths (PZ0217/PZ0218/PZ0221) and partitioned-output config (PZ0219). Mirrors
/// the PZ0212/PZ0213 <c>WindowedProject</c> fixture-construction style in <see cref="DagCompilerTests"/>
/// -- a single source dataset wired into a minimal project via a pipeline referencing
/// source('crm', 'orders'), so stage-0 validation runs over it exactly as PZ0212/PZ0213 do.</summary>
public class DagCompilerTemplatingTests
{
    /// <summary>A localfiles dataset with a date-templated <c>path</c>, optionally declaring an
    /// <c>incremental:</c> block and/or a <c>columns:</c> contract -- mirrors
    /// <see cref="DagCompilerTests"/>'s <c>WindowedProject</c> helper, swapping <c>table:</c>/
    /// <c>query:</c> for <c>path:</c>.</summary>
    private static PzProject TemplatedProject(string path, IncrementalDef? incremental = null,
        IReadOnlyDictionary<string, string>? columns = null)
    {
        var dataset = new DatasetDef("orders",
            new Dictionary<string, object?> { ["path"] = path, ["format"] = "parquet" },
            columns ?? new Dictionary<string, string> { ["ts"] = "timestamp" },
            incremental is null ? null : new SyncModeDef(SyncMode.Incremental, incremental));
        var source = new ConnectionDef("crm", "localfiles", new Dictionary<string, object?> { ["root"] = "/data" },
            [dataset], "connections.yml");
        return Project(
            [Pipe("stg", "select * from {{ source('crm', 'orders') }}")],
            sources: [source]);
    }

    /// <summary>A single sink output with a (possibly date-templated) <c>path</c> and an optional
    /// <c>partition_by</c> -- mirrors <see cref="TemplatedProject"/>'s dataset-side fixture, swapping the
    /// source dataset's <c>path</c>/<c>incremental</c> for the output's <c>path</c>/<c>partition_by</c>.
    /// The output is bound to pipeline 'stg', which itself reads source('crm', 'orders'), so the same
    /// stage-0 pass that validates the source dataset above also runs over this sink's outputs.</summary>
    private static PzProject TemplatedOutputProject(string outputPath, string? partitionBy)
    {
        var source = new ConnectionDef("crm", "localfiles", new Dictionary<string, object?> { ["root"] = "/data" },
            [new DatasetDef("orders", new Dictionary<string, object?> { ["path"] = "orders.csv", ["format"] = "csv" },
                null)],
            "connections.yml");

        // Path/partition_by are connector write options, so they are sink() kwargs -- and the pipeline
        // must carry the INSERT INTO, because an output exists only where a call site declares it.
        var partitionKwarg = partitionBy is null ? "" : $", partition_by: '{partitionBy}'";
        var insert = "INSERT INTO {{ sink('lake', 'orders', strategy: 'replace', format: 'parquet', " +
            $"path: '{outputPath}'{partitionKwarg}) }}}}\n";

        return Project(
            [Pipe("stg", insert + "select * from {{ source('crm', 'orders') }}")],
            sources: [source],
            sinks: [Sink()]);
    }

    /// <summary>Compiles and returns the aggregated errors -- empty if compile succeeds -- so the same
    /// helper serves both the error-case tests and the all-green case.</summary>
    private static IReadOnlyList<PzError> CompileAndCollectErrors(PzProject project)
    {
        try
        {
            DagCompiler.Compile(project, Ctx(project));
            return [];
        }
        catch (PzValidationException ex)
        {
            return ex.Errors;
        }
    }

    [Fact]
    public void Templated_path_without_date_cursor_is_PZ0217()
    {
        var project = TemplatedProject("e/{yyyy}/{MM}/*.parquet");
        var errors = CompileAndCollectErrors(project);
        Assert.Contains(errors, e => e.Code == "PZ0217" && e.Message.Contains("date cursor"));
    }

    [Fact]
    public void Templated_path_without_date_cursor_hint_shows_sync_block_not_retired_incremental_surface()
    {
        // The hint must show the unified `sync:` block, not the retired top-level `incremental:` one.
        var project = TemplatedProject("e/{yyyy}/{MM}/*.parquet");
        var errors = CompileAndCollectErrors(project);
        var error = Assert.Single(errors, e => e.Code == "PZ0217");
        Assert.Contains("sync:\n  mode: incremental\n  cursor: <column>", error.Hint);
        Assert.DoesNotContain("incremental:\n", error.Hint);
    }

    [Fact]
    public void Malformed_tokens_is_PZ0218()
    {
        // {yyyy} -> {dd} skips {MM}: not a contiguous coarse->fine run.
        var columns = new Dictionary<string, string> { ["ts"] = "timestamp" };
        var project = TemplatedProject("e/{yyyy}/{dd}/*.parquet", new IncrementalDef("ts"), columns);
        var errors = CompileAndCollectErrors(project);
        Assert.Contains(errors, e => e.Code == "PZ0218");
    }

    [Fact]
    public void Templated_path_without_bounded_window_is_PZ0221()
    {
        var columns = new Dictionary<string, string> { ["ts"] = "timestamp" };
        var project = TemplatedProject("e/{yyyy}/{MM}/{dd}/*.parquet", new IncrementalDef("ts"), columns);
        var errors = CompileAndCollectErrors(project);
        Assert.Contains(errors, e => e.Code == "PZ0221" && e.Message.Contains("bounded window"));
    }

    [Fact]
    public void Templated_path_without_bounded_window_hint_shows_sync_block_not_retired_incremental_surface()
    {
        // The hint must show the unified `sync:` block, not the retired top-level `incremental:` one.
        var columns = new Dictionary<string, string> { ["ts"] = "timestamp" };
        var project = TemplatedProject("e/{yyyy}/{MM}/{dd}/*.parquet", new IncrementalDef("ts"), columns);
        var errors = CompileAndCollectErrors(project);
        var error = Assert.Single(errors, e => e.Code == "PZ0221");
        Assert.Contains("sync:\n  mode: incremental\n  cursor: <column>\n  max_window:", error.Hint);
        Assert.DoesNotContain("incremental:\n", error.Hint);
    }

    [Fact]
    public void Templated_path_with_date_cursor_and_bounded_window_is_valid()
    {
        var columns = new Dictionary<string, string> { ["ts"] = "timestamp" };
        var incremental = new IncrementalDef("ts", MaxWindow: "1d", Initial: "2020-01-01");
        var project = TemplatedProject("e/{yyyy}/{MM}/{dd}/*.parquet", incremental, columns);
        var errors = CompileAndCollectErrors(project);
        Assert.DoesNotContain(errors, e => e.Code is "PZ0217" or "PZ0218" or "PZ0221");
    }

    [Fact]
    public void Partition_by_without_date_tokens_in_output_path_is_PZ0219()
    {
        var project = TemplatedOutputProject("e/orders/*.parquet", "ts");
        var errors = CompileAndCollectErrors(project);
        Assert.Contains(errors, e => e.Code == "PZ0219" && e.Message.Contains("partition_by"));
    }

    [Fact]
    public void Date_templated_output_path_without_partition_by_is_PZ0219()
    {
        var project = TemplatedOutputProject("e/{yyyy}/{MM}/{dd}/*.parquet", null);
        var errors = CompileAndCollectErrors(project);
        Assert.Contains(errors, e => e.Code == "PZ0219" && e.Message.Contains("partition_by"));
    }

    [Fact]
    public void Malformed_output_path_tokens_is_PZ0218()
    {
        // {yyyy} -> {dd} skips {MM}: not a contiguous coarse->fine run.
        var project = TemplatedOutputProject("e/{yyyy}/{dd}/*.parquet", "ts");
        var errors = CompileAndCollectErrors(project);
        Assert.Contains(errors, e => e.Code == "PZ0218");
    }

    [Fact]
    public void Partition_by_with_date_templated_output_path_is_valid()
    {
        var project = TemplatedOutputProject("e/{yyyy}/{MM}/{dd}/*.parquet", "ts");
        var errors = CompileAndCollectErrors(project);
        Assert.DoesNotContain(errors, e => e.Code is "PZ0218" or "PZ0219");
    }

    [Fact]
    public void Merge_key_token_in_output_path_is_not_a_date_token()
    {
        // The http sink's merge mode REQUIRES a {key} token in path: (the row's key value is
        // substituted in per request). The calendar-token pass must not claim it as PZ0218/PZ0219.
        var project = TemplatedOutputProject("/anything/comments/{key}", null);
        var errors = CompileAndCollectErrors(project);
        Assert.DoesNotContain(errors, e => e.Code is "PZ0218" or "PZ0219");
    }

    /// <summary>A non-templated project whose dataset carries a `files_per_partition` option -- for
    /// PZ0222 validation. Reuses <see cref="TemplatedProject"/>'s
    /// fixture shape (source 'crm', dataset 'orders') but with a plain, non-templated `path` and no
    /// `incremental:`, so only PZ0222 (never PZ0217/PZ0218/PZ0221) is in play.</summary>
    private static PzProject FilesPerPartitionProject(object? filesPerPartition)
    {
        var options = new Dictionary<string, object?> { ["path"] = "orders/*.parquet", ["format"] = "parquet" };
        if (filesPerPartition is not null)
        {
            options["files_per_partition"] = filesPerPartition;
        }

        var dataset = new DatasetDef("orders", options, new Dictionary<string, string> { ["id"] = "int" });
        var source = new ConnectionDef("crm", "localfiles", new Dictionary<string, object?> { ["root"] = "/data" },
            [dataset], "connections.yml");
        return Project(
            [Pipe("stg", "select * from {{ source('crm', 'orders') }}")],
            sources: [source]);
    }

    [Fact]
    public void Files_per_partition_zero_is_PZ0222()
    {
        var project = FilesPerPartitionProject(0);
        var errors = CompileAndCollectErrors(project);
        Assert.Contains(errors, e => e.Code == "PZ0222" && e.Message.Contains("crm.orders"));
    }

    [Fact]
    public void Files_per_partition_negative_is_PZ0222()
    {
        var project = FilesPerPartitionProject(-1);
        var errors = CompileAndCollectErrors(project);
        Assert.Contains(errors, e => e.Code == "PZ0222" && e.Message.Contains("crm.orders"));
    }

    [Fact]
    public void Files_per_partition_non_integer_is_PZ0222()
    {
        var project = FilesPerPartitionProject("abc");
        var errors = CompileAndCollectErrors(project);
        Assert.Contains(errors, e => e.Code == "PZ0222" && e.Message.Contains("crm.orders"));
    }

    [Fact]
    public void Files_per_partition_valid_positive_int_compiles_clean()
    {
        var project = FilesPerPartitionProject(50);
        var errors = CompileAndCollectErrors(project);
        Assert.DoesNotContain(errors, e => e.Code == "PZ0222");
    }

    [Fact]
    public void Files_per_partition_absent_compiles_clean()
    {
        var project = FilesPerPartitionProject(null);
        var errors = CompileAndCollectErrors(project);
        Assert.DoesNotContain(errors, e => e.Code == "PZ0222");
    }
}
