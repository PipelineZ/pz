using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Preview;

namespace Pz.Engine.Tests.Preview;

/// <summary>Reading a preview from a finished run's directory alone: every way the data can be missing is
/// PZ0537, never a raw DuckDB exception.</summary>
public sealed class PreviewRunReaderTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "pz-preview-reader-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static NodeId Id(string suffix) => new(suffix.PadLeft(16, '0'));

    /// <summary>A run dir whose staging holds <c>staging.stg</c> (120 rows, <c>contact</c> marked), a view
    /// <c>v</c> over it, and an index that also lists an ephemeral pipeline, a check and a sink.</summary>
    private async Task<string> RunDir(bool index = true, bool createStg = true, bool dropUnderView = false)
    {
        var dir = Path.Combine(_root, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        await using var duck = DuckSession.Open(Path.Combine(dir, "staging.duckdb"));
        await duck.ExecuteAsync("create schema staging");
        if (createStg)
        {
            await duck.ExecuteAsync(
                "create table staging.stg as select range as id, 'user' || range as contact from range(120)");
            await duck.ExecuteAsync("create view staging.v as select id from staging.stg");
            if (dropUnderView)
            {
                await duck.ExecuteAsync("drop table staging.stg cascade");
                await duck.ExecuteAsync("create table staging.base as select 1 as id");
                await duck.ExecuteAsync("create or replace view staging.v as select id from staging.base");
                await duck.ExecuteAsync("drop table staging.base");
            }
        }

        if (index)
        {
            var stg = new PipelineDef("stg", "select 1", "table", [], [], "pipelines/stg.sql") { RedactedColumns = ["contact"] };
            var v = new PipelineDef("v", "select 1", "view", [], [], "pipelines/v.sql");
            var eph = new PipelineDef("eph", "select 1", "ephemeral", [], [], "pipelines/eph.sql");
            var dag = new CompiledDag([
                new DagNode(Id("1"), NodeKind.Pipeline, "stg", [], "select 1", stg),
                new DagNode(Id("2"), NodeKind.Pipeline, "v", [Id("1")], "select 1", v),
                new DagNode(Id("3"), NodeKind.Check, "stg__not_null_id", [Id("1")], null,
                    new CheckNodeDef("stg", new CheckDef("not_null", ["id"], new Dictionary<string, object?>()))),
                new DagNode(Id("4"), NodeKind.SinkWrite, "lake__out", [Id("1")], null, new object()),
            ]);
            await PreviewIndex.WriteAsync(duck, dag, [stg, v, eph], default);
        }

        return dir;
    }

    private static async Task<PzError> Refused(string runDir, string node)
    {
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => PreviewRunReader.ReadAsync(runDir, "run-1", node, 50, default));
        return Assert.Single(ex.Errors);
    }

    [Fact]
    public async Task Reads_rows_total_and_marks_from_the_index()
    {
        var result = await PreviewRunReader.ReadAsync(await RunDir(), "run-1", "stg", 50, default);

        Assert.Equal("stg", result.Node);
        Assert.Equal("run", result.Mode);
        Assert.Equal("run-1", result.RunId);
        Assert.Equal(120, result.TotalRows);
        Assert.Equal(50, result.Limit);
        Assert.False(result.Sliced);
        Assert.Equal(50, result.Rows.Count);
        Assert.True(result.Columns.Single(c => c.Name == "contact").Redacted);
        Assert.All(result.Rows, r => Assert.Equal("•••", r[1].Text));
    }

    [Theory]
    [InlineData("dir")]
    [InlineData("file")]
    [InlineData("index")]
    [InlineData("relation")]
    public async Task Missing_dir_file_index_or_relation_is_PZ0537(string missing)
    {
        var dir = missing switch
        {
            "dir" => Path.Combine(_root, "absent"),
            "file" => Directory.CreateDirectory(Path.Combine(_root, "empty")).FullName,
            "index" => await RunDir(index: false),
            _ => await RunDir(createStg: false),
        };

        var error = await Refused(dir, "stg");

        Assert.Equal(PzErrorCode.PreviewUnavailable, error.Code);
        Assert.Contains("--fresh", error.Message + error.Hint, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("eph")]
    [InlineData("stg__not_null_id")]
    [InlineData("lake__out")]
    public async Task Excluded_node_is_PZ0538(string node) =>
        Assert.Equal(PzErrorCode.PreviewNodeNotPreviewable, (await Refused(await RunDir(), node)).Code);

    [Fact]
    public async Task Node_in_neither_table_is_SelectorNoMatch() =>
        Assert.Equal(PzErrorCode.SelectorNoMatch, (await Refused(await RunDir(), "nope")).Code);

    [Fact]
    public async Task A_held_run_lock_is_PZ0537_still_in_progress()
    {
        var dir = await RunDir();
        using var held = RunDirLock.Acquire(dir);

        var error = await Refused(dir, "stg");

        Assert.Equal(PzErrorCode.PreviewUnavailable, error.Code);
        Assert.Contains("still in progress", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_view_that_fails_to_evaluate_is_PZ0537() =>
        Assert.Equal(PzErrorCode.PreviewUnavailable, (await Refused(await RunDir(dropUnderView: true), "v")).Code);

    // The index is data from the run file: a relation that is not a plain staging identifier is refused,
    // never spliced into SQL.
    [Fact]
    public async Task A_tampered_relation_in_the_index_is_PZ0537()
    {
        var dir = await RunDir();
        await using (var duck = DuckSession.Open(Path.Combine(dir, "staging.duckdb")))
        {
            // The odd name exists, so only a shape check -- not the existence check -- can refuse it.
            await duck.ExecuteAsync("create table staging.\"stg limit 0; select 1\" as select 1 as id");
            await duck.ExecuteAsync(
                "update pz_meta.preview_nodes set relation = 'staging.stg limit 0; select 1' where node = 'stg'");
        }

        var error = await Refused(dir, "stg");
        Assert.Equal(PzErrorCode.PreviewUnavailable, error.Code);
        Assert.Contains("not a staging relation", error.Message, StringComparison.Ordinal);
    }
}
