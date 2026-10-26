using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Validation;
using Pz.TestSupport;

namespace Pz.Engine.Tests.Validation;

/// <summary>Tier 4 with project.yml's <c>extensions:</c>, against the jev community extension
/// (<c>jev_version()</c> answers locally, with no API call). Downloads it; skipped offline.</summary>
[Trait("Category", "DuckDbExtension")]
public sealed class SqlDryCompilerExtensionTests
{
    public SqlDryCompilerExtensionTests() => DockerFacts.SkipIfOffline();

    private static CompiledDag JevVersionDag()
    {
        const string sql = "select jev_version() as v";
        var def = new PipelineDef("ext", sql, "table", [], [], "pipelines/ext.sql");
        return new CompiledDag([new DagNode(new NodeId("1".PadLeft(16, '0')), NodeKind.Pipeline, "ext", [], sql, def)]);
    }

    [SkippableFact]
    public async Task A_listed_extension_function_dry_compiles()
    {
        var result = await SqlDryCompiler.RunAsync(JevVersionDag(), ["jev"], default);
        Assert.Empty(result.Errors);
    }

    [SkippableFact]
    public async Task An_unlisted_extension_function_fails_with_the_hint()
    {
        var result = await SqlDryCompiler.RunAsync(JevVersionDag(), [], default);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.SqlDryCompile, error.Code);
        Assert.Contains("extensions:", error.Hint);
    }

    [SkippableFact]
    public async Task An_extension_failure_stops_before_any_pipeline()
    {
        var result = await SqlDryCompiler.RunAsync(JevVersionDag(), ["pz_no_such_ext"], default);

        Assert.Equal(PzErrorCode.ExtensionLoadFailed, Assert.Single(result.Errors).Code);
    }
}
