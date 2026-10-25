using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.TestSupport;

namespace Pz.DuckDb.Tests;

/// <summary>Extension loading that downloads from DuckDB's repositories; skipped offline.</summary>
[Trait("Category", "DuckDbExtension")]
public sealed class DuckExtensionsDownloadTests
{
    public DuckExtensionsDownloadTests() => DockerFacts.SkipIfOffline();

    [SkippableFact]
    public async Task A_community_extension_installs_and_loads()
    {
        await using var duck = DuckSession.Open(":memory:");

        var result = await DuckExtensions.LoadAsync(duck, ["jev"], CancellationToken.None);

        Assert.Empty(result.Errors);
        Assert.Equal("jev", Assert.Single(result.Loaded).Name);
        // Not pinned: the community build changes under us.
        Assert.False(string.IsNullOrEmpty(await duck.ScalarAsync<string>("select jev_version()")));
    }

    [SkippableFact]
    public async Task Every_failure_is_reported()
    {
        await using var duck = DuckSession.Open(":memory:");
        var platform = await duck.ScalarAsync<string>("select platform from pragma_platform()");

        var result = await DuckExtensions.LoadAsync(duck, ["pz_no_such_ext_a", "pz_no_such_ext_b"],
            CancellationToken.None);

        Assert.Empty(result.Loaded);
        Assert.Equal(2, result.Errors.Count);
        Assert.All(result.Errors, e =>
        {
            Assert.Equal(PzErrorCode.ExtensionLoadFailed, e.Code);
            Assert.Equal("project.yml", e.File);
            Assert.Contains("~/.duckdb/extensions/v", e.Hint);
            Assert.Contains(platform, e.Hint);
        });
        Assert.Contains("'pz_no_such_ext_a'", result.Errors[0].Message);
        Assert.Contains("'pz_no_such_ext_b'", result.Errors[1].Message);
    }

    [SkippableFact]
    public async Task A_connector_can_reinstall_a_listed_extension()
    {
        await using var duck = DuckSession.Open(":memory:");

        var result = await DuckExtensions.LoadAsync(duck, ["httpfs"], CancellationToken.None);
        Assert.Empty(result.Errors);

        await duck.ExecuteAsync("install httpfs");
        await duck.ExecuteAsync("load httpfs");
    }
}
