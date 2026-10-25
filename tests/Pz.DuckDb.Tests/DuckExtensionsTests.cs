using Pz.DuckDb;

namespace Pz.DuckDb.Tests;

/// <summary>Extension loading that needs no download: DuckDB's own catalog and the extensions
/// statically linked into DuckDB.NET.</summary>
public sealed class DuckExtensionsTests
{
    [Fact]
    public async Task No_names_loads_nothing()
    {
        await using var duck = DuckSession.Open(":memory:");
        var result = await DuckExtensions.LoadAsync(duck, [], CancellationToken.None);
        Assert.Empty(result.Loaded);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public async Task A_statically_linked_extension_loads()
    {
        await using var duck = DuckSession.Open(":memory:");
        Assert.Equal("STATICALLY_LINKED", await duck.ScalarAsync<string>(
            "select install_mode from duckdb_extensions() where extension_name = 'parquet'"));

        var result = await DuckExtensions.LoadAsync(duck, ["parquet"], CancellationToken.None);

        Assert.Empty(result.Errors);
        var loaded = Assert.Single(result.Loaded);
        Assert.Equal("parquet", loaded.Name);
        Assert.False(string.IsNullOrEmpty(loaded.Version));
    }

    [Fact]
    public async Task An_already_loaded_extension_is_not_downloaded()
    {
        await using (var duck = DuckSession.Open(":memory:"))
        {
            await DuckExtensions.LoadAsync(duck, ["parquet"], CancellationToken.None);
        }

        // A fresh session would see a repository copy if the first one had run INSTALL.
        await using var fresh = DuckSession.Open(":memory:");
        Assert.Equal("STATICALLY_LINKED", await fresh.ScalarAsync<string>(
            "select install_mode from duckdb_extensions() where extension_name = 'parquet'"));
    }

    [Fact]
    public async Task An_alias_of_a_core_extension_is_core()
    {
        await using var duck = DuckSession.Open(":memory:");
        Assert.True(await DuckExtensions.IsCoreAsync(duck, "sqlite", CancellationToken.None));
        Assert.True(await DuckExtensions.IsCoreAsync(duck, "sqlite_scanner", CancellationToken.None));
    }

    [Fact]
    public async Task A_community_name_is_not_core()
    {
        await using var duck = DuckSession.Open(":memory:");
        Assert.False(await DuckExtensions.IsCoreAsync(duck, "jev", CancellationToken.None));
    }
}
