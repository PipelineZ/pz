using Pz.Core.Validation;

namespace Pz.DuckDb;

/// <summary>One extension a session loaded, with the build DuckDB reports for it.</summary>
public sealed record LoadedExtension(string Name, string Version);

public sealed record ExtensionLoadResult(IReadOnlyList<LoadedExtension> Loaded, IReadOnlyList<PzError> Errors);

/// <summary>Installs and loads project.yml's <c>extensions:</c> the way DuckDB itself does:
/// <c>INSTALL name [FROM community]; LOAD name;</c>. DuckDB's own extension folder decides whether
/// that downloads: a file already there is used without any network call, which is how an offline
/// machine works. Every failure is reported, never just the first.</summary>
public static class DuckExtensions
{
    public static async Task<ExtensionLoadResult> LoadAsync(IDuckSession session, IReadOnlyList<string> names,
        CancellationToken ct)
    {
        var loaded = new List<LoadedExtension>();
        var errors = new List<PzError>();
        foreach (var name in names)
        {
            var ident = QuoteIdent(name);
            try
            {
                // An extension already loaded (one statically linked into DuckDB.NET, say) needs nothing:
                // INSTALL would still download a repository copy of it.
                var alreadyLoaded = await session.ScalarAsync<long>(
                    $"select count(*) from duckdb_extensions() where ({Matches(name)}) and loaded", ct)
                    .ConfigureAwait(false) > 0;
                if (!alreadyLoaded)
                {
                    var from = await IsCoreAsync(session, name, ct).ConfigureAwait(false) ? "" : " from community";
                    await session.ExecuteAsync($"install {ident}{from}", ct).ConfigureAwait(false);
                    await session.ExecuteAsync($"load {ident}", ct).ConfigureAwait(false);
                }

                var version = await session.ScalarAsync<string>(
                    $"select coalesce(max(extension_version), '') from duckdb_extensions() where {Matches(name)}", ct)
                    .ConfigureAwait(false);
                loaded.Add(new LoadedExtension(name, version));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                errors.Add(await FailureAsync(session, name, ex, ct).ConfigureAwait(false));
            }
        }

        return new ExtensionLoadResult(loaded, errors);
    }

    /// <summary>Core extensions are the ones DuckDB itself knows, installed or not, by name or alias
    /// (<c>sqlite</c> is <c>sqlite_scanner</c>); any other name can only come from the community
    /// repository, and core and community names never collide. An installed community extension is
    /// listed too, so it is told apart by where it was installed from.</summary>
    internal static async Task<bool> IsCoreAsync(IDuckSession session, string name, CancellationToken ct) =>
        await session.ScalarAsync<long>(
            $"select count(*) from duckdb_extensions() where ({Matches(name)}) " +
            "and coalesce(installed_from, '') <> 'community'", ct).ConfigureAwait(false) > 0;

    private static async Task<PzError> FailureAsync(IDuckSession session, string name, Exception ex,
        CancellationToken ct)
    {
        var version = await session.ScalarAsync<string>("select version()", ct).ConfigureAwait(false);
        var platform = await session.ScalarAsync<string>("select platform from pragma_platform()", ct)
            .ConfigureAwait(false);
        var file = await session.ScalarAsync<string>(
            $"select coalesce(max(extension_name), {QuoteLiteral(name)}) from duckdb_extensions() where {Matches(name)}",
            ct).ConfigureAwait(false);
        var firstLine = ex.Message.Split('\n', 2)[0].Trim();
        return new PzError(PzErrorCode.ExtensionLoadFailed,
            $"extension '{name}' could not be installed or loaded: {firstLine}", "project.yml", null,
            $"check the name under extensions:; without internet access, copy {file}.duckdb_extension into " +
            $"~/.duckdb/extensions/{version}/{platform}/");
    }

    private static string Matches(string name)
    {
        var literal = QuoteLiteral(name);
        return $"extension_name = {literal} or list_contains(aliases, {literal})";
    }

    private static string QuoteIdent(string name) => $"\"{name.Replace("\"", "\"\"")}\"";

    private static string QuoteLiteral(string value) => $"'{value.Replace("'", "''")}'";
}
