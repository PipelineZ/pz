using Pz.DuckDb;

namespace Pz.Engine.Execution;

/// <summary>Shared plumbing for the pz_meta accounting schema inside the run's staging.duckdb: the
/// single copy of the escaping, schema creation, catalog qualification, and read-only ATTACH/DETACH
/// dance that <see cref="PartitionLedger"/> and <see cref="SinkDeliveryLedger"/> both need.
/// Engine-internal; never surfaced to connectors.</summary>
internal static class PzMeta
{
    public const string Schema = "pz_meta";

    public static string Escape(string s) => s.Replace("'", "''");

    public static Task EnsureSchemaAsync(IDuckSession duck, CancellationToken ct) =>
        duck.ExecuteAsync($"create schema if not exists {Schema}", ct);

    /// <summary><paramref name="qualifiedTable"/> is an already-schema-qualified name
    /// ("pz_meta.x"); a non-null <paramref name="catalog"/> (a quoted ATTACH alias) targets that
    /// table inside the attached prior-run staging DB instead of the local run's.</summary>
    public static string Qualify(string qualifiedTable, string? catalog) =>
        catalog is null ? qualifiedTable : $"{catalog}.{qualifiedTable}";

    /// <summary>ATTACHes <paramref name="path"/> read-only as <paramref name="alias"/> (quoted via
    /// <see cref="ArrowInterop.QuoteQualified"/>) and returns a scope whose DisposeAsync DETACHes
    /// best-effort on <see cref="CancellationToken.None"/> (teardown must run even when the
    /// triggering failure was a cancellation) with any detach exception suppressed -- the shared
    /// discipline every cross-run attach site follows. The returned scope also
    /// exposes the quoted alias for query composition. Attach failures propagate to the caller
    /// (whose own guard semantics decide what a failed attach means).</summary>
    public static async Task<AttachScope> AttachReadOnlyAsync(
        IDuckSession duck, string alias, string path, CancellationToken ct)
    {
        var quotedAlias = ArrowInterop.QuoteQualified(alias);
        await duck.ExecuteAsync($"attach '{Escape(path)}' as {quotedAlias} (read_only)", ct).ConfigureAwait(false);
        return new AttachScope(duck, quotedAlias);
    }

    internal sealed class AttachScope(IDuckSession duck, string quotedAlias) : IAsyncDisposable
    {
        public string QuotedAlias => quotedAlias;

        public async ValueTask DisposeAsync()
        {
            try
            {
                await duck.ExecuteAsync($"detach {quotedAlias}", CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // Best-effort DETACH -- never mask the caller's flow.
            }
        }
    }
}
