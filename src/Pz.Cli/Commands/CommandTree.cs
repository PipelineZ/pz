using System.CommandLine;

namespace Pz.Cli.Commands;

/// <summary>Walks <see cref="CliApp.Build"/>'s real <see cref="Command"/> tree instead of hand-maintaining
/// a second list of verb/sub-verb names — the source `pz completion` generates its scripts from, so a
/// verb added to <see cref="CliApp.Build"/> without a matching completion entry fails the
/// "every registered verb appears in each script" test rather than silently drifting out of sync with
/// `--help`. pz's tree is exactly two levels deep today (a verb, then an optional sub-verb — `connector
/// test`, `cdc status`/`drop`, `schema accept`, `state show`/`rollback`/`set`/`clear`, `mcp init`), so
/// this stays a flat verb→children map rather than a general recursive structure.</summary>
internal static class CommandTree
{
    /// <summary>One top-level verb and its own direct subcommands, in the order
    /// <see cref="CliApp.Build"/> registered them — what keeps a generated script's listed order stable
    /// across builds.</summary>
    internal sealed record VerbGroup(string Name, IReadOnlyList<string> Children);

    internal static IReadOnlyList<VerbGroup> Collect(Command root) =>
        [.. root.Subcommands.Select(c => new VerbGroup(c.Name, [.. c.Subcommands.Select(s => s.Name)]))];

    /// <summary>Every command name in the tree, at any depth — used by the "every registered verb
    /// appears in each generated script" test, which must catch a nested sub-verb (e.g. a new `state`
    /// subcommand) exactly as it would a missing top-level one.</summary>
    internal static IReadOnlyList<string> AllNames(Command root) =>
        [.. root.Subcommands.SelectMany(c => new[] { c.Name }.Concat(c.Subcommands.Select(s => s.Name)))];
}
