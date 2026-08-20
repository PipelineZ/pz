using Pz.Core.Dag;
using Pz.Core.Validation;

namespace Pz.Cli.Commands;

/// <summary>Shared node-selection resolution for verbs that operate on a full DAG. A null result
/// means "everything". In the 5-arg overload, positional flow names (exact-match, `+name+` closure),
/// <c>--select</c>, and <c>--all</c> are
/// mutually exclusive (PZ0216); with none given, `run` (<c>gateBareMultiFlow: true</c>) refuses a
/// 2+-component project (PZ0215) while `plan` stays ungated. Throws
/// <see cref="PzValidationException"/> — callers wrap this in their own error-printing/exit-code
/// convention.</summary>
internal static class RunSelection
{
    /// <summary>`pz test`'s --select-only path (no positional-name surface): a null/blank
    /// <c>--select</c> means "everything"; otherwise <see cref="Selector.Apply"/> expands the
    /// expression.</summary>
    public static IReadOnlySet<NodeId>? Resolve(CompiledDag fullDag, string? select) =>
        string.IsNullOrWhiteSpace(select) ? null : Selector.Apply(fullDag, select);

    public static IReadOnlySet<NodeId>? Resolve(
        CompiledDag fullDag, IReadOnlyList<string> names, string? select, bool all,
        bool gateBareMultiFlow)
    {
        var hasNames = names.Count > 0;
        var hasSelect = !string.IsNullOrWhiteSpace(select);
        if ((hasNames ? 1 : 0) + (hasSelect ? 1 : 0) + (all ? 1 : 0) > 1)
        {
            throw new PzValidationException([new PzError(PzErrorCode.SelectionConflict,
                "flow names, --select, and --all are mutually exclusive ways to choose what runs.",
                null, null,
                "pass exactly one: `pz run <name>`, `pz run --select '<expr>'`, or `pz run --all`")]);
        }

        if (all)
        {
            return null;
        }

        if (hasSelect)
        {
            return Selector.Apply(fullDag, select!);
        }

        if (hasNames)
        {
            return ResolveNames(fullDag, names);
        }

        if (gateBareMultiFlow)
        {
            var flows = FlowComponents.Compute(fullDag);
            if (flows.Count >= 2)
            {
                throw new PzValidationException([new PzError(PzErrorCode.MultiFlowNeedsSelection,
                    $"project has {flows.Count} independent flows ({DescribeFlows(flows)}); " +
                    "bare `pz run` would run all of them.",
                    null, null,
                    "run one flow with `pz run <name>`, or everything with `pz run --all`")]);
            }
        }

        return null;
    }

    /// <summary>Exact node-name matching (wildcards/tags stay --select-only), then the
    /// both-direction `+name+` closure per name, unioned. Unknown names aggregate into one PZ0210
    /// report (validation reports ALL errors), each naming the project's flows so the user can
    /// self-correct.</summary>
    private static IReadOnlySet<NodeId> ResolveNames(CompiledDag fullDag, IReadOnlyList<string> names)
    {
        var byName = fullDag.Nodes.ToLookup(n => n.Name, StringComparer.Ordinal);
        var unknown = names.Where(name => !byName[name].Any())
            .Distinct(StringComparer.Ordinal).ToList();
        if (unknown.Count > 0)
        {
            var flows = DescribeFlows(FlowComponents.Compute(fullDag));
            // A typo'd option (`--ful-refresh`) parses as a positional name, so a flag-shaped "name" is
            // reported as an unrecognized option rather than as a node-lookup failure.
            throw new PzValidationException([.. unknown.Select(name => name.StartsWith('-')
                ? new PzError(
                    PzErrorCode.SelectorNoMatch,
                    $"'{name}' is not a recognized option.",
                    null, null, "see `pz run --help` for the accepted options")
                : new PzError(
                    PzErrorCode.SelectorNoMatch,
                    $"no node named '{name}'; this project's flows: {flows}.",
                    null, null, "pick a node name from `pz ls`, or run everything with `pz run --all`"))]);
        }

        return Selector.FlowClosure(fullDag, names.SelectMany(name => byName[name]).Select(n => n.Id));
    }

    private static string DescribeFlows(IReadOnlyList<FlowComponent> flows) =>
        string.Join("; ", flows.Select(f => f.Label));
}
