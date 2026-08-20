using System.Text.RegularExpressions;
using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Dag;

/// <summary>
/// dbt-style node selector expressions. Grammar (whitespace = union, comma = intersection):
/// <c>expression = group , { WS , group }</c>; <c>group = atom , { "," , atom }</c>;
/// <c>atom = ["+"] , base , ["+"]</c>; <c>base = "tag:" name | "source:" pattern | pattern</c>.
/// A leading <c>+</c> pulls in ancestors of the atom's matches, a trailing <c>+</c> pulls in
/// descendants; both are inclusive of the atom's own matches. An atom whose base matches zero
/// nodes fails fast with <see cref="PzErrorCode.SelectorNoMatch"/> naming the atom.
/// </summary>
public static partial class Selector
{
    /// <summary>An entity is spelled the way its own system spells it, so a node name can carry the
    /// dots of a qualified table, the slashes of an endpoint, or
    /// the dashes of a bucket-style name. <c>-</c> is last in the class so it is a literal, not a range;
    /// <c>+</c> stays outside it because it is the ancestor/descendant operator.</summary>
    [GeneratedRegex(@"^(\+)?((tag|source):)?([A-Za-z0-9_.*/-]+)(\+)?$")]
    private static partial Regex AtomPattern();

    public static IReadOnlySet<NodeId> Apply(CompiledDag dag, string expression)
    {
        ArgumentNullException.ThrowIfNull(dag);
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);

        var byId = dag.Nodes.ToDictionary(n => n.Id);
        var result = new HashSet<NodeId>();

        foreach (var group in expression.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries))
        {
            HashSet<NodeId>? intersection = null;
            foreach (var atom in group.Split(',', StringSplitOptions.RemoveEmptyEntries))
            {
                var matches = MatchAtom(dag, byId, atom);
                if (intersection is null)
                {
                    intersection = matches;
                }
                else
                {
                    intersection.IntersectWith(matches);
                }
            }

            if (intersection is not null)
            {
                result.UnionWith(intersection);
            }
        }

        return result;
    }

    /// <summary>The both-direction closure (`+name+`) computed from already-resolved seed node ids —
    /// the structural backend for `pz run &lt;name&gt;`.
    /// Bypasses the atom grammar entirely: exact-name resolution happens at the caller, so
    /// positional names never gain wildcard or tag semantics on this path.</summary>
    public static IReadOnlySet<NodeId> FlowClosure(CompiledDag dag, IEnumerable<NodeId> seeds)
    {
        ArgumentNullException.ThrowIfNull(dag);
        ArgumentNullException.ThrowIfNull(seeds);

        var byId = dag.Nodes.ToDictionary(n => n.Id);
        var result = new HashSet<NodeId>();
        foreach (var seed in seeds)
        {
            result.Add(seed);
            CollectAncestors(byId, seed, result);
            foreach (var descendant in dag.Descendants(seed))
            {
                result.Add(descendant.Id);
            }
        }

        return result;
    }

    private static HashSet<NodeId> MatchAtom(CompiledDag dag, IReadOnlyDictionary<NodeId, DagNode> byId, string atom)
    {
        var match = AtomPattern().Match(atom);
        if (!match.Success)
        {
            throw NoMatch(atom);
        }

        var wantsAncestors = match.Groups[1].Success;
        var prefix = match.Groups[3].Success ? match.Groups[3].Value : null;
        var pattern = match.Groups[4].Value;
        var wantsDescendants = match.Groups[5].Success;

        var baseMatches = MatchBase(dag, prefix, pattern);
        if (baseMatches.Count == 0)
        {
            throw NoMatch(atom);
        }

        var result = new HashSet<NodeId>(baseMatches.Select(n => n.Id));

        if (wantsAncestors)
        {
            foreach (var node in baseMatches)
            {
                CollectAncestors(byId, node.Id, result);
            }
        }

        if (wantsDescendants)
        {
            foreach (var node in baseMatches)
            {
                foreach (var descendant in dag.Descendants(node.Id))
                {
                    result.Add(descendant.Id);
                }
            }
        }

        return result;
    }

    private static List<DagNode> MatchBase(CompiledDag dag, string? prefix, string pattern) => prefix switch
    {
        "tag" => [.. dag.Nodes.Where(n => n.Definition is PipelineDef p && p.Tags.Contains(pattern, StringComparer.Ordinal))],
        "source" => [.. dag.Nodes.Where(n => n.Definition is SourceDatasetDef s
            && WildcardMatch(pattern, $"{s.Source.Name}.{s.Dataset.Name}"))],
        _ => [.. dag.Nodes.Where(n => WildcardMatch(pattern, n.Name))],
    };

    private static bool WildcardMatch(string pattern, string candidate) =>
        Regex.IsMatch(candidate, "^" + Regex.Escape(pattern).Replace("\\*", ".*") + "$", RegexOptions.None);

    /// <summary>
    /// BFS over <see cref="DagNode.DependsOn"/> edges (parents), adding every transitive
    /// ancestor of <paramref name="start"/> to <paramref name="visited"/>. Guards re-entry via
    /// the shared <paramref name="visited"/> set so diamond-shaped ancestry (multiple paths to
    /// the same ancestor) is neither duplicated nor walked more than once.
    /// </summary>
    private static void CollectAncestors(IReadOnlyDictionary<NodeId, DagNode> byId, NodeId start, HashSet<NodeId> visited)
    {
        var queue = new Queue<NodeId>(byId[start].DependsOn);
        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            if (!visited.Add(current))
            {
                continue;
            }

            foreach (var parent in byId[current].DependsOn)
            {
                queue.Enqueue(parent);
            }
        }
    }

    private static PzValidationException NoMatch(string atom) => new([
        new PzError(PzErrorCode.SelectorNoMatch, $"Selector '{atom}' matched no nodes.", null, null, null),
    ]);
}
