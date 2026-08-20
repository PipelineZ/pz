using System.Text.RegularExpressions;

namespace Pz.Mcp.Docs;

/// <summary>A search hit: the page, its score, and the lines that matched.</summary>
public sealed record DocHit(DocPage Page, int Score, IReadOnlyList<string> Excerpts);

/// <summary>Keyword search over the fetched documentation.
///
/// Deliberately lexical, not semantic: the corpus is small enough that term matching finds the right
/// page, and an agent searching for "PZ0214" or "force_universal" wants the page containing that
/// exact token — which is precisely what an embedding model is worst at.
///
/// Fields are weighted because a term in a title means far more than the same term in a paragraph.
/// The weights match the ordering the Aspire CLI uses for the same problem.</summary>
public static class DocsSearch
{
    private const int TitleWeight = 10;
    private const int DescriptionWeight = 8;
    private const int HeadingWeight = 6;
    private const int CodeWeight = 5;
    private const int BodyWeight = 1;

    private static readonly Regex Term = new(@"[\w.:/-]{2,}", RegexOptions.Compiled);

    public static IReadOnlyList<DocHit> Rank(IEnumerable<DocPage> pages, string query, int limit)
    {
        var terms = Term.Matches(query.ToLowerInvariant())
            .Select(m => m.Value)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        if (terms.Count == 0)
        {
            return [];
        }

        var hits = new List<DocHit>();
        foreach (var page in pages)
        {
            var (score, excerpts) = Score(page, terms);
            if (score > 0)
            {
                hits.Add(new DocHit(page, score, excerpts));
            }
        }

        return hits
            .OrderByDescending(h => h.Score)
            // Slug break so equal scores return in a stable order rather than whatever order the
            // corpus happened to arrive in — an agent re-running the same query gets the same answer.
            .ThenBy(h => h.Page.Slug, StringComparer.Ordinal)
            .Take(limit)
            .ToList();
    }

    private static (int Score, IReadOnlyList<string> Excerpts) Score(DocPage page, IReadOnlyList<string> terms)
    {
        var score = Count(page.Title, terms) * TitleWeight
            + Count(page.Description, terms) * DescriptionWeight;

        var excerpts = new List<string>();
        var inFence = false;
        foreach (var raw in page.Body.Split('\n'))
        {
            var line = raw.TrimEnd('\r');
            if (line.TrimStart().StartsWith("```", StringComparison.Ordinal))
            {
                inFence = !inFence;
                continue;
            }

            var hits = Count(line, terms);
            if (hits == 0)
            {
                continue;
            }

            var weight = inFence ? CodeWeight
                : line.StartsWith('#') ? HeadingWeight
                : BodyWeight;
            score += hits * weight;

            // A few matching lines let an agent judge relevance without fetching the whole page.
            if (excerpts.Count < 3)
            {
                var trimmed = line.Trim();
                excerpts.Add(trimmed.Length > 200 ? trimmed[..197] + "..." : trimmed);
            }
        }

        return (score, excerpts);
    }

    private static int Count(string text, IReadOnlyList<string> terms)
    {
        if (text.Length == 0)
        {
            return 0;
        }

        var lower = text.ToLowerInvariant();
        var total = 0;
        foreach (var term in terms)
        {
            var from = 0;
            while (from < lower.Length)
            {
                var at = lower.IndexOf(term, from, StringComparison.Ordinal);
                if (at < 0)
                {
                    break;
                }

                total++;
                from = at + term.Length;
            }
        }

        return total;
    }
}
