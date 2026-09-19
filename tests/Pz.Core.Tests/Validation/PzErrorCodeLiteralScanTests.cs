using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Pz.Core.Validation;

namespace Pz.Core.Tests.Validation;

/// <summary>Enforces <see cref="PzErrorCode"/> as the one source of truth for a PZ code's *value*.
/// Every `"PZ####"` string literal found elsewhere under <c>src/</c> or <c>connectors/</c> falls into
/// exactly one of two buckets:
///
/// 1. Its containing project can reference <c>Pz.Core</c> (directly or transitively) -- it should
///    read `PzErrorCode.SomeName`, not a bare string, so the value can never drift from the catalog.
///    This test fails naming the file/line.
/// 2. Its containing project architecturally cannot reference <c>Pz.Core</c> --
///    <c>Pz.PackageManagement</c> and the connector projects, which CLAUDE.md's layering table keeps
///    below <c>Pz.Core</c> in the dependency graph (<see cref="RestoreException"/>'s and
///    <see cref="Pz.PackageManagement.Hosting.ConnectorHostException"/>'s own doc comments explain
///    why). The bare literal is then unavoidable, but its VALUE must still match a real
///    <see cref="PzErrorCode"/> constant -- this is exactly the check the comment above
///    <see cref="PzErrorCode.RestoreDiskFailure"/> says nothing currently enforces (a catalog value
///    changed there without a matching grep of PackageManagement is how PZ0322 came to be reused for
///    two unrelated errors).
///
/// A PZ code mentioned only in prose (a doc comment cross-referencing a related error, or
/// <c>RestoreException</c>/<c>ConnectorHostException</c>'s own "one of these literals" comment listing
/// every code their <c>Code</c> property can hold) is not a functional duplication and is excluded --
/// only a literal actually used as a value (outside any `//`/`///` comment) is this test's
/// concern.</summary>
public class PzErrorCodeLiteralScanTests
{
    private static readonly Regex CodeLiteral = new("\"(PZ\\d{4})\"", RegexOptions.Compiled);
    private static readonly Regex ProjectReference = new(
        "<ProjectReference\\s+Include=\"([^\"]+)\"", RegexOptions.Compiled);

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        // thisFile: tests/Pz.Core.Tests/Validation/PzErrorCodeLiteralScanTests.cs -> repo root is three
        // directories up (mirrors EventsDocReflectionTests' identical-depth helper).
        var dir = Path.GetDirectoryName(thisFile)!;
        for (var i = 0; i < 3; i++)
        {
            dir = Path.GetDirectoryName(dir)!;
        }

        return dir;
    }

    /// <summary>Strips a `//`/`///` line comment that is not itself inside a quoted string, so a PZ
    /// code mentioned only in prose is never mistaken for a functional literal.</summary>
    private static string StripLineComment(string line)
    {
        var inString = false;
        for (var i = 0; i < line.Length; i++)
        {
            var c = line[i];
            if (c == '"' && (i == 0 || line[i - 1] != '\\'))
            {
                inString = !inString;
            }
            else if (!inString && c == '/' && i + 1 < line.Length && line[i + 1] == '/')
            {
                return line[..i];
            }
        }

        return line;
    }

    private static bool UnderExcludedDir(string path) =>
        path.Split(Path.DirectorySeparatorChar).Any(segment => segment is "bin" or "obj");

    private static IReadOnlyList<string> SourceFiles(string root)
    {
        var files = new List<string>();
        foreach (var sub in new[] { "src", "connectors" })
        {
            var dir = Path.Combine(root, sub);
            if (!Directory.Exists(dir))
            {
                continue;
            }

            files.AddRange(Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories)
                .Where(f => !UnderExcludedDir(f)));
        }

        return files;
    }

    /// <summary>Every <c>.csproj</c> under the repo, mapped to the absolute, normalized paths of every
    /// project it directly <c>&lt;ProjectReference&gt;</c>s -- enough to compute, per project, whether
    /// <c>Pz.Core</c> is reachable transitively.</summary>
    private static Dictionary<string, List<string>> ProjectReferenceGraph(string root)
    {
        var graph = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        foreach (var csproj in Directory.EnumerateFiles(root, "*.csproj", SearchOption.AllDirectories)
                     .Where(f => !UnderExcludedDir(f)))
        {
            var dir = Path.GetDirectoryName(csproj)!;
            var refs = ProjectReference.Matches(File.ReadAllText(csproj))
                .Select(m => Path.GetFullPath(Path.Combine(dir, m.Groups[1].Value.Replace('\\', Path.DirectorySeparatorChar))))
                .ToList();
            graph[Path.GetFullPath(csproj)] = refs;
        }

        return graph;
    }

    private static bool CanReach(string from, string to, IReadOnlyDictionary<string, List<string>> graph)
    {
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var stack = new Stack<string>();
        stack.Push(from);
        while (stack.Count > 0)
        {
            var current = stack.Pop();
            if (!visited.Add(current))
            {
                continue;
            }

            if (string.Equals(current, to, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (graph.TryGetValue(current, out var refs))
            {
                foreach (var r in refs)
                {
                    stack.Push(r);
                }
            }
        }

        return false;
    }

    /// <summary>The nearest ancestor <c>.csproj</c> whose own directory contains <paramref name="file"/>
    /// -- matches how the .NET SDK's implicit glob assigns a <c>.cs</c> file to a project. Null for a
    /// file with no enclosing project (e.g. a <c>&lt;Compile Include&gt;</c>-linked file living outside
    /// every <c>.csproj</c>'s own directory, like <c>src/Shared/*.cs</c>) -- such a file is checked only
    /// against the catalog, never required to hold a reference.</summary>
    private static string? ContainingProject(string file, IReadOnlyCollection<string> allProjects)
    {
        var dir = Path.GetDirectoryName(file);
        while (dir is not null)
        {
            var match = allProjects.FirstOrDefault(p =>
                string.Equals(Path.GetDirectoryName(p), dir, StringComparison.OrdinalIgnoreCase));
            if (match is not null)
            {
                return match;
            }

            dir = Path.GetDirectoryName(dir);
        }

        return null;
    }

    [Fact]
    public void Every_PZ_code_literal_outside_the_catalog_is_either_unavoidable_or_should_reference_it()
    {
        var root = RepoRoot();

        var catalogValues = typeof(PzErrorCode)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.FieldType == typeof(string) && f.IsLiteral)
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToHashSet(StringComparer.Ordinal);
        Assert.NotEmpty(catalogValues); // a broken reflection query must fail loudly, not vacuously pass

        var graph = ProjectReferenceGraph(root);
        var allProjects = graph.Keys.ToList();
        var pzCoreProject = allProjects.Single(p =>
            string.Equals(Path.GetFileName(p), "Pz.Core.csproj", StringComparison.OrdinalIgnoreCase));
        var catalogFile = Path.GetFullPath(Path.Combine(root, "src", "Pz.Core", "Validation", "PzErrorCode.cs"));

        var shouldReferenceInstead = new List<string>();
        var notInCatalog = new List<string>();

        foreach (var file in SourceFiles(root))
        {
            if (string.Equals(Path.GetFullPath(file), catalogFile, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var lines = File.ReadAllLines(file);
            for (var lineNo = 0; lineNo < lines.Length; lineNo++)
            {
                var stripped = StripLineComment(lines[lineNo]);
                foreach (Match m in CodeLiteral.Matches(stripped))
                {
                    var code = m.Groups[1].Value;
                    var location = $"{Path.GetRelativePath(root, file)}:{lineNo + 1}";
                    var project = ContainingProject(file, allProjects);
                    var canReferenceCore = project is not null && CanReach(project, pzCoreProject, graph);

                    if (canReferenceCore)
                    {
                        shouldReferenceInstead.Add(
                            $"{location} (\"{code}\") -- this project can reference Pz.Core; use PzErrorCode instead of a literal");
                    }
                    else if (!catalogValues.Contains(code))
                    {
                        notInCatalog.Add($"{location} (\"{code}\") -- not a value PzErrorCode.cs declares");
                    }
                }
            }
        }

        var problems = shouldReferenceInstead.Concat(notInCatalog).ToList();
        Assert.True(problems.Count == 0, "PZ code literal(s) need attention:\n" + string.Join('\n', problems));
    }
}
