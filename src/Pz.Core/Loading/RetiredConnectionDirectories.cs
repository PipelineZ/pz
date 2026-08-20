using System.Globalization;
using Pz.Core.Validation;

namespace Pz.Core.Loading;

/// <summary><c>sources/</c> and <c>sinks/</c> are retired in favour of one <c>connections.yml</c>.
/// This is the first error such a project hits, so each leftover file gets its own PZ0346 whose hint
/// reconstructs that file as the block it becomes —
/// the same reasoning as PZ0347's pasteable <c>sink()</c> call. The reconstruction is deliberately
/// lossy about comments; it is a migration aid, not a formatter.</summary>
internal static class RetiredConnectionDirectories
{
    public static void Refuse(string projectDir, List<PzError> errors)
    {
        foreach (var (directory, nameKey, entityKey, direction) in new[]
        {
            ("sources", "source", "datasets", "read"),
            ("sinks", "sink", "outputs", "write"),
        })
        {
            var path = Path.Combine(projectDir, directory);
            if (!Directory.Exists(path))
            {
                continue;
            }

            foreach (var file in Directory.EnumerateFiles(path, "*.yml").OrderBy(f => f, StringComparer.Ordinal))
            {
                var relativePath = ProjectLoader.RelativePath(projectDir, file);
                errors.Add(new PzError(PzErrorCode.RetiredConnectionDirectory,
                    $"{relativePath}: `{directory}/` is retired -- every connection lives in " +
                    $"{ConnectionsLoader.FileName}.",
                    relativePath, null,
                    $"delete {directory}/ and put this in {ConnectionsLoader.FileName}:\n" +
                    Reconstruct(file, relativePath, nameKey, entityKey, direction)));
            }
        }
    }

    private static string Reconstruct(string file, string relativePath, string nameKey, string entityKey,
        string direction)
    {
        Dictionary<string, object?> yaml;
        try
        {
            yaml = YamlMapper.LoadFile(file, relativePath);
        }
        catch (PzConfigException)
        {
            // Unparseable: the PZ0346 message alone still names the file and what to do with it.
            return $"  <name>:\n    connector: <connector>";
        }

        var name = ProjectLoader.TryGetString(yaml, nameKey) ?? "<name>";
        var lines = new List<string> { $"  {name}:" };

        // connector, then the flattened `connection:` map, then the instance keys -- everything that is
        // not the entity block becomes a connection-level key.
        foreach (var (key, value) in yaml)
        {
            if (key == nameKey || key == entityKey || key == "connection")
            {
                continue;
            }

            lines.AddRange(Render(key, value, 2));
        }

        foreach (var (key, value) in ProjectLoader.GetDict(yaml, "connection"))
        {
            lines.AddRange(Render(key, value, 2));
        }

        var entities = ProjectLoader.GetDict(yaml, entityKey);
        if (entities.Count > 0)
        {
            lines.Add("    entities:");
            foreach (var (entity, value) in entities)
            {
                lines.Add($"      {entity}:");
                lines.Add($"        {direction}:");
                if (value is Dictionary<string, object?> body)
                {
                    foreach (var (key, inner) in body)
                    {
                        lines.AddRange(Render(key, inner, 5));
                    }
                }
            }
        }

        return string.Join('\n', lines);
    }

    /// <summary>One YAML key at <paramref name="indent"/> levels of two spaces, recursing into maps and
    /// rendering lists inline. Enough for a migration hint; not a general emitter.</summary>
    private static IEnumerable<string> Render(string key, object? value, int indent)
    {
        var pad = new string(' ', indent * 2);
        switch (value)
        {
            case Dictionary<string, object?> map:
                yield return $"{pad}{key}:";
                foreach (var (innerKey, inner) in map)
                {
                    foreach (var line in Render(innerKey, inner, indent + 1))
                    {
                        yield return line;
                    }
                }

                break;
            case List<object?> list:
                yield return $"{pad}{key}: [{string.Join(", ", list.Select(Scalar))}]";
                break;
            default:
                yield return $"{pad}{key}: {Scalar(value)}";
                break;
        }
    }

    private static string Scalar(object? value) => value switch
    {
        null => "",
        bool b => b ? "true" : "false",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };
}
