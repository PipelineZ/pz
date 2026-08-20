using System.Text.Json;

namespace Pz.Engine.Artifacts;

/// <summary>Writes .pz/target/schemas.json -- byte-stable (mirrors <see cref="PlanWriter"/>'s discipline
/// exactly): "source.dataset" keys sorted ordinally, fixed field order, LF, final newline. The one
/// artifact `pz validate --connect` may write; covers only datasets without a declared
/// `columns:` contract -- see <see cref="Pz.Engine.Validation.ConnectivityValidator"/>.</summary>
public static class SchemaCacheWriter
{
    public static void Write(IReadOnlyDictionary<string, string> fetchedSchemas, string targetDir)
    {
        Directory.CreateDirectory(targetDir);
        var path = Path.Combine(targetDir, "schemas.json");
        using var stream = File.Create(path);
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true, IndentSize = 2, NewLine = "\n" }))
        {
            writer.WriteStartObject();
            writer.WriteNumber("version", 1);
            writer.WriteStartObject("schemas");
            foreach (var (key, value) in fetchedSchemas.OrderBy(kv => kv.Key, StringComparer.Ordinal))
            {
                writer.WriteString(key, value);
            }

            writer.WriteEndObject();
            writer.WriteEndObject();
        }

        stream.WriteByte((byte)'\n');
    }
}
