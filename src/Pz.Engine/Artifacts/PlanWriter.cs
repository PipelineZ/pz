using System.Text.Json;
using Pz.Engine.Planning;

namespace Pz.Engine.Artifacts;

/// <summary>Writes .pz/target/plan.json — byte-stable: topological node order is the
/// caller's responsibility (ExecutionPlan preserves it), fixed field order, LF, final newline.</summary>
public static class PlanWriter
{
    public static void Write(ExecutionPlan plan, string targetDir)
    {
        Directory.CreateDirectory(targetDir);
        var path = Path.Combine(targetDir, "plan.json");
        using var stream = File.Create(path);
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true, IndentSize = 2, NewLine = "\n" }))
        {
            writer.WriteStartObject();
            writer.WriteNumber("version", 1);
            writer.WriteStartArray("nodes");
            foreach (var node in plan.Nodes)
            {
                writer.WriteStartObject();
                writer.WriteString("id", node.Id.Value);
                writer.WriteString("kind", node.Kind.ToString());
                writer.WriteString("name", node.Name);
                writer.WriteString("strategy", StrategyName(node.Strategy));
                writer.WriteNumber("partitions", node.Partitions);
                writer.WriteString("reason", node.Reason);

                // Additive, and written only when this node actually pushes something, so a plan.json
                // that pushed nothing carries no pushdown keys at all.
                // Counts only — predicate TEXT never lands here, same hygiene rule that keeps SQL out of
                // Reason strings. Null columns means the whole row is read, which is not zero columns.
                if (node.Pushdown is { } pushdown)
                {
                    WriteNullableNumber(writer, "columns_pushed", pushdown.ColumnsPushed);
                    writer.WriteBoolean("predicate_pushed", pushdown.PredicatePushed);
                }

                writer.WriteEndObject();
            }

            writer.WriteEndArray();

            // Additive, appended last so plan.json stays version-stable -- every byte up to here is
            // independent of this object.
            writer.WriteStartObject("memoryBudget");
            WriteNullableNumber(writer, "duckdbBytes", plan.MemoryBudget.DuckDbBytes);
            WriteNullableString(writer, "duckdbDisclaimer", plan.MemoryBudget.DuckDbDisclaimer);
            writer.WriteNumber("channelBytes", plan.MemoryBudget.ChannelBytes);
            writer.WriteNumber("fixedOverheadBytes", plan.MemoryBudget.FixedOverheadBytes);
            writer.WriteNumber("totalBytes", plan.MemoryBudget.TotalBytes);
            // Appended AFTER totalBytes, so every byte before it is unchanged for a pre-existing
            // consumer. Written as an explicit null when
            // engine.duckdb.threads IS set, matching duckdbDisclaimer's shape in this same object
            // rather than introducing a second convention inside one block.
            WriteNullableString(writer, "duckdbThreadsDisclaimer", plan.MemoryBudget.DuckDbThreadsDisclaimer);
            writer.WriteEndObject();

            writer.WriteEndObject();
        }

        stream.WriteByte((byte)'\n');
    }

    private static void WriteNullableNumber(Utf8JsonWriter writer, string name, long? value)
    {
        if (value is { } number)
        {
            writer.WriteNumber(name, number);
        }
        else
        {
            writer.WriteNull(name);
        }
    }

    private static void WriteNullableString(Utf8JsonWriter writer, string name, string? value)
    {
        if (value is not null)
        {
            writer.WriteString(name, value);
        }
        else
        {
            writer.WriteNull(name);
        }
    }

    internal static string StrategyName(EdgeStrategy strategy) => strategy switch
    {
        EdgeStrategy.NativeScan => "native_scan",
        EdgeStrategy.NativeCopy => "native_copy",
        EdgeStrategy.ArrowStream => "arrow_stream",
        EdgeStrategy.DuckSql => "duck_sql",
        _ => throw new ArgumentOutOfRangeException(nameof(strategy), strategy, "unknown strategy"),
    };
}
