using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Pz.Core.Validation;
using Pz.Engine.Preview;

namespace Pz.Cli.Commands;

/// <summary>How <c>pz preview</c> prints: one JSON object (the contract with PipelineX) or a text table.
/// The JSON is written by hand with relaxed escaping, so values reach PipelineX's secret masker as written
/// rather than <c>\uXXXX</c>-escaped, and nothing needs reflection (Native AOT).</summary>
internal static class PreviewOutput
{
    private static readonly JsonWriterOptions WriterOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    internal static string ToJson(PreviewResult result) => Write(json =>
    {
        json.WriteStartObject();
        json.WriteString("node", result.Node);
        json.WriteString("mode", result.Mode);
        if (result.RunId is null)
        {
            json.WriteNull("runId");
        }
        else
        {
            json.WriteString("runId", result.RunId);
        }

        json.WriteStartArray("columns");
        foreach (var column in result.Columns)
        {
            json.WriteStartObject();
            json.WriteString("name", column.Name);
            json.WriteString("type", column.Type);
            json.WriteBoolean("redacted", column.Redacted);
            json.WriteEndObject();
        }

        json.WriteEndArray();
        json.WriteStartArray("rows");
        foreach (var row in result.Rows)
        {
            json.WriteStartArray();
            foreach (var cell in row)
            {
                switch (cell.Kind)
                {
                    case PreviewCellKind.Null:
                        json.WriteNullValue();
                        break;
                    case PreviewCellKind.Number:
                        json.WriteRawValue(cell.Text!, skipInputValidation: false);
                        break;
                    case PreviewCellKind.Boolean:
                        json.WriteBooleanValue(cell.Text == "true");
                        break;
                    default:
                        json.WriteStringValue(cell.Text);
                        break;
                }
            }

            json.WriteEndArray();
        }

        json.WriteEndArray();
        if (result.TotalRows is { } total)
        {
            json.WriteNumber("totalRows", total);
        }
        else
        {
            json.WriteNull("totalRows");
        }

        json.WriteNumber("limit", result.Limit);
        json.WriteBoolean("sliced", result.Sliced);
        json.WriteEndObject();
    });

    internal static string ErrorJson(PzError error) => Write(json =>
    {
        json.WriteStartObject();
        json.WriteStartObject("error");
        json.WriteString("code", error.Code);
        json.WriteString("message", error.Hint is null ? error.Message : $"{error.Message} — hint: {error.Hint}");
        json.WriteEndObject();
        json.WriteEndObject();
    });

    internal static string ToTable(PreviewResult result)
    {
        var header = result.Columns.Select(c => c.Redacted ? $"{c.Name} (redacted)" : c.Name).ToList();
        var cells = result.Rows.Select(r => r.Select(c => c.Kind == PreviewCellKind.Null ? "NULL" : c.Text!).ToList())
            .ToList();
        var widths = header.Select((h, i) => cells.Select(r => r[i].Length).Prepend(h.Length).Max()).ToList();

        var table = new StringBuilder();
        AppendRow(table, header, widths);
        table.AppendJoin("  ", widths.Select(w => new string('-', w))).Append('\n');
        foreach (var row in cells)
        {
            AppendRow(table, row, widths);
        }

        table.Append(result.Mode == "run"
            ? $"{result.Rows.Count} of {result.TotalRows} rows\n"
            : $"{result.Rows.Count} rows, fresh: computed from a slice of each source\n");
        return table.ToString();
    }

    private static void AppendRow(StringBuilder table, IReadOnlyList<string> values, IReadOnlyList<int> widths) =>
        table.AppendJoin("  ", values.Select((v, i) => v.PadRight(widths[i]))).Append('\n');

    private static string Write(Action<Utf8JsonWriter> write)
    {
        using var buffer = new MemoryStream();
        using (var json = new Utf8JsonWriter(buffer, WriterOptions))
        {
            write(json);
        }

        return Encoding.UTF8.GetString(buffer.ToArray());
    }
}
