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
        var header = result.Columns.Select(c => Printable(c.Redacted ? $"{c.Name} (redacted)" : c.Name)).ToList();
        var cells = result.Rows
            .Select(r => r.Select(c => c.Kind == PreviewCellKind.Null ? "NULL" : Printable(c.Text!)).ToList())
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

    /// <summary>Values are source data on their way to a terminal: control characters (C0, DEL, C1) are shown
    /// as <c>\xNN</c> and bidi controls as <c>\uNNNN</c>, so a cell cannot move the cursor, retitle the
    /// window or reorder what is displayed.</summary>
    internal static string Printable(string value)
    {
        if (!value.Any(NeedsEscape))
        {
            return value;
        }

        var sb = new StringBuilder(value.Length + 8);
        foreach (var ch in value)
        {
            if (!NeedsEscape(ch))
            {
                sb.Append(ch);
            }
            else if (ch <= '\u00ff')
            {
                sb.Append("\\x").Append(((int)ch).ToString("x2", System.Globalization.CultureInfo.InvariantCulture));
            }
            else
            {
                sb.Append("\\u").Append(((int)ch).ToString("x4", System.Globalization.CultureInfo.InvariantCulture));
            }
        }

        return sb.ToString();
    }

    private static bool NeedsEscape(char ch) =>
        ch < ' ' || ch is >= '\u007f' and <= '\u009f'
        || ch is '\u061c' or '\u200e' or '\u200f' || ch is >= '\u202a' and <= '\u202e'
        || ch is >= '\u2066' and <= '\u2069';

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
