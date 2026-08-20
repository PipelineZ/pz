using System.Buffers;
using System.Text;
using System.Text.Json;
using Pz.Core.Validation;

namespace Pz.Mcp;

/// <summary>The one result shape every pz MCP tool returns. Byte-stable per the repo's
/// Utf8JsonWriter discipline: fixed field order (ok, applied?, errors?, result?), no indentation,
/// null file/line omitted. This shape is an append-only stability contract
/// (https://pipelinez.dev/reference/mcp-contract/) — agents pattern-match on it.</summary>
public static class ToolEnvelope
{
    public static string Ok(Action<Utf8JsonWriter>? writeResult = null, bool? applied = null) =>
        Write(true, applied, null, writeResult);

    /// <param name="writeResult">Writes a <c>result</c> object alongside the errors. An error envelope
    /// may still carry one: a mutation that applied and then failed self-verify has real facts to
    /// report about what it did, and those facts are often what explains the errors.</param>
    public static string Errors(
        IReadOnlyList<PzError> errors, bool? applied = null, Action<Utf8JsonWriter>? writeResult = null) =>
        Write(false, applied, errors, writeResult);

    private static string Write(
        bool ok, bool? applied, IReadOnlyList<PzError>? errors, Action<Utf8JsonWriter>? writeResult)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var json = new Utf8JsonWriter(buffer, new JsonWriterOptions { Indented = false }))
        {
            json.WriteStartObject();
            json.WriteBoolean("ok", ok);
            if (applied is { } a) { json.WriteBoolean("applied", a); }
            if (errors is { Count: > 0 })
            {
                json.WriteStartArray("errors");
                foreach (var e in errors)
                {
                    json.WriteStartObject();
                    json.WriteString("code", e.Code);
                    json.WriteString("message", e.Message);
                    if (e.File is { } file) { json.WriteString("file", file); }
                    if (e.Line is { } line) { json.WriteNumber("line", line); }
                    json.WriteString("next_step", e.Hint);
                    json.WriteEndObject();
                }
                json.WriteEndArray();
            }
            writeResult?.Invoke(json);
            json.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }
}
