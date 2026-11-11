using System.Text.Json;

namespace Pz.Engine.Artifacts;

/// <summary>The JSON writer settings every byte-stable <c>.pz</c> artifact shares: two-space
/// indentation and LF line endings on every platform.</summary>
internal static class StableJsonWriter
{
    public static readonly JsonWriterOptions Options = new() { Indented = true, IndentSize = 2, NewLine = "\n" };
}
