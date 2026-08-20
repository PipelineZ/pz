using System.Buffers;
using System.Text;
using System.Text.Json;
using Pz.Diagnostics.Events;

namespace Pz.Cli.Rendering;

/// <summary>`--log-format json`: one canonical JSON object per <see cref="RunEvent"/>, LF-terminated,
/// no indentation (repo's byte-stable writer discipline — matches <c>RunResultsWriter</c>'s
/// <see cref="Utf8JsonWriter"/> style). <c>event</c> is the record name in snake_case minus the
/// trailing "Event"; every other field name is the record property in camelCase. Writes to an injected
/// <see cref="TextWriter"/> (default <see cref="Console.Out"/>, resolved per call so tests that swap
/// <see cref="Console.SetOut"/> around the render still capture output) so tests can capture output
/// without touching the real stdout. The field contract this produces is documented, and kept honest,
/// by https://pipelinez.dev/events/.</summary>
public sealed class JsonRenderer(TextWriter? writer = null) : IEventRenderer
{
    private const string AtFormat = "yyyy-MM-ddTHH:mm:ss.fffZ";

    private TextWriter Writer => writer ?? Console.Out;

    public void Render(RunEvent evt)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var json = new Utf8JsonWriter(buffer, new JsonWriterOptions { Indented = false }))
        {
            WriteEvent(json, evt);
        }

        Writer.Write(Encoding.UTF8.GetString(buffer.WrittenSpan));
        Writer.Write('\n');
    }

    // The event-name mapping and per-event field writer live in Pz.Diagnostics.Events.RunEventFields so
    // that Pz.State.SqlServer.SqlEventSink's persisted-row shape and this renderer's stdout shape share
    // one source of truth instead of two mappings that could drift.
    private static void WriteEvent(Utf8JsonWriter json, RunEvent evt)
    {
        json.WriteStartObject();
        json.WriteString("event", RunEventFields.EventName(evt));
        json.WriteString("at", evt.At.ToString(AtFormat));
        json.WriteString("runId", evt.RunId);

        RunEventFields.WriteFields(json, evt);

        json.WriteEndObject();
    }
}
