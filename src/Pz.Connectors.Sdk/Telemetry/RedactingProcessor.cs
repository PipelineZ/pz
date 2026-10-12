using System.Diagnostics;
using OpenTelemetry;

namespace Pz.Connectors.Sdk.Telemetry;

/// <summary>Keeps data values out of the spans that client libraries emit, before export. Spans from the
/// SDK's own <c>Pz.Connector</c> sources are left alone: the SDK writes none of the keys below.
///
/// <para>For a foreign span: the listed attribute keys are removed and the status description is cleared
/// (libraries put the exception message there). An <see cref="Activity"/> event can be neither edited nor
/// removed, and <see cref="Activity.AddException"/> appends <c>exception.message</c> and
/// <c>exception.stacktrace</c> after any <see cref="ActivityListener.ExceptionRecorder"/> has run, so
/// exception text cannot be scrubbed. A span with any event tag other than <c>exception.type</c> is therefore
/// dropped whole, by clearing <see cref="ActivityTraceFlags.Recorded"/>, which the export processors honour.
/// A failed library call loses its span; an exported one could leak a data value. This processor must be
/// registered ahead of the exporter.</para></summary>
internal sealed class RedactingProcessor : BaseProcessor<Activity>
{
    private static readonly HashSet<string> ExactKeys = new(StringComparer.Ordinal)
    {
        "db.statement",
        "db.query.text",
        "url.full",
        "url.query",
        "url.path",
        "http.url",
        "http.target",
        "exception.message",
    };

    public override void OnEnd(Activity data)
    {
        var source = data.Source.Name;
        if (source == ConnectorTelemetry.SourceName
            || source.StartsWith(ConnectorTelemetry.SourceName + ".", StringComparison.Ordinal))
        {
            return;
        }

        if (data.StatusDescription is not null)
        {
            data.SetStatus(data.Status, null);
        }

        if (CarriesUnsafeEvent(data))
        {
            data.ActivityTraceFlags &= ~ActivityTraceFlags.Recorded;
            return;
        }

        List<string>? doomed = null;
        foreach (var tag in data.TagObjects)
        {
            if (IsRedacted(tag.Key))
            {
                (doomed ??= []).Add(tag.Key);
            }
        }

        if (doomed is null)
        {
            return;
        }

        foreach (var key in doomed)
        {
            data.SetTag(key, null);
        }
    }

    private static bool CarriesUnsafeEvent(Activity activity)
    {
        foreach (var activityEvent in activity.Events)
        {
            foreach (var tag in activityEvent.Tags)
            {
                if (tag.Key != "exception.type")
                {
                    return true;
                }
            }
        }

        return false;
    }

    internal static bool IsRedacted(string key) =>
        ExactKeys.Contains(key)
        || key.StartsWith("db.query.parameter.", StringComparison.Ordinal)
        || key.StartsWith("messaging.message.body", StringComparison.Ordinal)
        || (key.StartsWith("messaging.", StringComparison.Ordinal) && key.Contains(".payload", StringComparison.Ordinal));
}
