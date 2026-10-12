using System.Diagnostics;
using OpenTelemetry;

namespace Pz.Connectors.Sdk.Telemetry;

/// <summary>Strips attributes that can carry user data (statements, query parameters, URLs, message
/// bodies, exception messages) from spans that client libraries emit, before export. Spans from the
/// SDK's own <c>Pz.Connector</c> sources are left alone: the SDK writes none of those keys.
///
/// <para>Only tags are rewritten. <see cref="Activity"/> offers no way to remove or edit an event, so
/// the <c>exception</c> event a library records on failure keeps its message.</para></summary>
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

    internal static bool IsRedacted(string key) =>
        ExactKeys.Contains(key)
        || key.StartsWith("db.query.parameter.", StringComparison.Ordinal)
        || key.StartsWith("messaging.message.body", StringComparison.Ordinal)
        || (key.StartsWith("messaging.", StringComparison.Ordinal) && key.Contains(".payload", StringComparison.Ordinal));
}
