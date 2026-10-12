using System.Diagnostics;
using OpenTelemetry;

namespace Pz.Connectors.Sdk.Telemetry;

/// <summary>Keeps data values out of the spans that client libraries emit, before export. Only the exact
/// <c>Pz.Connector</c> source and meter (<see cref="ConnectorTelemetry.SourceName"/>) are the SDK's own and left
/// alone: the SDK writes none of the keys below. A name that merely starts with <c>Pz.Connector.</c> is foreign.
///
/// <para>Foreign span attributes fail closed: only keys in <see cref="SafeKeys"/> survive, every other tag is
/// removed, and the status description is cleared (libraries put the exception message there). A foreign span's
/// display name can embed data (a request path, a command), so it is replaced by a bounded name built only from
/// the source name plus, if present, the first of <c>db.operation.name</c>, <c>http.request.method</c>,
/// <c>rpc.method</c> and <c>messaging.operation.name</c>, capped at <see cref="MaxNameLength"/> characters. An
/// <see cref="Activity"/> event or link can be neither edited nor removed, and
/// <see cref="Activity.AddException"/> appends <c>exception.message</c> and <c>exception.stacktrace</c> after
/// any <see cref="ActivityListener.ExceptionRecorder"/> has run, so their text cannot be scrubbed. Event names are
/// free text too. A span with any event not named <c>exception</c>, any event tag other than
/// <c>exception.type</c>, or any link carrying tags, is therefore dropped whole, by clearing
/// <see cref="ActivityTraceFlags.Recorded"/>, which the export processors honour. A failed library
/// call loses its span; an exported one could leak a data value. Children of a dropped span export with a
/// parent id that points at the dropped span. This processor must be registered ahead of the exporter.</para></summary>
internal sealed class RedactingProcessor : BaseProcessor<Activity>
{
    /// <summary>The attribute keys a foreign span or metric may export: system and operation names, peer
    /// addresses, status codes and resource identifiers (database, collection, destination). Everything else,
    /// including statements, parameters, headers, paths, ids and exception text, is dropped.</summary>
    internal static readonly IReadOnlySet<string> SafeKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "db.system",
        "db.system.name",
        "db.operation.name",
        "db.operation.batch.size",
        "db.namespace",
        "db.collection.name",
        "db.response.status_code",
        "server.address",
        "server.port",
        "network.peer.address",
        "network.peer.port",
        "network.transport",
        "network.type",
        "network.protocol.name",
        "network.protocol.version",
        "http.request.method",
        "http.response.status_code",
        "url.scheme",
        "rpc.system",
        "rpc.service",
        "rpc.method",
        "rpc.grpc.status_code",
        "messaging.system",
        "messaging.operation.name",
        "messaging.operation.type",
        "messaging.destination.name",
        "messaging.batch.message_count",
        "error.type",
        "az.namespace",
        "otel.status_code",
    };

    /// <summary>The longest span name a foreign span exports.</summary>
    internal const int MaxNameLength = 96;

    private static readonly string[] NameOperationKeys =
        ["db.operation.name", "http.request.method", "rpc.method", "messaging.operation.name"];

    /// <summary>Only the exact <see cref="ConnectorTelemetry.SourceName"/> is the SDK's own; the SDK creates no
    /// other source or meter, so a name merely prefixed with it comes from a library and is filtered.</summary>
    internal static bool IsForeign(string sourceName) =>
        !string.Equals(sourceName, ConnectorTelemetry.SourceName, StringComparison.Ordinal);

    private static string BoundedName(Activity activity)
    {
        var name = activity.Source.Name;
        foreach (var key in NameOperationKeys)
        {
            if (activity.GetTagItem(key) is { } value && value.ToString() is { Length: > 0 } operation)
            {
                name = $"{name} {operation}";
                break;
            }
        }

        return name.Length <= MaxNameLength ? name : name[..MaxNameLength];
    }

    public override void OnEnd(Activity data)
    {
        if (!IsForeign(data.Source.Name))
        {
            return;
        }

        if (data.StatusDescription is not null)
        {
            data.SetStatus(data.Status, null);
        }

        if (CarriesUnsafeEvent(data) || CarriesTaggedLink(data))
        {
            data.ActivityTraceFlags &= ~ActivityTraceFlags.Recorded;
            return;
        }

        data.DisplayName = BoundedName(data);

        List<string>? doomed = null;
        foreach (var tag in data.TagObjects)
        {
            if (!SafeKeys.Contains(tag.Key))
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
            if (activityEvent.Name != "exception")
            {
                return true;
            }

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

    private static bool CarriesTaggedLink(Activity activity)
    {
        foreach (var link in activity.Links)
        {
            if (link.Tags is not null && link.Tags.Any())
            {
                return true;
            }
        }

        return false;
    }
}
