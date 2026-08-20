using System.Text.Json;

namespace Pz.Diagnostics.Events;

/// <summary>Single source of truth for the run-event stream's JSON shape: snake_case event names and
/// camelCase per-event fields. Both <c>Pz.Cli.Rendering.JsonRenderer</c> (stdout NDJSON) and
/// <c>Pz.State.SqlServer.SqlEventSink</c> (persisted rows) call this rather than keeping a switch of
/// their own, so the two representations cannot drift the next time a <see cref="RunEvent"/> type or
/// field is added. Lives here, not in either consumer, because this project owns
/// <see cref="RunEvent"/> and stays BCL-only -- both <c>Pz.Cli</c> and <c>Pz.State.SqlServer</c>
/// already reach it.
///
/// <c>https://pipelinez.dev/events/</c> is a stability contract on the stdout shape this feeds, and
/// <c>JsonRenderer</c>'s own tests (plus <c>EventsDocReflectionTests</c>) hold it to the byte.</summary>
public static class RunEventFields
{
    public static string EventName(RunEvent evt) => evt switch
    {
        RunStartedEvent => "run_started",
        NodeStartedEvent => "node_started",
        NodeProgressEvent => "node_progress",
        RetryScheduledEvent => "retry_scheduled",
        BreakerStateChangedEvent => "breaker_state_changed",
        NodeCompletedEvent => "node_completed",
        RunCompletedEvent => "run_completed",
        RetentionSweptEvent => "retention_swept",
        SourceDriftDetectedEvent => "source_drift_detected",
        MergeKeyDuplicatesDetectedEvent => "merge_key_duplicates_detected",
        LossyIntegerInferenceDetectedEvent => "lossy_integer_inference_detected",
        AmbiguousDateInferenceDetectedEvent => "ambiguous_date_inference_detected",
        _ => throw new ArgumentOutOfRangeException(nameof(evt), evt, "unknown RunEvent type"),
    };

    /// <summary>Writes only the per-event-type fields: no envelope (<c>event</c>/<c>at</c>/<c>runId</c>)
    /// and no surrounding <c>WriteStartObject</c>/<c>WriteEndObject</c> -- both are the caller's to own,
    /// since <c>JsonRenderer</c> writes the envelope inline and <c>SqlEventSink</c> puts those three
    /// fields in their own SQL columns instead of the JSON body.</summary>
    public static void WriteFields(Utf8JsonWriter json, RunEvent evt)
    {
        switch (evt)
        {
            case RunStartedEvent e:
                json.WriteString("projectName", e.ProjectName);
                json.WriteNumber("nodeCount", e.NodeCount);
                break;

            case NodeStartedEvent e:
                json.WriteString("nodeId", e.NodeId);
                json.WriteString("kind", e.Kind);
                json.WriteString("name", e.Name);
                break;

            case NodeProgressEvent e:
                json.WriteString("nodeId", e.NodeId);
                json.WriteString("name", e.Name);
                json.WriteNumber("rows", e.Rows);
                json.WriteNumber("bytes", e.Bytes);
                json.WriteNumber("batches", e.Batches);
                break;

            case RetryScheduledEvent e:
                json.WriteString("nodeId", e.NodeId);
                json.WriteString("name", e.Name);
                json.WriteNumber("attempt", e.Attempt);
                json.WriteNumber("maxAttempts", e.MaxAttempts);
                json.WriteNumber("delayMs", e.DelayMs);
                json.WriteString("reason", e.Reason);
                break;

            case BreakerStateChangedEvent e:
                json.WriteString("instance", e.Instance);
                json.WriteString("oldState", e.OldState);
                json.WriteString("newState", e.NewState);
                json.WriteString("trigger", e.Trigger);
                json.WriteNumber("coolDownMs", e.CoolDownMs);
                break;

            case NodeCompletedEvent e:
                json.WriteString("nodeId", e.NodeId);
                json.WriteString("kind", e.Kind);
                json.WriteString("name", e.Name);
                json.WriteString("status", e.Status);
                json.WriteNumber("rows", e.Rows);
                json.WriteNumber("durationMs", e.DurationMs);
                WriteNullableString(json, "errorCode", e.ErrorCode);
                WriteNullableString(json, "errorMessage", e.ErrorMessage);
                if (e.Timings is { } timings)
                {
                    json.WriteStartObject("timings");
                    json.WriteNumber("producerStallMs", timings.ProducerStallMs);
                    json.WriteNumber("consumerStallMs", timings.ConsumerStallMs);
                    json.WriteEndObject();
                }
                else
                {
                    json.WriteNull("timings");
                }

                // Additive-optional (https://pipelinez.dev/events/ append-only rule): omitted when null so pre-existing
                // node_completed lines stay byte-identical -- the `timings` omission precedent, not the
                // errorCode/WriteNull one, because absence (not null) is what keeps old consumers' bytes stable.
                if (e.Provenance is { } provenance)
                {
                    json.WriteString("provenance", provenance);
                }

                if (e.Ops is { } ops)
                {
                    json.WriteStartObject("ops");
                    json.WriteNumber("executed", ops.Executed);
                    json.WriteNumber("retried", ops.Retried);
                    json.WriteNumber("throttleWaitMs", ops.ThrottleWaitMs);
                    json.WriteEndObject();
                }

                if (e.Partitions is { } partitionStats)
                {
                    json.WriteStartObject("partitions");
                    json.WriteNumber("total", partitionStats.Total);
                    json.WriteNumber("completed", partitionStats.Completed);
                    json.WriteNumber("reused", partitionStats.Reused);
                    json.WriteNumber("resumed", partitionStats.Resumed);
                    json.WriteEndObject();
                }

                if (e.Delivery is { } deliveryPayload)
                {
                    json.WriteStartObject("delivery");
                    json.WriteString("abortSemantics", deliveryPayload.AbortSemantics);
                    json.WriteNumber("rowsVisible", deliveryPayload.RowsVisible);
                    json.WriteNumber("resumedRows", deliveryPayload.ResumedRows);
                    json.WriteEndObject();
                }

                if (e.Cdc is { } cdcPayload)
                {
                    json.WriteStartObject("cdc");
                    json.WriteNumber("inserts", cdcPayload.Inserts);
                    json.WriteNumber("updates", cdcPayload.Updates);
                    json.WriteNumber("deletes", cdcPayload.Deletes);
                    WriteNullableString(json, "position", cdcPayload.Position);
                    json.WriteEndObject();
                }

                break;

            case RunCompletedEvent e:
                json.WriteString("status", e.Status);
                json.WriteNumber("succeeded", e.Succeeded);
                json.WriteNumber("failed", e.Failed);
                json.WriteNumber("skipped", e.Skipped);
                json.WriteNumber("durationMs", e.DurationMs);
                break;

            case RetentionSweptEvent e:
                json.WriteNumber("runsSwept", e.RunsSwept);
                json.WriteNumber("bytesFreed", e.BytesFreed);
                json.WriteNumber("failures", e.Failures);
                break;

            case SourceDriftDetectedEvent e:
                json.WriteString("nodeId", e.NodeId);
                json.WriteString("connection", e.Connection);
                json.WriteString("entity", e.Entity);
                json.WriteString("policy", e.Policy);
                json.WriteStartArray("changes");
                foreach (var change in e.Changes)
                {
                    json.WriteStartObject();
                    json.WriteString("kind", change.Kind);
                    json.WriteString("column", change.Column);
                    WriteNullableString(json, "from", change.From);
                    WriteNullableString(json, "to", change.To);
                    json.WriteEndObject();
                }
                json.WriteEndArray();
                json.WriteStartArray("observed");
                foreach (var col in e.Observed)
                {
                    json.WriteStartObject();
                    json.WriteString("name", col.Name);
                    json.WriteString("type", col.Type);
                    json.WriteEndObject();
                }
                json.WriteEndArray();
                json.WriteString("hintsHash", e.HintsHash);
                break;

            case MergeKeyDuplicatesDetectedEvent e:
                json.WriteString("nodeId", e.NodeId);
                json.WriteString("output", e.Output);
                json.WriteStartArray("keys");
                foreach (var key in e.Keys)
                {
                    json.WriteStringValue(key);
                }
                json.WriteEndArray();
                json.WriteNumber("duplicateGroups", e.DuplicateGroups);
                json.WriteNumber("extraRows", e.ExtraRows);
                break;

            case LossyIntegerInferenceDetectedEvent e:
                json.WriteString("nodeId", e.NodeId);
                json.WriteString("connection", e.Connection);
                json.WriteString("entity", e.Entity);
                json.WriteStartArray("columns");
                foreach (var column in e.Columns)
                {
                    json.WriteStringValue(column);
                }
                json.WriteEndArray();
                break;

            case AmbiguousDateInferenceDetectedEvent e:
                json.WriteString("nodeId", e.NodeId);
                json.WriteString("connection", e.Connection);
                json.WriteString("entity", e.Entity);
                json.WriteStartArray("columns");
                foreach (var column in e.Columns)
                {
                    json.WriteStringValue(column);
                }
                json.WriteEndArray();
                json.WriteString("format", e.Format);
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(evt), evt, "unknown RunEvent type");
        }
    }

    private static void WriteNullableString(Utf8JsonWriter json, string name, string? value)
    {
        if (value is null)
        {
            json.WriteNull(name);
        }
        else
        {
            json.WriteString(name, value);
        }
    }
}
