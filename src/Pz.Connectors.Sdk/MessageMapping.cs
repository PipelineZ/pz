using Apache.Arrow;
using Apache.Arrow.Ipc;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;
using Pz.Shared;

namespace Pz.Connectors.Sdk;

/// <summary>Wire spec messages to their ABI records. Every optional field is carried, including the
/// two "was it set" booleans that keep null distinguishable from empty.</summary>
internal static class SpecMapping
{
    public static DatasetSpec ToDatasetSpec(DatasetSpecMsg spec) =>
        new(spec.Source, spec.Dataset, StructMapping.ToDictionary(spec.Options))
        {
            WatermarkCursor = spec.HasWatermarkCursor ? spec.WatermarkCursor : null,
            WatermarkValue = spec.HasWatermarkValue ? spec.WatermarkValue : null,
            WatermarkUpperBound = spec.HasWatermarkUpperBound ? spec.WatermarkUpperBound : null,
            WatermarkLowerInclusive = spec.WatermarkLowerInclusive,
            PriorSyncState = spec.HasPriorSyncState ? spec.PriorSyncState : null,
            ChangeCapture = spec.ChangeCapture,
            ChangeCaptureSlot = spec.HasChangeCaptureSlot ? spec.ChangeCaptureSlot : null,
        };

    public static OutputSpec ToOutputSpec(OutputSpecMsg spec) =>
        new(spec.Sink, spec.Output, spec.Mode, spec.SchemaPolicy, StructMapping.ToDictionary(spec.Options))
        {
            Keys = spec.Keys.ToArray(),
            OnDelete = spec.HasOnDelete ? spec.OnDelete : null,
            MaxTextLengths = spec.MaxTextLengthsSet
                ? spec.MaxTextLengths.ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal)
                : null,
            Attempt = spec.Attempt is { } attempt
                ? new WriteAttempt(attempt.Node, attempt.Run, attempt.Ordinal)
                : null,
        };

    /// <summary>The run context a request carries; null when the host sent none (an older host).</summary>
    public static ConnectorRunContext? ToRunContext(RunContextMsg? context) =>
        context is null
            ? null
            : new ConnectorRunContext(context.Run, context.HasNode ? context.Node : null)
            {
                StartedAt = context.HasStartedAt && DateTimeOffset.TryParse(context.StartedAt,
                    System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind,
                    out var startedAt)
                    ? startedAt
                    : null,
            };

    public static ReadHints ToReadHints(ReadHintsMsg? hints) => hints is null
        ? ReadHints.None
        : new ReadHints(
            hints.ColumnsSet ? hints.Columns.ToArray() : null,
            hints.HasPredicateSql ? hints.PredicateSql : null,
            hints.HasLimit ? hints.Limit : null);

    public static BatchOptions ToBatchOptions(BatchOptionsMsg? options) => options is null
        ? BatchOptions.Default
        : new BatchOptions(
            options.TargetBatchBytes > 0 ? options.TargetBatchBytes : BatchOptions.Default.TargetBatchBytes,
            options.MaxRowsPerBatch > 0 ? options.MaxRowsPerBatch : BatchOptions.Default.MaxRowsPerBatch);

    public static AbortSemanticsMsg ToAbortSemanticsMsg(AbortSemantics semantics) => semantics switch
    {
        AbortSemantics.DiscardsAll => AbortSemanticsMsg.AbortSemanticsDiscardsAll,
        AbortSemantics.BestEffort => AbortSemanticsMsg.AbortSemanticsBestEffort,
        AbortSemantics.None => AbortSemanticsMsg.AbortSemanticsNone,
        _ => throw new ArgumentOutOfRangeException(nameof(semantics), semantics, "unrecognized AbortSemantics"),
    };
}

/// <summary>Arrow schemas cross the control plane as an Arrow IPC stream that carries the schema
/// message and nothing else.</summary>
internal static class SchemaCodec
{
    public static async Task<ByteString> SerializeAsync(Schema schema, CancellationToken ct)
    {
        using var buffer = new MemoryStream();
        using (var writer = new ArrowStreamWriter(buffer, schema, leaveOpen: true))
        {
            await writer.WriteStartAsync(ct).ConfigureAwait(false);
            await writer.WriteEndAsync(ct).ConfigureAwait(false);
        }

        return ByteString.CopyFrom(buffer.ToArray());
    }

    public static async ValueTask<Schema> DeserializeAsync(ByteString bytes, CancellationToken ct)
    {
        using var buffer = new MemoryStream(bytes.ToByteArray(), writable: false);
        using var reader = new ArrowStreamReader(buffer, leaveOpen: true);
        return await reader.GetSchema(ct).ConfigureAwait(false);
    }
}
