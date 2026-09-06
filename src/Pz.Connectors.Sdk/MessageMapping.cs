using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;

namespace Pz.Connectors.Sdk;

/// <summary><c>google.protobuf.Struct</c> is the only shape configuration ever arrives in, and this is
/// the whole of its meaning: string, double, bool, null, list, nested map.</summary>
internal static class StructMapping
{
    public static IReadOnlyDictionary<string, object?> ToDictionary(Struct? value)
    {
        var result = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (value is null)
        {
            return result;
        }

        foreach (var (key, item) in value.Fields)
        {
            result[key] = ToObject(item);
        }

        return result;
    }

    private static object? ToObject(Value value) => value.KindCase switch
    {
        Value.KindOneofCase.NumberValue => value.NumberValue,
        Value.KindOneofCase.StringValue => value.StringValue,
        Value.KindOneofCase.BoolValue => value.BoolValue,
        Value.KindOneofCase.StructValue => ToNestedMap(value.StructValue),
        Value.KindOneofCase.ListValue => value.ListValue.Values.Select(ToObject).ToList(),
        _ => null,
    };

    /// <summary>A nested map arrives with no .NET type attached, and the ABI reads one of them two
    /// different ways: <c>columns:</c> is an <c>IReadOnlyDictionary&lt;string, string&gt;</c> in-proc
    /// (Pz.Engine's SpecBuilder puts the declared contract in the options bag under that type), while
    /// everything else is read as <c>object?</c>-valued. An all-string map is therefore rebuilt as a
    /// value that answers to both shapes rather than picking one and silently making the other
    /// option invisible to the connector.
    ///
    /// <para>ORDER IS LOAD-BEARING AND ONLY C# GUARANTEES IT. LocalFiles binds a <c>columns:</c>
    /// contract to the csv header BY POSITION, so the order the host declared must survive the wire.
    /// It does here because Google.Protobuf's <c>MapField</c> enumerates in insertion order on the C#
    /// side, and the rebuild below walks the fields in that order. That is an implementation property
    /// of one runtime, not a protobuf guarantee: proto3 map entries are explicitly unordered, and a
    /// peer in another language may hand its map back in any order at all. A connector that needs
    /// ordered columns cannot get them from a <c>Struct</c> — this is a spec-level hazard, tracked for
    /// the protocol follow-up rather than papered over here.</para></summary>
    private static object ToNestedMap(Struct value)
    {
        var plain = new Dictionary<string, object?>(StringComparer.Ordinal);
        var strings = new Dictionary<string, string>(StringComparer.Ordinal);
        var allStrings = value.Fields.Count > 0;
        foreach (var (key, item) in value.Fields)
        {
            plain[key] = ToObject(item);
            if (item.KindCase == Value.KindOneofCase.StringValue)
            {
                strings[key] = item.StringValue;
            }
            else
            {
                allStrings = false;
            }
        }

        return allStrings ? new StringValuedMap(strings) : plain;
    }
}

/// <summary>A nested option map whose values are all strings, readable either as
/// <c>IReadOnlyDictionary&lt;string, object?&gt;</c> (inherited) or as
/// <c>IReadOnlyDictionary&lt;string, string&gt;</c> (explicit). See
/// <c>StructMapping.ToNestedMap</c> for why both are needed.</summary>
internal sealed class StringValuedMap : Dictionary<string, object?>, IReadOnlyDictionary<string, string>
{
    private readonly Dictionary<string, string> _strings;

    public StringValuedMap(Dictionary<string, string> strings)
        : base(strings.Count, StringComparer.Ordinal)
    {
        _strings = strings;
        foreach (var (key, value) in strings)
        {
            Add(key, value);
        }
    }

    string IReadOnlyDictionary<string, string>.this[string key] => _strings[key];

    IEnumerable<string> IReadOnlyDictionary<string, string>.Keys => _strings.Keys;

    IEnumerable<string> IReadOnlyDictionary<string, string>.Values => _strings.Values;

    bool IReadOnlyDictionary<string, string>.TryGetValue(string key, out string value) =>
        _strings.TryGetValue(key, out value!);

    IEnumerator<KeyValuePair<string, string>> IEnumerable<KeyValuePair<string, string>>.GetEnumerator() =>
        _strings.GetEnumerator();
}

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
