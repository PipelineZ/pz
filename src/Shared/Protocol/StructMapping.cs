using Google.Protobuf.WellKnownTypes;

namespace Pz.Shared;

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
        Value.KindOneofCase.NumberValue => NormalizeNumber(value.NumberValue),
        Value.KindOneofCase.StringValue => value.StringValue,
        Value.KindOneofCase.BoolValue => value.BoolValue,
        Value.KindOneofCase.StructValue => ToNestedMap(value.StructValue),
        Value.KindOneofCase.ListValue => value.ListValue.Values.Select(ToObject).ToList(),
        _ => null,
    };

    /// <summary>protobuf's <c>Struct</c> has only <c>number</c> (a double), so an integer connector
    /// option (`max_connections: 5`) arrives here as <c>5.0</c>. Left as a double, a connector reading
    /// it via <c>ConnectorConfig.GetInt</c> or a bare cast would either misread it or have to know to
    /// re-round it itself; normalize an integral value within the range a double still represents
    /// exactly (|x| &lt;= 2^53) to <see cref="long"/> instead. A fractional value, or one wider than
    /// that range, stays a double -- rounding it would silently change its value or its precision.</summary>
    private static object NormalizeNumber(double d) =>
        double.IsFinite(d) && d == Math.Truncate(d) && d >= -MaxSafeInteger && d <= MaxSafeInteger
            // The (object) cast on the untaken branch is load-bearing: without it the ?: operator
            // unifies on double (long -> double is an implicit numeric conversion), silently widening
            // the long branch back to a double before it is ever boxed.
            ? (long)d
            : (object)d;

    private const double MaxSafeInteger = 9_007_199_254_740_992d; // 2^53

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
    /// ordered columns cannot get them from a <c>Struct</c> — this is a spec-level hazard, a known
    /// limitation of the protocol.</para></summary>
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
