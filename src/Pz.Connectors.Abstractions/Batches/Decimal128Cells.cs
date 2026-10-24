using System.Data.SqlTypes;
using System.Globalization;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connectors.Abstractions.Batches;

/// <summary>Exact reads of a <see cref="Decimal128Array"/> cell. Arrow's <c>GetValue</c> and
/// <c>TryGetValue</c> return <see cref="decimal"/>, which holds a 96-bit unscaled integer and a scale of
/// at most 28; a decimal128(38,x) cell outside that range comes back with digits silently dropped (or,
/// further out, as an <see cref="OverflowException"/>). Every read here checks the raw 128-bit unscaled
/// integer first and falls back to a representation that carries all 38 digits.</summary>
public static class Decimal128Cells
{
    private static readonly Int128 MantissaLimit = Int128.One << 96;

    /// <summary>True when the non-null cell at <paramref name="row"/> is exactly representable as a
    /// <see cref="decimal"/>: its unscaled integer's magnitude is below 2^96 and the column's scale is at
    /// most 28. Reads the little-endian two's-complement bytes directly, so it honours a sliced array's
    /// offset and never goes through the lossy conversion.</summary>
    public static bool FitsSystemDecimal(Decimal128Array array, int row)
    {
        ArgumentNullException.ThrowIfNull(array);
        if (((Decimal128Type)array.Data.DataType).Scale > 28)
        {
            return false;
        }

        var unscaled = MemoryMarshal.Read<Int128>(array.ValueBuffer.Span.Slice((array.Offset + row) * 16, 16));
        return unscaled > -MantissaLimit && unscaled < MantissaLimit;
    }

    /// <summary>The non-null cell as a <see cref="decimal"/> when it fits one, else as a
    /// <see cref="SqlDecimal"/> (38 digits, exact). Both are accepted by <see cref="ArrowBatchBuilder"/>'s
    /// decimal column, so a re-batching caller never needs to tell them apart.</summary>
    public static object GetExactValue(Decimal128Array array, int row) =>
        // Both arms boxed explicitly: decimal converts implicitly to SqlDecimal, so an unboxed conditional
        // would turn every in-range value into a SqlDecimal too.
        FitsSystemDecimal(array, row) ? (object)array.GetValue(row)!.Value : array.GetSqlDecimal(row)!.Value;

    /// <summary>The non-null cell as invariant decimal text: <see cref="decimal"/>'s own formatting when the
    /// value fits one (so existing output stays byte-identical), else Arrow's exact digit string.</summary>
    public static string GetExactString(Decimal128Array array, int row) =>
        FitsSystemDecimal(array, row)
            ? array.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture)
            : array.GetString(row);
}
