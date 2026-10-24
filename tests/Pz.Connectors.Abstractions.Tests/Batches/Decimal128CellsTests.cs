using System.Data.SqlTypes;
using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connectors.Abstractions.Tests.Batches;

/// <summary><see cref="Decimal128Cells"/> and the decimal column of <see cref="ArrowBatchBuilder"/>: a
/// decimal128 cell beyond System.Decimal's range is read and re-batched with every digit intact, and one
/// inside it reads exactly as before.</summary>
public class Decimal128CellsTests
{
    private const string Beyond = "12345678901234567890123456789.123456789";

    private static readonly Decimal128Type Type38x9 = new(38, 9);

    /// <summary>A decimal128 column of <paramref name="type"/> whose cells hold exactly these unscaled
    /// integers, written straight into the value buffer so a value no Arrow builder overload accepts can
    /// still be placed at the 2^96 boundary.</summary>
    private static Decimal128Array FromUnscaled(Decimal128Type type, params BigInteger[] unscaled)
    {
        var bytes = new byte[unscaled.Length * 16];
        for (var i = 0; i < unscaled.Length; i++)
        {
            var value = (Int128)unscaled[i];
            System.Runtime.InteropServices.MemoryMarshal.Write(bytes.AsSpan(i * 16, 16), in value);
        }

        var data = new ArrayData(type, unscaled.Length, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(bytes)]);
        return new Decimal128Array(data);
    }

    private static Decimal128Array Of(params string[] values)
    {
        var builder = new Decimal128Array.Builder(Type38x9);
        foreach (var value in values)
        {
            builder.Append(SqlDecimal.Parse(value));
        }

        return builder.Build();
    }

    [Fact]
    public void Mantissa_boundary_is_2_to_the_96_either_sign()
    {
        var limit = BigInteger.One << 96;
        var array = FromUnscaled(Type38x9, limit - 1, -(limit - 1), limit, -limit);

        Assert.True(Decimal128Cells.FitsSystemDecimal(array, 0));
        Assert.True(Decimal128Cells.FitsSystemDecimal(array, 1));
        Assert.False(Decimal128Cells.FitsSystemDecimal(array, 2));
        Assert.False(Decimal128Cells.FitsSystemDecimal(array, 3));
    }

    [Fact]
    public void Scale_above_28_never_fits_even_for_a_small_value()
    {
        var array = FromUnscaled(new Decimal128Type(38, 30), 1);
        Assert.False(Decimal128Cells.FitsSystemDecimal(array, 0));
        Assert.Equal("0.000000000000000000000000000001", Decimal128Cells.GetExactString(array, 0));
    }

    [Fact]
    public void Beyond_range_reads_exactly_as_SqlDecimal_and_text()
    {
        var array = Of(Beyond, "-" + Beyond);

        Assert.Equal(SqlDecimal.Parse(Beyond), Assert.IsType<SqlDecimal>(Decimal128Cells.GetExactValue(array, 0)));
        Assert.Equal(Beyond, Decimal128Cells.GetExactString(array, 0));
        Assert.Equal("-" + Beyond, Decimal128Cells.GetExactString(array, 1));
    }

    [Fact]
    public void In_range_reads_as_decimal_with_unchanged_text()
    {
        var array = Of("1234.567891234", "-0.5");

        Assert.Equal(1234.567891234m, Assert.IsType<decimal>(Decimal128Cells.GetExactValue(array, 0)));
        Assert.Equal("1234.567891234", Decimal128Cells.GetExactString(array, 0));
        Assert.Equal("-0.500000000", Decimal128Cells.GetExactString(array, 1));
    }

    [Fact]
    public void A_sliced_array_reads_the_cell_at_its_own_offset()
    {
        var sliced = (Decimal128Array)Of("1.5", Beyond, "2.5").Slice(1, 2);

        Assert.False(Decimal128Cells.FitsSystemDecimal(sliced, 0));
        Assert.Equal(Beyond, Decimal128Cells.GetExactString(sliced, 0));
        Assert.True(Decimal128Cells.FitsSystemDecimal(sliced, 1));
        Assert.Equal(2.5m, Decimal128Cells.GetExactValue(sliced, 1));
    }

    [Fact]
    public void Builder_rebatches_beyond_range_values_exactly_through_both_entry_points()
    {
        var schema = new Schema([new Field("amount", Type38x9, nullable: true)], null);
        using var source = new RecordBatch(schema, [Of(Beyond, "1.5")], 2);

        var copied = new ArrowBatchBuilder(schema);
        copied.AppendFrom([source.Column(0)], 0);
        copied.AppendFrom([source.Column(0)], 1);
        using var fromArrow = copied.Flush()!;

        var appended = new ArrowBatchBuilder(schema);
        appended.AppendRow([Decimal128Cells.GetExactValue((Decimal128Array)source.Column(0), 0)]);
        appended.AppendRow([SqlDecimal.Parse("1.5")]);
        appended.AppendRow([null]);
        using var fromRows = appended.Flush()!;

        foreach (var batch in new[] { fromArrow, fromRows })
        {
            var amounts = (Decimal128Array)batch.Column(0);
            Assert.Equal(Beyond, amounts.GetString(0));
            Assert.Equal(1.5m, amounts.GetValue(1));
        }

        Assert.True(fromRows.Column(0).IsNull(2));
    }
}
