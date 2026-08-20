using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Formats;

namespace Pz.Connectors.Abstractions.Tests.Formats;

/// <summary>Offline determinism tests for <see cref="NdjsonCodec.WriteAsync"/>: byte-exact NDJSON for the
/// happy path, the null-cell case, and the full "columns:" contract type matrix (mirrors
/// <c>AzureBlobFormat.ScalarToString</c>'s type coverage -- int/bigint/double/decimal/varchar/boolean/
/// date/timestamp).</summary>
public class NdjsonCodecWriteTests
{
    private static readonly Schema TwoColumnSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: true),
        new Field("level", StringType.Default, nullable: true),
    ], null);

    [Fact]
    public async Task Writes_one_object_per_row_lf_terminated()
    {
        var idBuilder = new Int64Array.Builder();
        var levelBuilder = new StringArray.Builder();
        idBuilder.Append(1);
        levelBuilder.Append("info");
        idBuilder.Append(2);
        levelBuilder.Append("warn");
        using var batch = new RecordBatch(TwoColumnSchema, [idBuilder.Build(), levelBuilder.Build()], 2);

        using var ms = new MemoryStream();
        await NdjsonCodec.WriteAsync(batch, ms, default);
        var text = Encoding.UTF8.GetString(ms.ToArray());
        Assert.Equal("{\"id\":1,\"level\":\"info\"}\n{\"id\":2,\"level\":\"warn\"}\n", text);
    }

    [Fact]
    public async Task Null_value_emits_json_null()
    {
        var idBuilder = new Int64Array.Builder();
        var levelBuilder = new StringArray.Builder();
        idBuilder.Append(1);
        levelBuilder.AppendNull();
        using var batch = new RecordBatch(TwoColumnSchema, [idBuilder.Build(), levelBuilder.Build()], 1);

        using var ms = new MemoryStream();
        await NdjsonCodec.WriteAsync(batch, ms, default);
        Assert.Equal("{\"id\":1,\"level\":null}\n", Encoding.UTF8.GetString(ms.ToArray()));
    }

    [Fact]
    public async Task Serializes_every_contract_type_deterministically()
    {
        var schema = new Schema(
        [
            new Field("n", Int32Type.Default, nullable: true),
            new Field("big", Int64Type.Default, nullable: true),
            new Field("amt", new Decimal128Type(38, 9), nullable: true),
            new Field("price", DoubleType.Default, nullable: true),
            new Field("active", BooleanType.Default, nullable: true),
            new Field("day", Date32Type.Default, nullable: true),
            new Field("created", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: true),
            new Field("name", StringType.Default, nullable: true),
        ], null);

        var n = new Int32Array.Builder();
        var big = new Int64Array.Builder();
        var amt = new Decimal128Array.Builder(new Decimal128Type(38, 9));
        var price = new DoubleArray.Builder();
        var active = new BooleanArray.Builder();
        var day = new Date32Array.Builder();
        var created = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var name = new StringArray.Builder();

        n.Append(7);
        big.Append(123456789012345L);
        amt.Append(12345.678901234m);
        price.Append(3.14);
        active.Append(true);
        day.Append(new DateOnly(2026, 7, 13));
        created.Append(new DateTimeOffset(2026, 7, 13, 10, 30, 15, TimeSpan.Zero));
        name.Append("widget");

        using var batch = new RecordBatch(schema,
        [
            n.Build(), big.Build(), amt.Build(), price.Build(),
            active.Build(), day.Build(), created.Build(), name.Build(),
        ], 1);

        using var ms = new MemoryStream();
        await NdjsonCodec.WriteAsync(batch, ms, default);
        var text = Encoding.UTF8.GetString(ms.ToArray());

        Assert.Equal(
            "{\"n\":7,\"big\":123456789012345,\"amt\":12345.678901234,\"price\":3.14,\"active\":true," +
            "\"day\":\"2026-07-13\",\"created\":\"2026-07-13T10:30:15.000000Z\",\"name\":\"widget\"}\n",
            text);
    }

    [Fact]
    public async Task Non_finite_double_emits_json_null()
    {
        var schema = new Schema(
        [
            new Field("price", DoubleType.Default, nullable: true),
        ], null);

        var price = new DoubleArray.Builder();
        price.Append(double.NaN);
        price.Append(double.PositiveInfinity);
        price.Append(double.NegativeInfinity);

        using var batch = new RecordBatch(schema, [price.Build()], 3);

        using var ms = new MemoryStream();
        await NdjsonCodec.WriteAsync(batch, ms, default);
        var text = Encoding.UTF8.GetString(ms.ToArray());

        Assert.Equal(
            "{\"price\":null}\n{\"price\":null}\n{\"price\":null}\n",
            text);
    }
}
