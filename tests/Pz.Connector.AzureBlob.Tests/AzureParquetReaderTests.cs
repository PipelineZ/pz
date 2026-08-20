using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet;
using Parquet.Schema;
using Pz.Connector.AzureBlob;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.AzureBlob.Tests;

public sealed class AzureParquetReaderTests
{
    // Writes a small parquet file to a MemoryStream using Parquet.Net with columns id:int64, name:utf8,
    // amount:double and N rows, then rewinds. Mirrors LocalFiles' ParquetSinkWriteSession column-writing:
    // value columns via WriteAsync(field, ReadOnlyMemory<T?>), strings via the IReadOnlyCollection<string?>
    // overload. The reader is exercised entirely offline over this stream (no docker/network).
    private static MemoryStream WriteSampleParquet(int rows)
    {
        var idField = new DataField<long>("id");
        var nameField = new DataField<string>("name");
        var amountField = new DataField<double>("amount");
        var schema = new ParquetSchema(idField, nameField, amountField);

        var ids = new long?[rows];
        var names = new List<string?>(rows);
        var amounts = new double?[rows];
        for (var i = 0; i < rows; i++)
        {
            ids[i] = i;
            names.Add($"row-{i}");
            amounts[i] = i * 1.5;
        }

        var ms = new MemoryStream();
        var writer = ParquetWriter.CreateAsync(schema, ms).GetAwaiter().GetResult();
        try
        {
            using var rowGroup = writer.CreateRowGroup();
            rowGroup.WriteAsync(idField, new ReadOnlyMemory<long?>(ids)).GetAwaiter().GetResult();
            rowGroup.WriteAsync(nameField, names).GetAwaiter().GetResult();
            rowGroup.WriteAsync(amountField, new ReadOnlyMemory<double?>(amounts)).GetAwaiter().GetResult();
        }
        finally
        {
            writer.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        // ParquetWriter disposal completes the footer but may close the stream; return a fresh readable
        // stream over the bytes so callers can ReadSchema + ReadAsync it independently.
        return new MemoryStream(ms.ToArray());
    }

    [Fact]
    public void ReadSchema_maps_v0_types()
    {
        using var ms = WriteSampleParquet(rows: 1);
        var schema = AzureParquetReader.ReadSchema(ms);

        Assert.Equal(3, schema.FieldsList.Count);
        Assert.Equal("id", schema.FieldsList[0].Name);
        Assert.IsType<Int64Type>(schema.FieldsList[0].DataType);
        Assert.Equal("name", schema.FieldsList[1].Name);
        Assert.IsType<StringType>(schema.FieldsList[1].DataType);
        Assert.Equal("amount", schema.FieldsList[2].Name);
        Assert.IsType<DoubleType>(schema.FieldsList[2].DataType);
    }

    // Writes one parquet file exercising every remaining v0 Arrow type not covered by
    // WriteSampleParquet (int32, bool, decimal128(38,9), date32, timestamp-micros-UTC), written directly
    // via Parquet.Net (not through LocalFilesSink, which refuses decimal128 on write, so this is the only
    // offline way to exercise decimal on the read side; mirrors
    // Pz.Connector.LocalFiles.Tests.ParquetSourceTests.WriteMatrixParquetAsync). Row 0 and row 2 are fully
    // populated (row 2's decimal has a different fractional part to further exercise the rescale-to-scale-9
    // path); row 1 is entirely NULL to cover the null path for every type in one pass.
    private static MemoryStream WriteMultiTypeParquet()
    {
        var intField = new DataField<int>("c_int");
        var boolField = new DataField<bool>("c_bool");
        var decField = new DecimalDataField("c_dec", precision: 38, scale: 9, forceByteArrayEncoding: false, isNullable: true);
        var dateField = new DateTimeDataField("c_date", DateTimeFormat.Date, isNullable: true);
        var tsField = new DateTimeDataField(
            "c_ts", DateTimeFormat.DateAndTime, isAdjustedToUTC: true, unit: DateTimeTimeUnit.Micros, isNullable: true);
        var schema = new ParquetSchema(intField, boolField, decField, dateField, tsField);

        var ints = new int?[] { 1, null, -7 };
        var bools = new bool?[] { true, null, false };
        var decimals = new decimal?[] { 123.45m, null, 7m };
        var dates = new DateTime?[] { new DateTime(2026, 3, 27), null, new DateTime(2000, 1, 1) };
        var timestamps = new DateTime?[]
        {
            new DateTime(2026, 3, 27, 10, 30, 15, DateTimeKind.Utc),
            null,
            new DateTime(2030, 12, 31, 23, 59, 59, DateTimeKind.Utc),
        };

        var ms = new MemoryStream();
        var writer = ParquetWriter.CreateAsync(schema, ms).GetAwaiter().GetResult();
        try
        {
            using var rowGroup = writer.CreateRowGroup();
            rowGroup.WriteAsync(intField, new ReadOnlyMemory<int?>(ints)).GetAwaiter().GetResult();
            rowGroup.WriteAsync(boolField, new ReadOnlyMemory<bool?>(bools)).GetAwaiter().GetResult();
            rowGroup.WriteAsync(decField, new ReadOnlyMemory<decimal?>(decimals)).GetAwaiter().GetResult();
            rowGroup.WriteAsync(dateField, new ReadOnlyMemory<DateTime?>(dates)).GetAwaiter().GetResult();
            rowGroup.WriteAsync(tsField, new ReadOnlyMemory<DateTime?>(timestamps)).GetAwaiter().GetResult();
        }
        finally
        {
            writer.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        return new MemoryStream(ms.ToArray());
    }

    // Writes a single-column parquet file whose type has no v0 mapping (float32 / Arrow FloatType), so
    // AzureParquetTypeMap.MapClrType's fallback throws. A real unsupported-typed fixture (rather than a
    // hand-built Arrow schema) so the test exercises the actual parquet-footer-driven rejection path.
    private static MemoryStream WriteUnsupportedTypeParquet()
    {
        var floatField = new DataField<float>("c_float");
        var schema = new ParquetSchema(floatField);

        var ms = new MemoryStream();
        var writer = ParquetWriter.CreateAsync(schema, ms).GetAwaiter().GetResult();
        try
        {
            using var rowGroup = writer.CreateRowGroup();
            rowGroup.WriteAsync(floatField, new ReadOnlyMemory<float?>(new float?[] { 1.5f })).GetAwaiter().GetResult();
        }
        finally
        {
            writer.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        return new MemoryStream(ms.ToArray());
    }

    [Fact]
    public void ReadSchema_maps_every_remaining_v0_type()
    {
        using var ms = WriteMultiTypeParquet();
        var schema = AzureParquetReader.ReadSchema(ms);

        Assert.Equal(5, schema.FieldsList.Count);
        var byName = schema.FieldsList.ToDictionary(f => f.Name);

        Assert.IsType<Int32Type>(byName["c_int"].DataType);
        Assert.IsType<BooleanType>(byName["c_bool"].DataType);

        var dec = Assert.IsType<Decimal128Type>(byName["c_dec"].DataType);
        Assert.Equal(38, dec.Precision);
        Assert.Equal(9, dec.Scale);

        Assert.IsType<Date32Type>(byName["c_date"].DataType);

        var ts = Assert.IsType<TimestampType>(byName["c_ts"].DataType);
        Assert.Equal(TimeUnit.Microsecond, ts.Unit);
        Assert.Equal("+00:00", ts.Timezone);
    }

    [Fact]
    public void ReadSchema_on_unsupported_parquet_type_throws_named_permanent_error()
    {
        using var ms = WriteUnsupportedTypeParquet();

        var ex = Assert.Throws<PzConnectorException>(() => AzureParquetReader.ReadSchema(ms));

        Assert.False(ex.IsTransient);
        Assert.Contains("c_float", ex.Message);
    }
}
