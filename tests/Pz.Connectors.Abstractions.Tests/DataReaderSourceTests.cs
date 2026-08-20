using System.Data;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connectors.Abstractions.Tests;

/// <summary>Unit tests for <see cref="DataReaderSource"/> against a <c>DataTable.CreateDataReader()</c>
/// fake ADO.NET reader. <see cref="System.Data.DataColumn"/> cannot carry a <see cref="DateOnly"/>
/// value, so this suite cannot exercise the date32 branch of the v0 type matrix (int32/int64/double/
/// decimal128/utf8/bool/date32/timestamp) via a DataTable-backed reader; date32 coverage lives in the
/// Postgres container acceptance suite (<c>PgTypeMatrixTests</c>) instead, against a real provider
/// reader that returns <see cref="DateOnly"/> for a pg <c>date</c> column.</summary>
public class DataReaderSourceTests
{
    private static DataTable MatrixTable()
    {
        var table = new DataTable("matrix");
        table.Columns.Add("id", typeof(int));
        table.Columns.Add("big", typeof(long));
        table.Columns.Add("amount", typeof(double));
        table.Columns.Add("price", typeof(decimal));
        table.Columns.Add("name", typeof(string));
        table.Columns.Add("active", typeof(bool));
        table.Columns.Add("created", typeof(DateTime));
        table.Columns.Add("created_offset", typeof(DateTimeOffset));
        return table;
    }

    [Fact]
    public void Schema_derived_from_reader_field_types()
    {
        var table = MatrixTable();
        using var reader = table.CreateDataReader();

        var schema = DataReaderSource.BuildArrowSchema(reader);

        Assert.Equal(8, schema.FieldsList.Count);
        AssertField(schema, 0, "id", ArrowTypeId.Int32);
        AssertField(schema, 1, "big", ArrowTypeId.Int64);
        AssertField(schema, 2, "amount", ArrowTypeId.Double);
        AssertField(schema, 3, "price", ArrowTypeId.Decimal128);
        AssertField(schema, 4, "name", ArrowTypeId.String);
        AssertField(schema, 5, "active", ArrowTypeId.Boolean);
        AssertField(schema, 6, "created", ArrowTypeId.Timestamp);
        AssertField(schema, 7, "created_offset", ArrowTypeId.Timestamp);

        var price = (Decimal128Type)schema.FieldsList[3].DataType;
        Assert.Equal(38, price.Precision);
        Assert.Equal(9, price.Scale);

        var created = (TimestampType)schema.FieldsList[6].DataType;
        Assert.Equal(TimeUnit.Microsecond, created.Unit);
        Assert.Equal("+00:00", created.Timezone);

        foreach (var field in schema.FieldsList)
        {
            Assert.True(field.IsNullable);
        }
    }

    private static void AssertField(Schema schema, int index, string name, ArrowTypeId typeId)
    {
        Assert.Equal(name, schema.FieldsList[index].Name);
        Assert.Equal(typeId, schema.FieldsList[index].DataType.TypeId);
    }

    [Fact]
    public async Task Rows_pivot_to_batches_at_byte_target()
    {
        var table = MatrixTable();
        var wide = new string('x', 500);
        for (var i = 0; i < 20; i++)
        {
            table.Rows.Add(i, (long)i * 10, i * 0.5, (decimal)i, wide + i, i % 2 == 0,
                new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(i),
                new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).AddDays(i));
        }

        using var reader = table.CreateDataReader();

        var batches = new List<RecordBatch>();
        await foreach (var batch in DataReaderSource.ReadBatchesAsync(reader, targetBatchBytes: 512))
        {
            batches.Add(batch);
        }

        Assert.True(batches.Count > 1, $"expected multiple batches, got {batches.Count}");

        var totalRows = batches.Sum(b => b.Length);
        Assert.Equal(20, totalRows);

        foreach (var batch in batches)
        {
            batch.Dispose();
        }
    }

    [Fact]
    public async Task Nulls_roundtrip_every_column()
    {
        var table = MatrixTable();
        table.Rows.Add(DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);

        using var reader = table.CreateDataReader();

        var batches = new List<RecordBatch>();
        await foreach (var batch in DataReaderSource.ReadBatchesAsync(reader, targetBatchBytes: 32 * 1024 * 1024))
        {
            batches.Add(batch);
        }

        var batch0 = Assert.Single(batches);
        Assert.Equal(1, batch0.Length);
        for (var col = 0; col < batch0.ColumnCount; col++)
        {
            Assert.True(batch0.Column(col).IsNull(0), $"column {col} expected null");
        }

        batch0.Dispose();
    }

    [Fact]
    public void Unmapped_type_throws_named_error()
    {
        var table = new DataTable("bad");
        table.Columns.Add("token", typeof(Guid));
        table.Rows.Add(Guid.NewGuid());

        using var reader = table.CreateDataReader();

        var ex = Assert.Throws<PzConnectorException>(() => DataReaderSource.BuildArrowSchema(reader));

        Assert.Contains("token", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Guid", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }
}
