using System.Collections;
using System.Data.Common;
using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.SqlServer;

/// <summary>Forward-only DbDataReader over ONE engine-owned RecordBatch: the bridge that
/// lets SqlBulkCopy stream Arrow data without a DataTable or row materialization. Values convert to
/// the CLR shapes SqlClient's TDS writer expects: date32 → DateTime (date), timestamp(µs, UTC) →
/// UtcDateTime (Kind=Utc; SqlClient's datetime2 writer serializes wall-clock components and ignores Kind).
/// The batch must stay valid for this reader's lifetime — the sink session's WriteBatchAsync completes
/// the bulk write before returning, inside the engine's ownership window.</summary>
internal sealed class ArrowBatchDataReader(RecordBatch batch) : DbDataReader
{
    private int _row = -1;

    private static readonly Func<IArrowArray, int, object> Date32 =
        (a, r) => ((Date32Array)a).GetDateOnly(r)!.Value.ToDateTime(TimeOnly.MinValue);

    private object Cell(int ordinal)
    {
        var column = batch.Column(ordinal);
        return column switch
        {
            Int32Array a => a.GetValue(_row)!.Value,
            Int64Array a => a.GetValue(_row)!.Value,
            DoubleArray a => a.GetValue(_row)!.Value,
            Decimal128Array a => ReadDecimal(a, ordinal),
            StringArray a => a.GetString(_row),
            BooleanArray a => a.GetValue(_row)!.Value,
            Date32Array => Date32(column, _row),
            TimestampArray a => a.GetTimestamp(_row)!.Value.UtcDateTime, // naive UTC into datetime2(6)
            _ => throw new NotSupportedException($"unsupported Arrow column type {column.Data.DataType}"),
        };
    }

    public override bool Read() => ++_row < batch.Length;
    public override int FieldCount => batch.ColumnCount;
    public override bool IsDBNull(int ordinal) => batch.Column(ordinal).IsNull(_row);
    public override object GetValue(int ordinal) => IsDBNull(ordinal) ? DBNull.Value : Cell(ordinal);
    public override string GetName(int ordinal) => batch.Schema.FieldsList[ordinal].Name;
    public override int GetOrdinal(string name) => batch.Schema.GetFieldIndex(name);
    public override Type GetFieldType(int ordinal) => batch.Schema.FieldsList[ordinal].DataType.TypeId switch
    {
        Apache.Arrow.Types.ArrowTypeId.Int32 => typeof(int),
        Apache.Arrow.Types.ArrowTypeId.Int64 => typeof(long),
        Apache.Arrow.Types.ArrowTypeId.Double => typeof(double),
        Apache.Arrow.Types.ArrowTypeId.Decimal128 => typeof(decimal),
        Apache.Arrow.Types.ArrowTypeId.String => typeof(string),
        Apache.Arrow.Types.ArrowTypeId.Boolean => typeof(bool),
        Apache.Arrow.Types.ArrowTypeId.Date32 => typeof(DateTime),
        Apache.Arrow.Types.ArrowTypeId.Timestamp => typeof(DateTime),
        _ => throw new NotSupportedException($"unsupported Arrow column type {batch.Schema.FieldsList[ordinal].DataType}"),
    };
    public override bool HasRows => batch.Length > 0;
    public override bool IsClosed => false;
    public override int RecordsAffected => -1;
    public override int Depth => 0;
    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));
    public override bool NextResult() => false;
    public override int GetValues(object[] values)
    {
        var n = Math.Min(values.Length, FieldCount);
        for (var i = 0; i < n; i++) values[i] = GetValue(i);
        return n;
    }

    public override bool GetBoolean(int o) => (bool)GetValue(o);
    public override byte GetByte(int o) => (byte)GetValue(o);
    public override char GetChar(int o) => throw new NotSupportedException();
    public override DateTime GetDateTime(int o) => (DateTime)GetValue(o);
    public override decimal GetDecimal(int o) => (decimal)GetValue(o);
    public override double GetDouble(int o) => (double)GetValue(o);
    public override float GetFloat(int o) => (float)GetValue(o);
    public override Guid GetGuid(int o) => (Guid)GetValue(o);
    public override short GetInt16(int o) => (short)GetValue(o);
    public override int GetInt32(int o) => (int)GetValue(o);
    public override long GetInt64(int o) => (long)GetValue(o);
    public override string GetString(int o) => (string)GetValue(o);
    public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;
    public override long GetBytes(int o, long d, byte[]? b, int i, int l) => throw new NotSupportedException();
    public override long GetChars(int o, long d, char[]? b, int i, int l) => throw new NotSupportedException();
    public override IEnumerator GetEnumerator() => throw new NotSupportedException();

    public override System.IO.TextReader GetTextReader(int ordinal) =>
        new System.IO.StringReader(GetString(ordinal));

    private decimal ReadDecimal(Decimal128Array a, int ordinal)
    {
        try
        {
            return a.GetValue(_row)!.Value;
        }
        catch (OverflowException ex)
        {
            throw new PzConnectorException(
                $"column '{batch.Schema.FieldsList[ordinal].Name}': decimal value exceeds .NET decimal range " +
                "during bulk write -- reduce the column's precision in the pipeline (e.g. round/cast) before sinking",
                isTransient: false, innerException: ex);
        }
    }
}
