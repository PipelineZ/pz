using System.Collections.ObjectModel;
using System.Data.Common;

namespace Pz.TestSupport;

/// <summary>Minimal DbDataReader over in-memory columns, exposing DataTypeName column schema the way
/// SqlDataReader does (via IDbColumnSchemaGenerator). Typed getters unbox the stored values, which is
/// exactly the shape SqlServerArrowReader's per-kind appenders consume.</summary>
public sealed class FakeDbDataReader : DbDataReader, IDbColumnSchemaGenerator
{
    private readonly IReadOnlyList<(string Name, string DataTypeName, Type ClrType, int? Precision, int? Scale)> _columns;
    private readonly IReadOnlyList<object?[]> _rows;
    private int _row = -1;

    public FakeDbDataReader(
        IReadOnlyList<(string Name, string DataTypeName, Type ClrType)> columns,
        IReadOnlyList<object?[]> rows)
        : this([.. columns.Select(c => (c.Name, c.DataTypeName, c.ClrType, (int?)null, (int?)null))], rows)
    {
    }

    /// <summary>Overload for callers that need DbColumn's NumericPrecision/NumericScale populated
    /// (e.g. driving a reader's precision/scale-dependent type resolution end-to-end) — the 3-tuple
    /// overload above leaves both null, matching drivers whose column schema doesn't report them.</summary>
    public FakeDbDataReader(
        IReadOnlyList<(string Name, string DataTypeName, Type ClrType, int? Precision, int? Scale)> columns,
        IReadOnlyList<object?[]> rows)
    {
        _columns = columns;
        _rows = rows;
    }

    private sealed class FakeColumn : DbColumn
    {
        public FakeColumn(string name, string dataTypeName, Type clrType, int? precision, int? scale)
        {
            ColumnName = name;
            DataTypeName = dataTypeName;
            DataType = clrType;
            AllowDBNull = true;
            NumericPrecision = precision;
            NumericScale = scale;
        }
    }

    public ReadOnlyCollection<DbColumn> GetColumnSchema() =>
        new([.. _columns.Select(c => (DbColumn)new FakeColumn(c.Name, c.DataTypeName, c.ClrType, c.Precision, c.Scale))]);

    public override int FieldCount => _columns.Count;
    public override bool Read() => ++_row < _rows.Count;
    public override Task<bool> ReadAsync(CancellationToken ct) => Task.FromResult(Read());
    public override bool IsDBNull(int ordinal) => _rows[_row][ordinal] is null;
    public override object GetValue(int ordinal) => _rows[_row][ordinal]!;
    public override T GetFieldValue<T>(int ordinal) => (T)_rows[_row][ordinal]!;
    public override int GetInt32(int ordinal) => (int)_rows[_row][ordinal]!;
    public override long GetInt64(int ordinal) => (long)_rows[_row][ordinal]!;
    public override short GetInt16(int ordinal) => (short)_rows[_row][ordinal]!;
    public override byte GetByte(int ordinal) => (byte)_rows[_row][ordinal]!;
    public override double GetDouble(int ordinal) => (double)_rows[_row][ordinal]!;
    public override float GetFloat(int ordinal) => (float)_rows[_row][ordinal]!;
    public override decimal GetDecimal(int ordinal) => (decimal)_rows[_row][ordinal]!;
    public override string GetString(int ordinal) => (string)_rows[_row][ordinal]!;
    public override bool GetBoolean(int ordinal) => (bool)_rows[_row][ordinal]!;
    public override Guid GetGuid(int ordinal) => (Guid)_rows[_row][ordinal]!;
    public override DateTime GetDateTime(int ordinal) => (DateTime)_rows[_row][ordinal]!;
    public override string GetName(int ordinal) => _columns[ordinal].Name;
    public override Type GetFieldType(int ordinal) => _columns[ordinal].ClrType;
    public override string GetDataTypeName(int ordinal) => _columns[ordinal].DataTypeName;
    public override bool HasRows => _rows.Count > 0;
    public override bool IsClosed => false;
    public override int RecordsAffected => -1;
    public override int Depth => 0;
    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => throw new NotSupportedException();
    public override int GetOrdinal(string name) =>
        _columns.Select((c, i) => (c.Name, i)).First(x => x.Name == name).i;
    public override int GetValues(object[] values) => throw new NotSupportedException();
    public override long GetBytes(int o, long d, byte[]? b, int i, int l) => throw new NotSupportedException();
    public override long GetChars(int o, long d, char[]? b, int i, int l) => throw new NotSupportedException();
    public override char GetChar(int ordinal) => throw new NotSupportedException();
    public override System.Collections.IEnumerator GetEnumerator() => throw new NotSupportedException();
    public override bool NextResult() => false;
}
