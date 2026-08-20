using System.Collections.ObjectModel;
using System.Data.Common;

namespace Pz.TestSupport;

/// <summary>Minimal DbDataReader over in-memory columns, exposing DataTypeName column schema the way
/// SqlDataReader does (via IDbColumnSchemaGenerator). Typed getters unbox the stored values, which is
/// exactly the shape SqlServerArrowReader's per-kind appenders consume.</summary>
public sealed class FakeDbDataReader(
    IReadOnlyList<(string Name, string DataTypeName, Type ClrType)> columns,
    IReadOnlyList<object?[]> rows) : DbDataReader, IDbColumnSchemaGenerator
{
    private int _row = -1;

    private sealed class FakeColumn : DbColumn
    {
        public FakeColumn(string name, string dataTypeName, Type clrType)
        {
            ColumnName = name;
            DataTypeName = dataTypeName;
            DataType = clrType;
            AllowDBNull = true;
        }
    }

    public ReadOnlyCollection<DbColumn> GetColumnSchema() =>
        new([.. columns.Select(c => (DbColumn)new FakeColumn(c.Name, c.DataTypeName, c.ClrType))]);

    public override int FieldCount => columns.Count;
    public override bool Read() => ++_row < rows.Count;
    public override Task<bool> ReadAsync(CancellationToken ct) => Task.FromResult(Read());
    public override bool IsDBNull(int ordinal) => rows[_row][ordinal] is null;
    public override object GetValue(int ordinal) => rows[_row][ordinal]!;
    public override T GetFieldValue<T>(int ordinal) => (T)rows[_row][ordinal]!;
    public override int GetInt32(int ordinal) => (int)rows[_row][ordinal]!;
    public override long GetInt64(int ordinal) => (long)rows[_row][ordinal]!;
    public override short GetInt16(int ordinal) => (short)rows[_row][ordinal]!;
    public override byte GetByte(int ordinal) => (byte)rows[_row][ordinal]!;
    public override double GetDouble(int ordinal) => (double)rows[_row][ordinal]!;
    public override float GetFloat(int ordinal) => (float)rows[_row][ordinal]!;
    public override decimal GetDecimal(int ordinal) => (decimal)rows[_row][ordinal]!;
    public override string GetString(int ordinal) => (string)rows[_row][ordinal]!;
    public override bool GetBoolean(int ordinal) => (bool)rows[_row][ordinal]!;
    public override Guid GetGuid(int ordinal) => (Guid)rows[_row][ordinal]!;
    public override DateTime GetDateTime(int ordinal) => (DateTime)rows[_row][ordinal]!;
    public override string GetName(int ordinal) => columns[ordinal].Name;
    public override Type GetFieldType(int ordinal) => columns[ordinal].ClrType;
    public override string GetDataTypeName(int ordinal) => columns[ordinal].DataTypeName;
    public override bool HasRows => rows.Count > 0;
    public override bool IsClosed => false;
    public override int RecordsAffected => -1;
    public override int Depth => 0;
    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => throw new NotSupportedException();
    public override int GetOrdinal(string name) =>
        columns.Select((c, i) => (c.Name, i)).First(x => x.Name == name).i;
    public override int GetValues(object[] values) => throw new NotSupportedException();
    public override long GetBytes(int o, long d, byte[]? b, int i, int l) => throw new NotSupportedException();
    public override long GetChars(int o, long d, char[]? b, int i, int l) => throw new NotSupportedException();
    public override char GetChar(int ordinal) => throw new NotSupportedException();
    public override System.Collections.IEnumerator GetEnumerator() => throw new NotSupportedException();
    public override bool NextResult() => false;
}
