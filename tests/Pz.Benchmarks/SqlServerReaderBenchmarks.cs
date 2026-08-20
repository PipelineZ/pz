using System.Collections.ObjectModel;
using System.Data.Common;
using BenchmarkDotNet.Attributes;
using Pz.Connector.SqlServer;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Benchmarks;

/// <summary>The provider-typed SqlServer reader (SqlServerArrowReader, keyed on TDS type names) vs
/// the shared CLR-typed reader (DataReaderSource) over identical in-memory rows: isolates reader
/// overhead from network/parse. BOTH paths use typed getters and compiled per-column appenders, so
/// this benchmark guards that the two stay at parity rather than documenting a gap. The baseline's
/// column set is restricted to CLR shapes DataReaderSource supports (no
/// widened types -- that path cannot read them at all, which is itself part of the comparison story).
///
/// Storage-fidelity note: rows are backed by <see cref="TypedStorageDataReader"/>, not a pre-boxed
/// object?[][] fixture. A real SqlDataReader keeps values in typed TDS buffers -- its typed getters
/// (GetInt64, GetDecimal, ...) return an unboxed value straight from those buffers, while GetValue
/// allocates a fresh box every call. TypedStorageDataReader models the typed-buffer storage honestly,
/// which is what makes a boxed baseline pay its boxing cost here the same way it would in
/// production -- and what would expose any future regression back onto an untyped getter path.</summary>
[MemoryDiagnoser]
public class SqlServerReaderBenchmarks
{
    [Params(100_000)]
    public int Rows { get; set; }

    private long[] _ids = null!;
    private string[] _names = null!;
    private double[] _amounts = null!;
    private decimal[] _prices = null!;
    private bool[] _flags = null!;

    [GlobalSetup]
    public void Setup()
    {
        _ids = new long[Rows];
        _names = new string[Rows];
        _amounts = new double[Rows];
        _prices = new decimal[Rows];
        _flags = new bool[Rows];
        for (var i = 0; i < Rows; i++)
        {
            _ids[i] = i;
            _names[i] = $"row-{i}";
            _amounts[i] = i * 1.5d;
            _prices[i] = i + 0.123456789m;
            _flags[i] = i % 2 == 0;
        }
    }

    [Benchmark(Baseline = true)]
    public async Task<long> RowPivotBaseline()
    {
        var total = 0L;
        await foreach (var batch in DataReaderSource.ReadBatchesAsync(NewReader(), BatchOptions.Default.TargetBatchBytes))
        {
            total += batch.Length;
            batch.Dispose();
        }

        return total;
    }

    [Benchmark]
    public async Task<long> TypedReader()
    {
        var total = 0L;
        await foreach (var batch in SqlServerArrowReader.ReadBatchesAsync(NewReader(), BatchOptions.Default))
        {
            total += batch.Length;
            batch.Dispose();
        }

        return total;
    }

    private DbDataReader NewReader() => new TypedStorageDataReader(_ids, _names, _amounts, _prices, _flags);

    /// <summary>Benchmark-only DbDataReader modeling SqlDataReader's typed-buffer storage: each column is
    /// a parallel typed array (long[], string[], double[], decimal[], bool[]), matching the 5-column
    /// shape (bigint, nvarchar, float, decimal, bit) the benchmark exercises. Typed getters (GetInt64,
    /// GetString, ...) read directly from the typed array -- zero boxing, exactly like SqlDataReader's
    /// typed accessors over TDS buffers. GetValue boxes a fresh object per call via a column-kind switch,
    /// exactly what SqlDataReader does for value types on the untyped path. No nulls are generated, so
    /// IsDBNull is unconditionally false. This is intentionally not a copy of Pz.TestSupport's
    /// FakeDbDataReader (which pre-boxes every cell into object?[][] and so cannot reproduce a boxing
    /// cost) -- it exists only to give this benchmark an honest storage model.</summary>
    private sealed class TypedStorageDataReader(
        long[] ids, string[] names, double[] amounts, decimal[] prices, bool[] flags)
        : DbDataReader, IDbColumnSchemaGenerator
    {
        private static readonly (string Name, string DataTypeName, Type ClrType)[] Columns =
        [
            ("id", "bigint", typeof(long)), ("name", "nvarchar", typeof(string)),
            ("amount", "float", typeof(double)), ("price", "decimal", typeof(decimal)),
            ("flag", "bit", typeof(bool)),
        ];

        private int _row = -1;

        private sealed class TypedColumn : DbColumn
        {
            public TypedColumn(string name, string dataTypeName, Type clrType)
            {
                ColumnName = name;
                DataTypeName = dataTypeName;
                DataType = clrType;
                AllowDBNull = true;
            }
        }

        public ReadOnlyCollection<DbColumn> GetColumnSchema() =>
            new([.. Columns.Select(c => (DbColumn)new TypedColumn(c.Name, c.DataTypeName, c.ClrType))]);

        public override int FieldCount => Columns.Length;
        public override bool Read() => ++_row < ids.Length;
        public override Task<bool> ReadAsync(CancellationToken ct) => Task.FromResult(Read());
        public override bool IsDBNull(int ordinal) => false;

        public override object GetValue(int ordinal) => ordinal switch
        {
            0 => ids[_row],
            1 => names[_row],
            2 => amounts[_row],
            3 => prices[_row],
            4 => flags[_row],
            _ => throw new ArgumentOutOfRangeException(nameof(ordinal)),
        };

        public override T GetFieldValue<T>(int ordinal) => (T)GetValue(ordinal);
        public override long GetInt64(int ordinal) => ids[_row];
        public override string GetString(int ordinal) => names[_row];
        public override double GetDouble(int ordinal) => amounts[_row];
        public override decimal GetDecimal(int ordinal) => prices[_row];
        public override bool GetBoolean(int ordinal) => flags[_row];
        public override string GetName(int ordinal) => Columns[ordinal].Name;
        public override Type GetFieldType(int ordinal) => Columns[ordinal].ClrType;
        public override string GetDataTypeName(int ordinal) => Columns[ordinal].DataTypeName;
        public override bool HasRows => ids.Length > 0;
        public override bool IsClosed => false;
        public override int RecordsAffected => -1;
        public override int Depth => 0;
        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => throw new NotSupportedException();
        public override int GetOrdinal(string name) =>
            Columns.Select((c, i) => (c.Name, i)).First(x => x.Name == name).i;
        public override int GetValues(object[] values) => throw new NotSupportedException();
        public override int GetInt32(int ordinal) => throw new NotSupportedException();
        public override short GetInt16(int ordinal) => throw new NotSupportedException();
        public override byte GetByte(int ordinal) => throw new NotSupportedException();
        public override float GetFloat(int ordinal) => throw new NotSupportedException();
        public override Guid GetGuid(int ordinal) => throw new NotSupportedException();
        public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
        public override long GetBytes(int o, long d, byte[]? b, int i, int l) => throw new NotSupportedException();
        public override long GetChars(int o, long d, char[]? b, int i, int l) => throw new NotSupportedException();
        public override char GetChar(int ordinal) => throw new NotSupportedException();
        public override System.Collections.IEnumerator GetEnumerator() => throw new NotSupportedException();
        public override bool NextResult() => false;
    }
}
