using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Text;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Memory;

namespace Pz.Connector.SqlServer;

/// <summary>Typed DbDataReader → Arrow streaming: per-column compiled appenders bind the
/// reader's typed getters directly to Arrow builders — no object[] row, no boxing. Buffers come from
/// PooledNativeAllocator.Shared — pooled off-heap native memory, so steady-state ingest never puts a
/// batch on the LOH; builders are reused across batches via Clear(). Columns
/// are appended in ascending ordinal order every row, which is what SequentialAccess requires.
/// Byte accounting matches the engine's conventions: fixed widths, utf8 = 4 + UTF-8 bytes, +1
/// validity bit (0.125) per value.</summary>
internal static class SqlServerArrowReader
{
    public static Schema BuildSchema(DbDataReader reader, string subject)
    {
        var fields = new List<Field>(reader.FieldCount);
        foreach (var column in reader.GetColumnSchema())
        {
            var typeName = column.DataTypeName ?? "<unknown>";
            if (!MsTypeMap.TryResolve(typeName, out var ms))
            {
                throw new PzConnectorException(
                    $"'{subject}': column '{column.ColumnName}' has SQL Server type '{typeName}', which is " +
                    "outside the supported matrix -- hint: cast it in query: (e.g. cast(col as nvarchar(max))) " +
                    "or exclude it via columns:",
                    isTransient: false);
            }

            fields.Add(new Field(column.ColumnName, ms!.ArrowType, nullable: true));
        }

        return new Schema(fields, null);
    }

    public static async IAsyncEnumerable<RecordBatch> ReadBatchesAsync(
        DbDataReader reader, BatchOptions options, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var schema = BuildSchema(reader, "read");
        var plans = BuildColumnPlans(reader, schema);
        var pendingRows = 0;
        var bytesEstimate = 0d;

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            for (var i = 0; i < plans.Length; i++)
            {
                bytesEstimate += plans[i].Append() + 0.125d;
            }

            pendingRows++;
            if (bytesEstimate >= options.TargetBatchBytes || pendingRows >= options.MaxRowsPerBatch)
            {
                ct.ThrowIfCancellationRequested();
                yield return Build(schema, plans, pendingRows);
                pendingRows = 0;
                bytesEstimate = 0;
            }
        }

        if (pendingRows > 0)
        {
            ct.ThrowIfCancellationRequested();
            yield return Build(schema, plans, pendingRows);
        }
    }

    private static RecordBatch Build(Schema schema, ColumnPlan[] plans, int rows)
    {
        var arrays = new IArrowArray[plans.Length];
        for (var i = 0; i < plans.Length; i++)
        {
            arrays[i] = plans[i].BuildAndReset();
        }

        return new RecordBatch(schema, arrays, rows);
    }

    /// <summary>One column's hot-path pair: Append() reads the current row's value (typed getter →
    /// typed builder append; null check first) and returns the value's estimated byte width;
    /// BuildAndReset() finishes the Arrow array from pooled memory and clears the builder for reuse.</summary>
    private sealed record ColumnPlan(Func<double> Append, Func<IArrowArray> BuildAndReset);

    private static ColumnPlan[] BuildColumnPlans(DbDataReader reader, Schema schema)
    {
        var allocator = PooledNativeAllocator.Shared;
        var columns = reader.GetColumnSchema();
        var plans = new ColumnPlan[columns.Count];
        for (var i = 0; i < columns.Count; i++)
        {
            MsTypeMap.TryResolve(columns[i].DataTypeName ?? "", out var ms); // BuildSchema already validated
            var ordinal = i;
            var name = columns[i].ColumnName;
            plans[i] = ms!.Kind switch
            {
                MsColumnKind.Int32 => Plan<Int32Array, Int32Array.Builder>(new Int32Array.Builder(), allocator,
                    b => b.Append(reader.GetInt32(ordinal)), _ => 4d, reader, ordinal),
                MsColumnKind.Int32FromByte => Plan<Int32Array, Int32Array.Builder>(new Int32Array.Builder(), allocator,
                    b => b.Append(reader.GetByte(ordinal)), _ => 4d, reader, ordinal),
                MsColumnKind.Int32FromInt16 => Plan<Int32Array, Int32Array.Builder>(new Int32Array.Builder(), allocator,
                    b => b.Append(reader.GetInt16(ordinal)), _ => 4d, reader, ordinal),
                MsColumnKind.Int64 => Plan<Int64Array, Int64Array.Builder>(new Int64Array.Builder(), allocator,
                    b => b.Append(reader.GetInt64(ordinal)), _ => 8d, reader, ordinal),
                MsColumnKind.Double => Plan<DoubleArray, DoubleArray.Builder>(new DoubleArray.Builder(), allocator,
                    b => b.Append(reader.GetDouble(ordinal)), _ => 8d, reader, ordinal),
                MsColumnKind.DoubleFromFloat => Plan<DoubleArray, DoubleArray.Builder>(new DoubleArray.Builder(), allocator,
                    b => b.Append(reader.GetFloat(ordinal)), _ => 8d, reader, ordinal),
                MsColumnKind.Decimal => Plan<Decimal128Array, Decimal128Array.Builder>(
                    new Decimal128Array.Builder((Apache.Arrow.Types.Decimal128Type)schema.FieldsList[i].DataType), allocator,
                    b =>
                    {
                        try
                        {
                            b.Append(reader.GetDecimal(ordinal));
                        }
                        catch (OverflowException ex)
                        {
                            throw new PzConnectorException(
                                $"column '{name}': value exceeds decimal128(38,9) scale -- cast the column " +
                                "in query: (e.g. cast(col as decimal(38,9))) or declare it varchar in columns:",
                                isTransient: false, innerException: ex);
                        }
                    }, _ => 16d, reader, ordinal),
                MsColumnKind.Utf8 => PlanUtf8(allocator, reader, ordinal, r => r.GetString(ordinal)),
                MsColumnKind.Utf8FromGuid => PlanUtf8(allocator, reader, ordinal, r => r.GetGuid(ordinal).ToString("D")),
                MsColumnKind.Bool => Plan<BooleanArray, BooleanArray.Builder>(new BooleanArray.Builder(), allocator,
                    b => b.Append(reader.GetBoolean(ordinal)), _ => 0.125d, reader, ordinal),
                MsColumnKind.Date => Plan<Date32Array, Date32Array.Builder>(new Date32Array.Builder(), allocator,
                    b => b.Append(reader.GetFieldValue<DateOnly>(ordinal)), _ => 4d, reader, ordinal),
                MsColumnKind.TimestampFromDateTime => Plan<TimestampArray, TimestampArray.Builder>(
                    new TimestampArray.Builder((Apache.Arrow.Types.TimestampType)schema.FieldsList[i].DataType), allocator,
                    b => b.Append(new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc))),
                    _ => 8d, reader, ordinal),
                MsColumnKind.TimestampFromDateTimeOffset => Plan<TimestampArray, TimestampArray.Builder>(
                    new TimestampArray.Builder((Apache.Arrow.Types.TimestampType)schema.FieldsList[i].DataType), allocator,
                    b => b.Append(reader.GetFieldValue<DateTimeOffset>(ordinal).ToUniversalTime()),
                    _ => 8d, reader, ordinal),
                _ => throw new InvalidOperationException("unreachable"),
            };
        }

        return plans;
    }

    private static ColumnPlan Plan<TArray, TBuilder>(
        TBuilder builder, Apache.Arrow.Memory.MemoryAllocator allocator,
        Action<TBuilder> appendValue, Func<TBuilder, double> width,
        DbDataReader reader, int ordinal)
        where TArray : IArrowArray
        where TBuilder : class, IArrowArrayBuilder<TArray, TBuilder> =>
        new(
            Append: () =>
            {
                if (reader.IsDBNull(ordinal))
                {
                    builder.AppendNull();
                    return 0d;
                }

                appendValue(builder);
                return width(builder);
            },
            BuildAndReset: () =>
            {
                var array = builder.Build(allocator);
                builder.Clear();
                return array;
            });

    private static ColumnPlan PlanUtf8(
        Apache.Arrow.Memory.MemoryAllocator allocator, DbDataReader reader, int ordinal,
        Func<DbDataReader, string> read)
    {
        var builder = new StringArray.Builder();
        return new ColumnPlan(
            Append: () =>
            {
                if (reader.IsDBNull(ordinal))
                {
                    builder.AppendNull();
                    return 0d;
                }

                var value = read(reader);
                builder.Append(value);
                return 4d + Encoding.UTF8.GetByteCount(value);
            },
            BuildAndReset: () =>
            {
                var array = builder.Build(allocator);
                builder.Clear();
                return array;
            });
    }
}
