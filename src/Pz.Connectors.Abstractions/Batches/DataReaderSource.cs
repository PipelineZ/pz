using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Memory;

namespace Pz.Connectors.Abstractions.Batches;

/// <summary>Adapts any ADO.NET DbDataReader into the universal Arrow batch stream — the reusable
/// row→columnar pivot for ADO.NET-backed connectors. Schema derives from the reader's CLR field
/// types (v0 matrix); an unmapped column type throws a permanent <see cref="PzConnectorException"/>
/// naming the column and type. DBNull → null for every column.
///
/// The hot loop is per-column compiled appenders binding the reader's TYPED getters (GetInt64,
/// GetDecimal, ...) directly to Arrow builders — the same no-boxing shape as
/// <c>SqlServerArrowReader</c>, keyed on CLR field types instead of provider type names. The
/// apparently simpler route, boxing every cell (<c>GetValue</c> into an <c>object?[]</c> row, then
/// <c>ArrowBatchBuilder.AppendRow</c>), makes that boxing the dominant ingest cost against a live
/// provider reader. Buffers come from <see cref="PooledNativeAllocator.Shared"/> (pooled, off-heap);
/// builders are reused across batches via <c>Clear()</c>. Byte accounting matches the engine's
/// conventions: fixed widths, utf8 = 4 + UTF-8 bytes, +1 validity bit (0.125) per value; a batch is
/// emitted at the byte target or <see cref="BatchOptions.Default"/>'s max rows per batch.
///
/// Contract: a <c>decimal</c> value whose scale
/// exceeds the mapped <c>decimal128(38,9)</c> column's 9 fractional digits surfaces as a permanent
/// (<c>isTransient: false</c>) <see cref="PzConnectorException"/> naming the offending column and the
/// remedy, consistent with the unmapped-column-type failure mode above.
///
/// Cancellation: <see cref="ReadBatchesAsync"/> honors <c>ct</c> at two points -- (1) every reader call
/// (<c>reader.ReadAsync(ct)</c>) so a genuine network wait is cancellable, and (2) an explicit
/// <c>ct.ThrowIfCancellationRequested()</c> immediately before every batch <c>yield return</c> (including
/// the final flush). (2) is required because ADO.NET providers commonly buffer/read ahead: when the next
/// row is already available client-side, <c>ReadAsync(ct)</c> can complete without ever consulting the
/// token, letting an entire extra batch's worth of rows be processed after cancellation was requested.
/// Checking at the batch boundary bounds that to the batch already in flight, regardless of provider
/// buffering behavior. Per-row checks are deliberately not added -- batch-boundary + reader-call token is
/// the contract.</summary>
public static class DataReaderSource
{
    public static Schema BuildArrowSchema(DbDataReader reader) => BuildArrowSchema(reader, arrayElement: null);

    /// <summary><see cref="BuildArrowSchema(DbDataReader)"/> for a provider that reports an array column only
    /// as <see cref="System.Array"/> (Npgsql does): <paramref name="arrayElement"/> names the CLR element type of
    /// column <c>i</c> (from the provider's own type name), or returns null when it has none. A
    /// one-dimensional array of a matrix type becomes an Arrow list of that type.</summary>
    public static Schema BuildArrowSchema(DbDataReader reader, Func<int, Type?>? arrayElement)
    {
        var fields = new List<Field>(reader.FieldCount);
        for (var i = 0; i < reader.FieldCount; i++)
        {
            var name = reader.GetName(i);
            var type = ElementType(reader, i, arrayElement) is { } element
                ? new ListType(new Field("item", ToArrowType(element, name), nullable: true))
                : ToArrowType(reader.GetFieldType(i), name);
            fields.Add(new Field(name, type, nullable: true));
        }

        return new Schema(fields, null);
    }

    public static IAsyncEnumerable<RecordBatch> ReadBatchesAsync(
        DbDataReader reader, int targetBatchBytes, CancellationToken ct = default) =>
        ReadBatchesAsync(reader, targetBatchBytes, arrayElement: null, ct);

    /// <summary><see cref="ReadBatchesAsync(DbDataReader, int, CancellationToken)"/> with the array element
    /// resolver of <see cref="BuildArrowSchema(DbDataReader, Func{int, Type?})"/>.</summary>
    public static async IAsyncEnumerable<RecordBatch> ReadBatchesAsync(
        DbDataReader reader, int targetBatchBytes, Func<int, Type?>? arrayElement,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var schema = BuildArrowSchema(reader, arrayElement);
        var plans = BuildColumnPlans(reader, schema, arrayElement);
        var maxRowsPerBatch = BatchOptions.Default.MaxRowsPerBatch;
        var pendingRows = 0;
        var bytesEstimate = 0d;

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            for (var i = 0; i < plans.Length; i++)
            {
                bytesEstimate += plans[i].Append() + 0.125d;
            }

            pendingRows++;
            if (bytesEstimate >= targetBatchBytes || pendingRows >= maxRowsPerBatch)
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

    private static ColumnPlan[] BuildColumnPlans(DbDataReader reader, Schema schema, Func<int, Type?>? arrayElement)
    {
        var allocator = PooledNativeAllocator.Shared;
        var plans = new ColumnPlan[schema.FieldsList.Count];
        for (var i = 0; i < plans.Length; i++)
        {
            var clr = reader.GetFieldType(i); // BuildArrowSchema already validated the matrix
            var ordinal = i;
            var name = schema.FieldsList[i].Name;
            if (schema.FieldsList[i].DataType is ListType)
            {
                plans[i] = PlanArray(reader, ordinal, schema.FieldsList[i], ElementType(reader, i, arrayElement)!, allocator);
                continue;
            }

            plans[i] = clr switch
            {
                _ when clr == typeof(int) => Plan<Int32Array, Int32Array.Builder>(new Int32Array.Builder(), allocator,
                    b => b.Append(reader.GetInt32(ordinal)), _ => 4d, reader, ordinal),
                _ when clr == typeof(long) => Plan<Int64Array, Int64Array.Builder>(new Int64Array.Builder(), allocator,
                    b => b.Append(reader.GetInt64(ordinal)), _ => 8d, reader, ordinal),
                _ when clr == typeof(double) => Plan<DoubleArray, DoubleArray.Builder>(new DoubleArray.Builder(), allocator,
                    b => b.Append(reader.GetDouble(ordinal)), _ => 8d, reader, ordinal),
                _ when clr == typeof(decimal) => Plan<Decimal128Array, Decimal128Array.Builder>(
                    new Decimal128Array.Builder((Decimal128Type)schema.FieldsList[i].DataType), allocator,
                    b =>
                    {
                        try
                        {
                            b.Append(reader.GetDecimal(ordinal));
                        }
                        catch (OverflowException ex)
                        {
                            throw new PzConnectorException(
                                $"column '{name}': value exceeds decimal128(38,9) scale -- cast the column in " +
                                "query: (e.g. ::numeric(38,9) or ::text) or declare it varchar in columns:",
                                isTransient: false, innerException: ex);
                        }
                    }, _ => 16d, reader, ordinal),
                _ when clr == typeof(string) => PlanUtf8(allocator, reader, ordinal),
                _ when clr == typeof(bool) => Plan<BooleanArray, BooleanArray.Builder>(new BooleanArray.Builder(), allocator,
                    b => b.Append(reader.GetBoolean(ordinal)), _ => 0.125d, reader, ordinal),
                _ when clr == typeof(DateOnly) => Plan<Date32Array, Date32Array.Builder>(new Date32Array.Builder(), allocator,
                    b => b.Append(reader.GetFieldValue<DateOnly>(ordinal)), _ => 4d, reader, ordinal),
                // DateTime (pg timestamp) is trusted-UTC.
                _ when clr == typeof(DateTime) => Plan<TimestampArray, TimestampArray.Builder>(
                    new TimestampArray.Builder((TimestampType)schema.FieldsList[i].DataType), allocator,
                    b => b.Append(new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc))),
                    _ => 8d, reader, ordinal),
                _ when clr == typeof(DateTimeOffset) => Plan<TimestampArray, TimestampArray.Builder>(
                    new TimestampArray.Builder((TimestampType)schema.FieldsList[i].DataType), allocator,
                    b => b.Append(reader.GetFieldValue<DateTimeOffset>(ordinal)),
                    _ => 8d, reader, ordinal),
                _ => throw new InvalidOperationException(
                    $"unreachable: '{clr.Name}' should already have been rejected by BuildArrowSchema"),
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

    /// <summary>A one-dimensional provider array (a postgres <c>integer[]</c>, <c>text[]</c>…) as an Arrow list.
    /// Read with nullable elements, so a NULL element is kept rather than failing the read.</summary>
    private static ColumnPlan PlanArray(
        DbDataReader reader, int ordinal, Field field, Type element, Apache.Arrow.Memory.MemoryAllocator allocator)
    {
        var column = ArrowBatchBuilder.CreateColumn(field, allocator);
        Func<object> read = element switch
        {
            _ when element == typeof(int) => () => reader.GetFieldValue<int?[]>(ordinal),
            _ when element == typeof(long) => () => reader.GetFieldValue<long?[]>(ordinal),
            _ when element == typeof(double) => () => reader.GetFieldValue<double?[]>(ordinal),
            _ when element == typeof(decimal) => () => reader.GetFieldValue<decimal?[]>(ordinal),
            _ when element == typeof(bool) => () => reader.GetFieldValue<bool?[]>(ordinal),
            _ when element == typeof(string) => () => reader.GetFieldValue<string?[]>(ordinal),
            _ when element == typeof(DateOnly) => () => reader.GetFieldValue<DateOnly?[]>(ordinal),
            // DateTime (pg timestamp) is trusted-UTC, as for a scalar column.
            _ when element == typeof(DateTime) => () => System.Array.ConvertAll(reader.GetFieldValue<DateTime?[]>(ordinal),
                d => d is { } v ? new DateTimeOffset(DateTime.SpecifyKind(v, DateTimeKind.Utc)) : (DateTimeOffset?)null),
            _ when element == typeof(DateTimeOffset) => () => reader.GetFieldValue<DateTimeOffset?[]>(ordinal),
            _ => throw new InvalidOperationException(
                $"unreachable: array of '{element.Name}' should already have been rejected by BuildArrowSchema"),
        };

        return new ColumnPlan(
            Append: () =>
            {
                if (reader.IsDBNull(ordinal))
                {
                    column.Append(null);
                    return 0d;
                }

                var value = read();
                try
                {
                    column.Append(value);
                }
                catch (OverflowException ex)
                {
                    throw new PzConnectorException(
                        $"column '{field.Name}': an element exceeds decimal128(38,9) scale -- cast the column in " +
                        "query: (e.g. ::numeric(38,9)[] or ::text[])",
                        isTransient: false, innerException: ex);
                }

                return column.WidthOf(value);
            },
            BuildAndReset: column.Build);
    }

    /// <summary>Column <paramref name="i"/>'s array element type: from a typed CLR array, or, for a provider that
    /// reports only <see cref="System.Array"/>, from <paramref name="arrayElement"/>; null for a non-array column.</summary>
    private static Type? ElementType(DbDataReader reader, int i, Func<int, Type?>? arrayElement)
    {
        var clr = reader.GetFieldType(i);
        return ArrayElement(clr) ?? (clr == typeof(System.Array) ? arrayElement?.Invoke(i) : null);
    }

    /// <summary>The element type of a one-dimensional provider array (<c>int[]</c>, <c>int?[]</c>), or null.</summary>
    private static Type? ArrayElement(Type clr) =>
        clr.IsArray && clr.GetArrayRank() == 1
            ? Nullable.GetUnderlyingType(clr.GetElementType()!) ?? clr.GetElementType()
            : null;

    private static ColumnPlan PlanUtf8(Apache.Arrow.Memory.MemoryAllocator allocator, DbDataReader reader, int ordinal)
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

                var value = reader.GetString(ordinal);
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

    private static IArrowType ToArrowType(Type clr, string column) => clr switch
    {
        _ when clr == typeof(int) => Int32Type.Default,
        _ when clr == typeof(long) => Int64Type.Default,
        _ when clr == typeof(double) => DoubleType.Default,
        _ when clr == typeof(decimal) => new Decimal128Type(38, 9),
        _ when clr == typeof(string) => StringType.Default,
        _ when clr == typeof(bool) => BooleanType.Default,
        _ when clr == typeof(DateOnly) || clr == typeof(DateTime) => clr == typeof(DateOnly)
            ? Date32Type.Default : new TimestampType(TimeUnit.Microsecond, "+00:00"),
        _ when clr == typeof(DateTimeOffset) => new TimestampType(TimeUnit.Microsecond, "+00:00"),
        _ => throw new PzConnectorException(
            $"column '{column}': provider type '{clr.Name}' is outside the v0 type matrix", isTransient: false),
    };
}
