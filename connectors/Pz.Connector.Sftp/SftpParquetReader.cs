using System.Runtime.CompilerServices;
using Apache.Arrow;
using Parquet;
using Parquet.Schema;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connector.Sftp;

/// <summary>Parquet → Arrow row reading over a seekable stream (SSH.NET's SftpFileStream is
/// seekable, which is what makes footer-first parquet reading possible remotely). Parquet.Net 6.0.3
/// has no single `DataColumn`/`ReadColumnAsync(field)` entry point on <see cref="ParquetRowGroupReader"/>
/// — reading is per-CLR-type generic (<c>ReadAsync&lt;T&gt;(field, Memory&lt;T?&gt;)</c> for value
/// types, a dedicated overload for strings) — so one column read per field per row group fills a
/// boxed, row-indexable buffer, then one row-major pass feeds
/// <see cref="ArrowBatchBuilder.AppendRow"/> — boxed per cell, but acceptable on this tier where the
/// network transfer dominates. Schema mapping mirrors AzureParquetReader/LocalFiles ParquetSource
/// (parquet field → v0 type name → Arrow field); decimal is refused like the LocalFiles universal
/// parquet WRITE path (<c>ParquetSinkWriteSession.BuildDataField</c>) refuses it — this reader runs
/// row reads in managed code, unlike Azure/LocalFiles' footer-only readers, so there is no fallback
/// once a decimal column is actually asked for.
///
/// <paramref name="projectedColumns"/>'s order (see <see cref="ReadAsync"/>), when given, is
/// authoritative over the footer's own physical column order: the engine binds a landed batch's
/// columns to <c>SftpSource.GetSchemaAsync</c>'s reported schema BY POSITION, so a caller that
/// passes a declared contract's column names MUST get them back in that same order regardless of
/// where those columns physically sit in the file, or values silently land under the wrong
/// column.</summary>
internal static class SftpParquetReader
{
    public static async Task<Schema> ReadSchemaAsync(Stream stream, string context, CancellationToken ct)
    {
        await using var reader = await ParquetReader.CreateAsync(stream, leaveStreamOpen: true, cancellationToken: ct)
            .ConfigureAwait(false);
        return BuildArrowSchema(reader.Schema.GetDataFields(), context);
    }

    public static async IAsyncEnumerable<RecordBatch> ReadAsync(
        Stream stream, IReadOnlyList<string>? projectedColumns, string context, BatchOptions options,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await using var reader = await ParquetReader.CreateAsync(stream, leaveStreamOpen: true, cancellationToken: ct)
            .ConfigureAwait(false);
        var fields = SelectFields(reader.Schema.GetDataFields(), projectedColumns, context);
        var schema = BuildArrowSchema(fields, context);
        var builder = new ArrowBatchBuilder(schema, options.TargetBatchBytes, maxRowsPerBatch: options.MaxRowsPerBatch);

        for (var g = 0; g < reader.RowGroupCount; g++)
        {
            using var group = reader.OpenRowGroupReader(g);
            var rows = checked((int)group.RowCount);
            var columns = new object?[fields.Length][];
            for (var i = 0; i < fields.Length; i++)
            {
                columns[i] = await ReadColumnAsync(group, fields[i], rows, context, ct).ConfigureAwait(false);
            }

            var row = new object?[fields.Length];
            for (var r = 0; r < rows; r++)
            {
                for (var i = 0; i < fields.Length; i++)
                {
                    row[i] = columns[i][r];
                }

                builder.AppendRow(row);
                if (builder.TryTakeBatch(out var batch))
                {
                    yield return batch!;
                }
            }
        }

        if (builder.Flush() is { } tail)
        {
            yield return tail;
        }
    }

    /// <summary>Reads one row group's worth of one column into a boxed, row-indexable array already
    /// converted to the CLR shape <see cref="ArrowBatchBuilder.AppendRow"/> expects: <see
    /// cref="DateOnly"/> for date fields, a UTC <see cref="DateTimeOffset"/> for timestamp fields,
    /// everything else passed through as-is. A non-null <c>T?</c> cell boxes as its underlying
    /// <c>T</c> (not as <c>Nullable&lt;T&gt;</c>) under C#'s own boxing rule, and a null cell boxes
    /// as an actual null reference — which is exactly what <see cref="ArrowBatchBuilder"/>'s
    /// per-column appenders expect, so <see cref="System.Array.ConvertAll{TInput,TOutput}"/> needs no
    /// per-cell type dispatch of its own.</summary>
    private static async Task<object?[]> ReadColumnAsync(
        ParquetRowGroupReader group, DataField field, int rows, string context, CancellationToken ct)
    {
        if (field is DateTimeDataField dateTimeField)
        {
            var buffer = new DateTime?[rows];
            await group.ReadAsync<DateTime>(field, buffer, cancellationToken: ct).ConfigureAwait(false);
            var converted = new object?[rows];
            for (var i = 0; i < rows; i++)
            {
                converted[i] = buffer[i] is { } dt
                    ? dateTimeField.DateTimeFormat == DateTimeFormat.Date
                        ? DateOnly.FromDateTime(dt)
                        : new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
                    : null;
            }

            return converted;
        }

        if (field.ClrType == typeof(int))
        {
            var buffer = new int?[rows];
            await group.ReadAsync<int>(field, buffer, cancellationToken: ct).ConfigureAwait(false);
            return System.Array.ConvertAll(buffer, v => (object?)v);
        }

        if (field.ClrType == typeof(long))
        {
            var buffer = new long?[rows];
            await group.ReadAsync<long>(field, buffer, cancellationToken: ct).ConfigureAwait(false);
            return System.Array.ConvertAll(buffer, v => (object?)v);
        }

        if (field.ClrType == typeof(double))
        {
            var buffer = new double?[rows];
            await group.ReadAsync<double>(field, buffer, cancellationToken: ct).ConfigureAwait(false);
            return System.Array.ConvertAll(buffer, v => (object?)v);
        }

        if (field.ClrType == typeof(bool))
        {
            var buffer = new bool?[rows];
            await group.ReadAsync<bool>(field, buffer, cancellationToken: ct).ConfigureAwait(false);
            return System.Array.ConvertAll(buffer, v => (object?)v);
        }

        if (field.ClrType == typeof(string))
        {
            var buffer = new string?[rows];
            await group.ReadAsync(field, buffer, cancellationToken: ct).ConfigureAwait(false);
            return buffer;
        }

        // Unreachable in practice: ToV0TypeName runs over every field while building the schema, before
        // any column is ever read, and already rejects every CLR type not handled above.
        throw new NotSupportedException(
            $"{context}: parquet column '{field.Name}' has unsupported type '{field.ClrType.Name}' for the sftp read path");
    }

    /// <summary>Which footer fields to read, and in what order. A null projection reads every field in
    /// the footer's own physical order — the contract-less case, where that footer order literally IS
    /// the schema <c>SftpSource.GetSchemaAsync</c> reported, so there is no order to preserve beyond it.
    /// A non-null projection is honored POSITIONALLY — fields come back in <paramref
    /// name="projectedColumns"/>'s own order, never the footer's — because the only caller that ever
    /// passes one is projecting against a declared contract, whose order IS what GetSchemaAsync
    /// reported; reverting to footer order here would silently land a reordered contract's values under
    /// the wrong column names.</summary>
    private static DataField[] SelectFields(
        IReadOnlyList<DataField> footerFields, IReadOnlyList<string>? projectedColumns, string context)
    {
        DataField[] fields;
        if (projectedColumns is null)
        {
            fields = footerFields.ToArray();
        }
        else
        {
            var byName = new Dictionary<string, DataField>(StringComparer.OrdinalIgnoreCase);
            foreach (var field in footerFields)
            {
                byName.TryAdd(field.Name, field);
            }

            var ordered = new List<DataField>(projectedColumns.Count);
            foreach (var name in projectedColumns)
            {
                if (byName.TryGetValue(name, out var field))
                {
                    ordered.Add(field);
                }
            }

            fields = ordered.ToArray();
        }

        if (fields.Length == 0)
        {
            throw new PzConnectorException(
                $"{context}: none of the requested columns exist in the parquet file", isTransient: false);
        }

        return fields;
    }

    private static Schema BuildArrowSchema(IReadOnlyList<DataField> fields, string context) =>
        new(fields.Select(f => SftpTypeNameMap.ToArrowField(f.Name, ToV0TypeName(f, context))).ToArray(), null);

    /// <summary>Mirrors <c>AzureParquetTypeMap.ToV0TypeName</c>/LocalFiles' <c>ParquetTypeMap</c> field
    /// by field, except decimal: those two only ever build a schema (row reading for parquet runs on
    /// the native tier for both), so they map decimal straight through as a v0 type name. This reader
    /// executes actual row reads in managed code and decimal is out of the v0 matrix here (parity with
    /// <c>ParquetSinkWriteSession.BuildDataField</c>'s decimal refusal on the write side), so it fails
    /// fast naming the column instead of reaching a read that was never going to work.</summary>
    private static string ToV0TypeName(DataField field, string context)
    {
        if (field is DecimalDataField)
        {
            throw new PzConnectorException(
                $"{context}: parquet column '{field.Name}' is decimal-typed, which the sftp universal " +
                "read path does not support — regenerate the file with double/varchar, or convert upstream",
                isTransient: false);
        }

        if (field is DateTimeDataField { DateTimeFormat: DateTimeFormat.Date })
        {
            return "date";
        }

        if (field is DateTimeDataField)
        {
            return "timestamp"; // DateAndTime, DateAndTimeMicros, Timestamp, Impala variants
        }

        var clrType = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;
        if (clrType == typeof(int)) return "int";
        if (clrType == typeof(long)) return "bigint";
        if (clrType == typeof(double)) return "double";
        if (clrType == typeof(string)) return "varchar";
        if (clrType == typeof(bool)) return "boolean";

        throw new PzConnectorException(
            $"{context}: parquet column '{field.Name}' has unsupported type '{clrType.Name}' for the sftp " +
            "read path", isTransient: false);
    }
}
