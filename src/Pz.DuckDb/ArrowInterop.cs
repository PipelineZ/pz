using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Types;
using DuckDB.NET.Data;

namespace Pz.DuckDb;

/// <summary>
/// Path-independent Arrow/DuckDB helpers (type map, SQL building) plus the chosen ingest path:
/// the Arrow C Data Interface. DuckDB.NET's managed surface exposes no ingest-side
/// Arrow members, so this talks to the native `duckdb` library directly via the functions it ships
/// for converting foreign (externally produced) Arrow C Data Interface structures into DuckDB's own
/// data chunks: `duckdb_schema_from_arrow` + `duckdb_data_chunk_from_arrow`, then bulk-appends each
/// resulting chunk with a native Appender via `duckdb_append_data_chunk`. This is a genuine columnar
/// transfer (no per-row marshalling). Egress is the other way round: DuckDB.NET >= 1.5.5 does expose
/// a managed Arrow surface, so <see cref="DuckDBCommand.ExecuteArrowBatchesAsync"/>
/// drives the egress row-pivot path (<see cref="DuckSession.ProduceArrowBatchesAsync"/>) and
/// <see cref="DuckDBCommand.ExecuteArrowStream"/> backs <see cref="NormalizeNativeArrowSchema"/>'s
/// schema-only derivation.
/// </summary>
internal static class ArrowInterop
{
    /// <summary>Arrow v0-matrix type → DuckDB DDL type. Throws NotSupportedException naming the type otherwise.</summary>
    internal static string ToDuckDbType(IArrowType type) => type switch
    {
        Int32Type => "INTEGER",
        Int64Type => "BIGINT",
        DoubleType => "DOUBLE",
        Decimal128Type d => $"DECIMAL({d.Precision},{d.Scale})",
        StringType => "VARCHAR",
        BooleanType => "BOOLEAN",
        Date32Type => "DATE",
        TimestampType { Unit: TimeUnit.Microsecond } => "TIMESTAMP",
        _ => throw new NotSupportedException($"Arrow type '{type}' has no DuckDB mapping in the v0 type matrix."),
    };

    /// <summary>Validates a <see cref="Schema"/> obtained from DuckDB's native Arrow export
    /// (<see cref="DuckDBCommand.ExecuteArrowStream"/>, used by <see cref="DuckSession.GetResultSchemaAsync"/>'s
    /// <see cref="DuckSession.PeekSchema"/>) against the v0 type matrix <see cref="ToDuckDbType"/> maps from --
    /// the same fail-fast contract every schema-derivation path in this codebase enforces -- and normalizes
    /// each TIMESTAMP field's timezone to <c>"+00:00"</c>. DuckDB's native Arrow export reports an empty
    /// timezone string for a plain (timezone-less) TIMESTAMP column, which would otherwise diverge from the
    /// "+00:00" convention every other Arrow schema this codebase produces for that same DuckDB column type
    /// already uses (see <see cref="DuckSession.ProduceArrowBatchesAsync"/>'s egress path, which gets it
    /// straight from DuckDB.NET's own <c>ExecuteArrowBatchesAsync</c>-produced batches and so needs no
    /// normalizing there). Every field is also forced non-nullable-agnostic (<c>nullable: true</c>), since
    /// DuckDB's Arrow export does not reliably carry a source column's NOT NULL constraint through
    /// either. <see cref="DuckSession.PeekSchema"/>'s doc comment records why the native Arrow export is
    /// the schema source at all.</summary>
    internal static Schema NormalizeNativeArrowSchema(Schema nativeSchema)
    {
        var fields = new Field[nativeSchema.FieldsList.Count];
        for (var i = 0; i < fields.Length; i++)
        {
            var field = nativeSchema.FieldsList[i];
            IArrowType normalizedType = field.DataType switch
            {
                Int32Type => field.DataType,
                Int64Type => field.DataType,
                DoubleType => field.DataType,
                Decimal128Type => field.DataType,
                StringType => field.DataType,
                BooleanType => field.DataType,
                Date32Type => field.DataType,
                TimestampType { Unit: TimeUnit.Microsecond } => new TimestampType(TimeUnit.Microsecond, "+00:00"),
                _ => throw new NotSupportedException(
                    $"DuckDB column '{field.Name}' has Arrow type '{field.DataType}', which has no DuckDB mapping in the v0 type matrix."),
            };

            fields[i] = new Field(field.Name, normalizedType, nullable: true);
        }

        return new Schema(fields, null);
    }

    /// <summary>"a.b" → "\"a\".\"b\"" (each part double-quote-doubled).</summary>
    internal static string QuoteQualified(string qualifiedName)
    {
        ArgumentException.ThrowIfNullOrEmpty(qualifiedName);
        return string.Join('.', qualifiedName.Split('.').Select(QuoteIdentifier));
    }

    internal static string BuildCreateTableSql(string targetTable, Schema schema)
    {
        var columns = schema.FieldsList.Select(f => $"{QuoteIdentifier(f.Name)} {ToDuckDbType(f.DataType)}");
        return $"CREATE TABLE {QuoteQualified(targetTable)} ({string.Join(", ", columns)})";
    }

    private static string QuoteIdentifier(string part) => "\"" + part.Replace("\"", "\"\"") + "\"";

    /// <summary>Splits "a.b" into schema "a" / table "b"; a bare "b" yields a null schema (current schema).</summary>
    internal static (string? Schema, string Table) SplitQualified(string qualifiedName)
    {
        var parts = qualifiedName.Split('.');
        return parts.Length switch
        {
            1 => (null, parts[0]),
            2 => (parts[0], parts[1]),
            _ => throw new ArgumentException(
                $"expected 'table' or 'schema.table', got '{qualifiedName}'", nameof(qualifiedName)),
        };
    }

    /// <summary>Converts DuckDB's `duckdb_error_data`/`duckdb_state` failures into exceptions with the
    /// native error message, then frees the error object.</summary>
    private static void ThrowIfError(nint errorData, string context)
    {
        if (errorData == 0)
        {
            return;
        }

        try
        {
            if (NativeMethods.duckdb_error_data_has_error(errorData))
            {
                var message = Marshal.PtrToStringUTF8(NativeMethods.duckdb_error_data_message(errorData));
                throw new InvalidOperationException($"{context}: {message}");
            }
        }
        finally
        {
            NativeMethods.duckdb_destroy_error_data(ref errorData);
        }
    }

    /// <summary>Owns the native handles needed to bulk-append a stream of same-schema Arrow batches into
    /// a single already-created DuckDB table: the Arrow→DuckDB converted schema (computed once) and a
    /// native Appender bound to <paramref name="targetTable"/>.</summary>
    internal sealed unsafe class ArrowIngestWriter : IDisposable
    {
        private readonly nint _connectionHandle;
        private readonly nint _convertedSchema;
        private readonly nint _appender;
        private bool _closed;
        private bool _disposed;

        private ArrowIngestWriter(nint connectionHandle, nint convertedSchema, nint appender)
        {
            _connectionHandle = connectionHandle;
            _convertedSchema = convertedSchema;
            _appender = appender;
        }

        internal static ArrowIngestWriter Create(nint connectionHandle, string targetTable, Schema schema)
        {
            var cSchema = CArrowSchema.Create();
            nint convertedSchema = 0;
            // Tracks whether `convertedSchema` still needs to be destroyed by this method. Set true the
            // instant duckdb_schema_from_arrow hands us ownership, and cleared only immediately before the
            // successful return, where ownership transfers onward to the new ArrowIngestWriter. Every
            // other exit from this method — SplitQualified throwing on a malformed name,
            // duckdb_appender_create failing, or anything else thrown after the conversion succeeded —
            // leaves the flag set, so the `finally` below destroys it. This turns "destroy on every path
            // that doesn't hand ownership onward" into a single flag check instead of duplicating the
            // destroy call at each throw site.
            var convertedSchemaOwned = false;
            try
            {
                CArrowSchemaExporter.ExportSchema(schema, cSchema);
                var err = NativeMethods.duckdb_schema_from_arrow(connectionHandle, (nint)cSchema, out convertedSchema);
                ThrowIfError(err, "duckdb_schema_from_arrow");
                convertedSchemaOwned = true;

                var (schemaName, tableName) = SplitQualified(targetTable);
                var state = NativeMethods.duckdb_appender_create(connectionHandle, schemaName, tableName, out var appender);
                if (state != 0)
                {
                    var errorData = NativeMethods.duckdb_appender_error_data(appender);
                    var message = Marshal.PtrToStringUTF8(NativeMethods.duckdb_error_data_message(errorData));
                    NativeMethods.duckdb_appender_destroy(ref appender);
                    throw new InvalidOperationException($"duckdb_appender_create failed for '{targetTable}': {message}");
                }

                convertedSchemaOwned = false; // ownership transfers to the returned ArrowIngestWriter
                return new ArrowIngestWriter(connectionHandle, convertedSchema, appender);
            }
            finally
            {
                // CArrowSchema.Free calls the exported struct's release callback (if any) before freeing
                // the outer unmanaged block — duckdb_schema_from_arrow only reads the schema to build its
                // own internal representation, it does not take ownership of it.
                CArrowSchema.Free(cSchema);

                if (convertedSchemaOwned)
                {
                    NativeMethods.duckdb_destroy_arrow_converted_schema(ref convertedSchema);
                }
            }
        }

        /// <summary>Converts <paramref name="batch"/> into a native DuckDB data chunk (ownership of the
        /// exported Arrow array transfers to that chunk — no copy at this step) and bulk-appends it.
        /// Does not take ownership of <paramref name="batch"/>; the caller disposes it.</summary>
        internal void AppendBatch(RecordBatch batch)
        {
            var cArray = CArrowArray.Create();
            nint chunk = 0;
            try
            {
                CArrowArrayExporter.ExportRecordBatch(batch, cArray);
                var err = NativeMethods.duckdb_data_chunk_from_arrow(_connectionHandle, (nint)cArray, _convertedSchema, out chunk);
                ThrowIfError(err, "duckdb_data_chunk_from_arrow");

                var appendState = NativeMethods.duckdb_append_data_chunk(_appender, chunk);
                if (appendState != 0)
                {
                    var errorData = NativeMethods.duckdb_appender_error_data(_appender);
                    var message = Marshal.PtrToStringUTF8(NativeMethods.duckdb_error_data_message(errorData));
                    throw new InvalidOperationException($"duckdb_append_data_chunk failed: {message}");
                }
            }
            finally
            {
                if (chunk != 0)
                {
                    NativeMethods.duckdb_destroy_data_chunk(ref chunk);
                }

                // CArrowArray.Free calls the exported struct's release callback before freeing the outer
                // block. duckdb_data_chunk_from_arrow takes ownership of the array's *data* into the
                // resulting chunk (no copy), but the chunk itself calls back into this release when the
                // chunk is destroyed — by the time duckdb_destroy_data_chunk above returns, the array's
                // release callback has already run, so this is the (idempotent) shell-memory free.
                CArrowArray.Free(cArray);
            }
        }

        /// <summary>Flushes and closes the appender, surfacing any pending write error. Call once after
        /// the last successfully-consumed batch; do not call on the cancellation/error path (Dispose
        /// still releases native resources without re-raising close-time errors there).</summary>
        internal void Complete()
        {
            if (_closed)
            {
                return;
            }

            _closed = true;
            var state = NativeMethods.duckdb_appender_close(_appender);
            if (state != 0)
            {
                var errorData = NativeMethods.duckdb_appender_error_data(_appender);
                var message = Marshal.PtrToStringUTF8(NativeMethods.duckdb_error_data_message(errorData));
                throw new InvalidOperationException($"duckdb_appender_close failed: {message}");
            }
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            if (!_closed)
            {
                _closed = true;
                // Best-effort close on an aborted (e.g. cancelled) ingest: never throw from Dispose.
                NativeMethods.duckdb_appender_close(_appender);
            }

            var appender = _appender;
            NativeMethods.duckdb_appender_destroy(ref appender);

            var convertedSchema = _convertedSchema;
            NativeMethods.duckdb_destroy_arrow_converted_schema(ref convertedSchema);
        }
    }

    /// <summary>Minimal P/Invoke surface for the native `duckdb` shared library's Arrow C Data Interface
    /// conversion functions (`duckdb_schema_from_arrow`/`duckdb_data_chunk_from_arrow`) plus the Appender
    /// functions needed to bulk-load the resulting chunks. None of these are exposed by DuckDB.NET's
    /// managed API — they're called directly against the native library DuckDB.NET.Data.Full
    /// already ships and resolves onto the process via its own native-library loading.</summary>
    private static unsafe class NativeMethods
    {
        [DllImport("duckdb")]
        internal static extern nint duckdb_schema_from_arrow(nint connection, nint schema, out nint out_types);

        [DllImport("duckdb")]
        internal static extern nint duckdb_data_chunk_from_arrow(nint connection, nint arrow_array, nint converted_schema, out nint out_chunk);

        [DllImport("duckdb")]
        internal static extern void duckdb_destroy_arrow_converted_schema(ref nint arrow_converted_schema);

        [DllImport("duckdb")]
        internal static extern void duckdb_destroy_data_chunk(ref nint chunk);

        [DllImport("duckdb")]
        internal static extern int duckdb_appender_create(
            nint connection,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string? schema,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string table,
            out nint out_appender);

        [DllImport("duckdb")]
        internal static extern int duckdb_append_data_chunk(nint appender, nint chunk);

        [DllImport("duckdb")]
        internal static extern int duckdb_appender_close(nint appender);

        [DllImport("duckdb")]
        internal static extern int duckdb_appender_destroy(ref nint appender);

        [DllImport("duckdb")]
        internal static extern nint duckdb_appender_error_data(nint appender);

        [DllImport("duckdb")]
        [return: MarshalAs(UnmanagedType.I1)]
        internal static extern bool duckdb_error_data_has_error(nint error_data);

        [DllImport("duckdb")]
        internal static extern nint duckdb_error_data_message(nint error_data);

        [DllImport("duckdb")]
        internal static extern void duckdb_destroy_error_data(ref nint error_data);
    }
}
