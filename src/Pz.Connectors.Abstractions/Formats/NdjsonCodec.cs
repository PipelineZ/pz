using System.Buffers;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connectors.Abstractions.Formats;

/// <summary>Arrow RecordBatch ↔ NDJSON, shared by every file connector's universal tier. Pure and
/// dependency-free beyond Apache.Arrow + System.Text.Json (both in the ABI allowlist / shared framework
/// -- System.Text.Json ships in the shared framework, so this adds no package reference).</summary>
public static partial class NdjsonCodec
{
    /// <summary>Bytes to accumulate before pushing to <c>ndjson</c>. One writer-to-stream handoff per
    /// ~64 KiB of output rather than per row.</summary>
    private const int FlushThreshold = 64 * 1024;

    /// <summary>Writes <paramref name="batch"/> to <paramref name="ndjson"/> as newline-delimited JSON:
    /// one JSON object per row, field order = <paramref name="batch"/>'s <c>Schema</c> order, LF-terminated
    /// including a trailing newline after the final row. Does not dispose <paramref name="ndjson"/> --
    /// ownership stays with the caller. Formatting is invariant-culture / ISO-8601 UTC throughout so output
    /// is byte-stable across machines and locales (mirrors the repo's byte-stable-writer rule).</summary>
    public static async Task WriteAsync(RecordBatch batch, Stream ndjson, CancellationToken ct)
    {
        var opts = new JsonWriterOptions { Indented = false, SkipValidation = true };
        var fields = batch.Schema.FieldsList;

        // One Utf8JsonWriter reused across rows (Reset per line keeps LF framing explicit and the bytes
        // unchanged), writing into a byte buffer that is handed to the stream every ~64 KiB. Constructing
        // and flushing a writer per row instead costs ~650 bytes of allocation and one stream write per
        // row -- on a 1M-row batch, ~650 MiB of garbage and 1M awaits for 70 MiB of output.
        var buffer = new ArrayBufferWriter<byte>(FlushThreshold);
        var writer = new Utf8JsonWriter(buffer, opts);
        await using (writer.ConfigureAwait(false))
        {
            // Property names are escaped once per batch rather than re-escaped on every row, which is
            // what passing a raw string to Utf8JsonWriter does.
            var columns = new IArrowArray[batch.ColumnCount];
            var names = new JsonEncodedText[batch.ColumnCount];
            for (var col = 0; col < batch.ColumnCount; col++)
            {
                columns[col] = batch.Column(col);
                names[col] = JsonEncodedText.Encode(fields[col].Name);
            }

            for (var row = 0; row < batch.Length; row++)
            {
                ct.ThrowIfCancellationRequested();

                writer.Reset();
                writer.WriteStartObject();
                for (var col = 0; col < columns.Length; col++)
                {
                    WriteScalar(writer, names[col], columns[col], row);
                }

                writer.WriteEndObject();
                writer.Flush();
                buffer.Write("\n"u8);

                if (buffer.WrittenCount >= FlushThreshold)
                {
                    await ndjson.WriteAsync(buffer.WrittenMemory, ct).ConfigureAwait(false);

                    // A row far wider than the threshold (a multi-megabyte string cell) grows the buffer
                    // to hold it; hand that capacity back rather than keeping it for the whole batch.
                    buffer = buffer.Capacity > FlushThreshold * 4
                        ? new ArrayBufferWriter<byte>(FlushThreshold)
                        : Cleared(buffer);
                    writer.Reset(buffer);
                }
            }

            if (buffer.WrittenCount > 0)
            {
                await ndjson.WriteAsync(buffer.WrittenMemory, ct).ConfigureAwait(false);
            }
        }
    }

    private static ArrayBufferWriter<byte> Cleared(ArrayBufferWriter<byte> buffer)
    {
        buffer.ResetWrittenCount();
        return buffer;
    }

    /// <summary>Writes one row's scalar for one column: a null cell emits JSON <c>null</c>, else dispatches
    /// on the Arrow array's runtime type. Covers the full "columns:" contract type matrix -- int32/int64/
    /// double/decimal128/utf8/bool/date32/timestamp-µs-UTC -- the same set <c>AzureTypeNameMap</c> maps and
    /// <c>AzureBlobFormat.ScalarToString</c>/<see cref="Batches.ArrowBatchBuilder"/> already handle.</summary>
    private static void WriteScalar(Utf8JsonWriter jw, JsonEncodedText name, IArrowArray array, int index)
    {
        if (array.IsNull(index))
        {
            jw.WriteNull(name);
            return;
        }

        switch (array)
        {
            case Int32Array a:
                jw.WriteNumber(name, a.GetValue(index)!.Value);
                break;
            case Int64Array a:
                jw.WriteNumber(name, a.GetValue(index)!.Value);
                break;
            case DoubleArray a:
                // Non-finite doubles (NaN/+Infinity/-Infinity) serialize as JSON `null` because NDJSON has
                // no numeric literal for them and Utf8JsonWriter.WriteNumber throws ArgumentException on
                // non-finite input. This is a deliberate, lossy-but-valid encoding: DuckDB double columns
                // can legitimately produce these values (e.g. 1.0/0.0, 0.0/0.0), and the alternative is a
                // crash on otherwise-valid data. Round-trips through ReadAsync as null, same as an actual
                // null cell.
                var d = a.GetValue(index)!.Value;
                if (!double.IsFinite(d))
                {
                    jw.WriteNull(name);
                }
                else
                {
                    jw.WriteNumber(name, d);
                }
                break;
            case Decimal128Array a:
                jw.WriteNumber(name, a.GetValue(index)!.Value);
                break;
            case BooleanArray a:
                jw.WriteBoolean(name, a.GetValue(index)!.Value);
                break;
            case StringArray a:
                // The value's UTF-8 bytes go to the writer as-is; decoding to a string first would
                // allocate per cell and re-encode to the same bytes.
                jw.WriteString(name, a.GetBytes(index));
                break;
            case Date32Array a:
                jw.WriteString(name, a.GetDateTime(index)!.Value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
                break;
            case TimestampArray a:
                jw.WriteString(name, FormatTimestamp(a.GetTimestamp(index)!.Value));
                break;
            default:
                throw new NotSupportedException(
                    $"NdjsonCodec.WriteAsync does not support Arrow array type '{array.GetType()}' for column '{name}'");
        }
    }

    /// <summary>ISO-8601 UTC, microsecond precision, 'Z' designator -- e.g. "2026-07-13T10:30:15.000000Z".</summary>
    private static string FormatTimestamp(DateTimeOffset value) =>
        value.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "Z";

    /// <summary>Streams <paramref name="ndjson"/> (one JSON object per line) into Arrow <see cref="RecordBatch"/>es,
    /// projecting <paramref name="projection"/> (or every declared column when null) and typing each value per
    /// <paramref name="contract"/> -- the same eight-type "columns:" matrix <see cref="WriteAsync"/> covers
    /// (int/bigint/double/decimal/varchar/boolean/date/timestamp). Reads line-by-line via <see cref="StreamReader"/>
    /// (never materializes the whole stream) and parses each line with <see cref="JsonDocument"/>; a JSON key
    /// not in the projection is ignored, a projected column absent from a line yields Arrow null (same as an
    /// explicit JSON <c>null</c> -- see the round-trip note on <see cref="WriteAsync"/>'s non-finite-double
    /// handling). Rows are handed to <see cref="ArrowBatchBuilder"/>, which flushes a batch once
    /// <paramref name="options"/>'s row/byte cap is reached; whatever remains is flushed once more at EOF.
    /// Does not dispose <paramref name="ndjson"/> -- ownership stays with the caller. A top-level JSON array
    /// (first non-whitespace character is <c>[</c>) is not valid NDJSON and throws a permanent
    /// <see cref="PzConnectorException"/> (surfaced on the first <c>MoveNextAsync</c>, per async-iterator
    /// semantics, not on the call to <see cref="ReadAsync"/> itself).</summary>
    public static async IAsyncEnumerable<RecordBatch> ReadAsync(
        Stream ndjson,
        IReadOnlyDictionary<string, string> contract,
        IReadOnlyList<string>? projection,
        BatchOptions options,
        [EnumeratorCancellation] CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(ndjson);
        ArgumentNullException.ThrowIfNull(contract);
        ArgumentNullException.ThrowIfNull(options);

        var columns = projection ?? contract.Keys.ToList();
        var fields = new Field[columns.Count];
        for (var i = 0; i < columns.Count; i++)
        {
            var name = columns[i];
            if (!contract.TryGetValue(name, out var typeName))
            {
                throw new PzConnectorException(
                    $"column '{name}': projected but not declared in the columns: contract", isTransient: false);
            }

            fields[i] = new Field(name, ToArrowType(typeName, name), nullable: true);
        }

        var schema = new Schema(fields, null);
        var builder = new ArrowBatchBuilder(schema, options.TargetBatchBytes, maxRowsPerBatch: options.MaxRowsPerBatch);

        using var reader = new StreamReader(ndjson, System.Text.Encoding.UTF8, detectEncodingFromByteOrderMarks: true,
            bufferSize: -1, leaveOpen: true);

        // NDJSON is line-delimited objects, not a single JSON document -- reject a top-level array up
        // front so the error is unambiguous rather than surfacing as a per-line parse failure later.
        int peek;
        while ((peek = reader.Peek()) != -1 && char.IsWhiteSpace((char)peek))
        {
            reader.Read();
        }

        if (peek == '[')
        {
            throw new PzConnectorException(
                "NDJSON input must be newline-delimited JSON objects, not a top-level JSON array",
                isTransient: false);
        }

        string? line;
        var lineNumber = 0;
        while ((line = await reader.ReadLineAsync(ct).ConfigureAwait(false)) is not null)
        {
            ct.ThrowIfCancellationRequested();
            lineNumber++;
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var values = new object?[columns.Count];
            JsonDocument doc;
            try
            {
                doc = JsonDocument.Parse(line);
            }
            catch (JsonException ex)
            {
                throw new PzConnectorException(
                    $"malformed JSON on NDJSON line {lineNumber}", isTransient: false, innerException: ex);
            }

            using (doc)
            {
                var root = doc.RootElement;
                for (var i = 0; i < columns.Count; i++)
                {
                    var name = columns[i];
                    values[i] = ExtractValue(root, name, contract[name], lineNumber);
                }
            }

            builder.AppendRow(values);
            if (builder.TryTakeBatch(out var batch))
            {
                yield return batch!;
            }
        }

        var final = builder.Flush();
        if (final is not null)
        {
            yield return final;
        }
    }

    /// <summary>Resolves one <c>columns:</c> contract type name to its Arrow type -- the same eight-name
    /// canonical vocabulary <see cref="Pz.Engine.Validation.ContractTypes"/> and
    /// <c>AzureTypeNameMap.ToArrowType</c> use (int/bigint/double/decimal/varchar/boolean/date/
    /// timestamp), including the "+00:00" timestamp timezone (not the literal string "UTC") so this
    /// matches every other Arrow-expectation map in the codebase. Throws a permanent
    /// <see cref="PzConnectorException"/> naming <paramref name="columnName"/> for an unknown type name.</summary>
    private static IArrowType ToArrowType(string typeName, string columnName) => typeName switch
    {
        "int" => Int32Type.Default,
        "bigint" => Int64Type.Default,
        "double" => DoubleType.Default,
        "decimal" => new Decimal128Type(38, 9),
        "varchar" => StringType.Default,
        "boolean" => BooleanType.Default,
        "date" => Date32Type.Default,
        "timestamp" => new TimestampType(TimeUnit.Microsecond, "+00:00"),
        _ => throw new PzConnectorException(
            $"column '{columnName}': unknown columns: contract type '{typeName}'", isTransient: false),
    };

    /// <summary>Extracts and types one declared column's value from a parsed JSON line: a JSON key absent
    /// from <paramref name="line"/>, or present with JSON <c>null</c>, yields a null cell (both are
    /// indistinguishable downstream, matching <see cref="ArrowBatchBuilder.AppendRow"/>'s null convention).
    /// The returned value's runtime type matches exactly what <see cref="ArrowBatchBuilder"/>'s per-type
    /// appender expects for <paramref name="typeName"/> (int/long/double/decimal/string/bool/DateOnly/
    /// DateTimeOffset). A value that is present but whose JSON kind doesn't match <paramref name="typeName"/>
    /// (e.g. a JSON string for a declared <c>bigint</c>) surfaces as a permanent <see cref="PzConnectorException"/>
    /// naming <paramref name="columnName"/>, <paramref name="typeName"/>, and <paramref name="lineNumber"/> --
    /// never the offending value, which may be untrusted external data.</summary>
    private static object? ExtractValue(JsonElement row, string columnName, string typeName, int lineNumber)
    {
        if (!row.TryGetProperty(columnName, out var element) || element.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        try
        {
            return typeName switch
            {
                "int" => element.GetInt32(),
                "bigint" => element.GetInt64(),
                "double" => element.GetDouble(),
                "decimal" => element.GetDecimal(),
                "varchar" => element.GetString(),
                "boolean" => element.GetBoolean(),
                "date" => DateOnly.ParseExact(element.GetString()!, "yyyy-MM-dd", CultureInfo.InvariantCulture),
                "timestamp" => ParseTimestamp(element.GetString()!, columnName, lineNumber),
                _ => throw new PzConnectorException(
                    $"column '{columnName}': unknown columns: contract type '{typeName}'", isTransient: false),
            };
        }
        catch (Exception ex) when (ex is InvalidOperationException or FormatException or OverflowException)
        {
            throw new PzConnectorException(
                $"NDJSON line {lineNumber}, column '{columnName}': value is not a valid {typeName}",
                isTransient: false, innerException: ex);
        }
    }

    /// <summary>Parses an ISO-8601 timestamp back to a UTC <see cref="DateTimeOffset"/>, accepting any
    /// fractional-second precision and either a 'Z' or numeric-offset designator -- covering both
    /// <see cref="WriteAsync"/>'s own "yyyy-MM-ddTHH:mm:ss.ffffffZ" wire format (so the write/read round-trip
    /// holds) and the looser forms real-world NDJSON and DuckDB's native <c>read_json</c>/<c>COPY ... (FORMAT
    /// json)</c> produce. Malformed input surfaces as a permanent <see cref="PzConnectorException"/> naming
    /// <paramref name="columnName"/> and <paramref name="lineNumber"/> only -- never <paramref name="text"/>,
    /// which may be untrusted external data (mirrors <see cref="ExtractValue"/>'s message shape).</summary>
    private static DateTimeOffset ParseTimestamp(string text, string columnName, int lineNumber)
    {
        if (!DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var value))
        {
            throw new PzConnectorException(
                $"NDJSON line {lineNumber}, column '{columnName}': value is not a valid timestamp",
                isTransient: false);
        }

        return value;
    }
}
