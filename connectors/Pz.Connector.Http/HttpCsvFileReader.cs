using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json.Nodes;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Toolkit.Formats;
using Sylvan.Data.Csv;

namespace Pz.Connector.Http;

/// <summary>Reads a CSV or TSV file. Text is decoded with `encoding` (UTF-8 when unset), a byte-order
/// mark wins over it and is never part of the first column name, and `skip_lines` lines are discarded
/// before the header. Line numbers in errors count from the top of the file.
///
/// Contract mode with a header reads through <see cref="CsvArrowReader"/> (columns located by name).
/// Contract mode without a header reads positionally, value by value through
/// <see cref="TextValueParser"/> (the same rules), so a row with fewer fields than the contract is an
/// error rather than trailing NULLs. Raw mode lands each row as a JSON object of strings keyed by
/// header name, or `c1..cN` without a header.</summary>
internal sealed class HttpCsvFileReader : IFileFormatReader
{
    /// <summary>Sylvan refuses any row wider than its read buffer; this is a ceiling, not a
    /// preallocation, so narrow rows pay nothing for the headroom.</summary>
    private const int MaxRowBytes = 16 * 1024 * 1024;

    public async IAsyncEnumerable<RecordBatch> ReadAsync(FileSource file, HttpDatasetConfig config,
        DateTimeOffset fetchedAt, BatchOptions options, string label, [EnumeratorCancellation] CancellationToken ct)
    {
        // A corrupt deflate stream throws InvalidDataException from whichever read reaches it.
        await using var batches = ReadCoreAsync(file, config, fetchedAt, options, label, ct).GetAsyncEnumerator(ct);
        while (true)
        {
            bool more;
            try
            {
                more = await batches.MoveNextAsync().ConfigureAwait(false);
            }
            catch (InvalidDataException ex)
            {
                throw new PzConnectorException($"{label}: file '{file.Name}': {ex.Message}", isTransient: false,
                    innerException: ex);
            }

            if (!more)
            {
                yield break;
            }

            yield return batches.Current;
        }
    }

    private static CsvDataReaderOptions CsvOptions(char delimiter, bool header) =>
        new() { HasHeaders = header, MaxBufferSize = MaxRowBytes, Delimiter = delimiter };

    private static async IAsyncEnumerable<RecordBatch> ReadCoreAsync(FileSource file, HttpDatasetConfig config,
        DateTimeOffset fetchedAt, BatchOptions options, string label, [EnumeratorCancellation] CancellationToken ct)
    {
        var fileConfig = config.File!;
        using var text = new StreamReader(file.Stream, fileConfig.Encoding ?? Encoding.UTF8,
            detectEncodingFromByteOrderMarks: true, leaveOpen: true);
        for (var i = 0; i < fileConfig.SkipLines; i++)
        {
            if (await text.ReadLineAsync(ct).ConfigureAwait(false) is null)
            {
                break;
            }
        }

        using var csv = await CsvDataReader
            .CreateAsync(text, CsvOptions(fileConfig.Delimiter, fileConfig.Header), ct).ConfigureAwait(false);

        // Sylvan numbers data rows from 1; this turns that into the file's own line number.
        var lineOffset = fileConfig.SkipLines + (fileConfig.Header ? 1 : 0);
        var source = !config.IsContractMode
            ? ReadRawAsync(csv, file.Name, config, fetchedAt, options, label, lineOffset, ct)
            : fileConfig.Header
                ? ReadByHeaderAsync(csv, file.Name, config, options, label, lineOffset, ct)
                : ReadPositionalAsync(csv, file.Name, config, options, label, lineOffset, ct);

        await foreach (var batch in source.ConfigureAwait(false))
        {
            yield return batch;
        }
    }

    private static async IAsyncEnumerable<RecordBatch> ReadByHeaderAsync(CsvDataReader csv, string name,
        HttpDatasetConfig config, BatchOptions options, string label, long lineOffset,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var columns = config.Columns!;
        var names = columns.Keys.ToArray();
        var typeNames = columns.Values.ToArray();
        var ordinals = new int[names.Length];
        for (var i = 0; i < names.Length; i++)
        {
            int ordinal;
            try
            {
                ordinal = csv.GetOrdinal(names[i]);
            }
            catch (IndexOutOfRangeException)
            {
                ordinal = -1;
            }

            if (ordinal < 0)
            {
                throw new PzConnectorException(
                    $"{label}: file '{name}': missing declared column '{names[i]}' in header", isTransient: false);
            }

            ordinals[i] = ordinal;
        }

        var contractSchema = ContractProjector.BuildSchema(columns);
        var outputSchema = config.File!.FileColumn is null ? null : FileOutput.Schema(config);
        await foreach (var batch in CsvArrowReader
            .ReadAsync(csv, contractSchema, typeNames, ordinals, name, options, lineOffset, ct)
            .ConfigureAwait(false))
        {
            // The appended batch owns the original's arrays; the original is never disposed.
            yield return outputSchema is null ? batch : FileOutput.AppendFileColumn(batch, outputSchema, name);
        }
    }

    private static async IAsyncEnumerable<RecordBatch> ReadPositionalAsync(CsvDataReader csv, string name,
        HttpDatasetConfig config, BatchOptions options, string label, long lineOffset,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var columns = config.Columns!;
        var names = columns.Keys.ToArray();
        var typeNames = columns.Values.ToArray();
        var schema = FileOutput.Schema(config);
        var hasFileColumn = config.File!.FileColumn is not null;
        var builder = new ArrowBatchBuilder(schema, options.TargetBatchBytes, maxRowsPerBatch: options.MaxRowsPerBatch);

        while (await csv.ReadAsync(ct).ConfigureAwait(false))
        {
            var line = csv.RowNumber + lineOffset;
            if (csv.RowFieldCount < names.Length)
            {
                throw new PzConnectorException(
                    $"{label}: file '{name}' line {line}: expected {names.Length} fields, got {csv.RowFieldCount}",
                    isTransient: false);
            }

            var row = new object?[schema.FieldsList.Count];
            for (var i = 0; i < names.Length; i++)
            {
                var cell = csv.GetString(i);
                row[i] = cell.Length == 0
                    ? null
                    : TextValueParser.Parse(cell, typeNames[i], $"{label}: file '{name}' line {line}: column '{names[i]}'");
            }

            if (hasFileColumn)
            {
                row[^1] = name;
            }

            builder.AppendRow(row);
            if (builder.TryTakeBatch(out var batch))
            {
                yield return batch!;
            }
        }

        if (builder.Flush() is { } tail)
        {
            yield return tail;
        }
    }

    private static async IAsyncEnumerable<RecordBatch> ReadRawAsync(CsvDataReader csv, string name,
        HttpDatasetConfig config, DateTimeOffset fetchedAt, BatchOptions options, string label, long lineOffset,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var builder = new ArrowBatchBuilder(FileOutput.Schema(config), options.TargetBatchBytes,
            maxRowsPerBatch: options.MaxRowsPerBatch);
        var header = config.File!.Header ? HeaderNames(csv, name, label) : null;

        long row = 0;
        while (await csv.ReadAsync(ct).ConfigureAwait(false))
        {
            row++;
            var payload = new JsonObject();
            if (header is not null)
            {
                for (var i = 0; i < header.Length; i++)
                {
                    payload[header[i]] = csv.GetString(i);
                }
            }
            else
            {
                for (var i = 0; i < csv.RowFieldCount; i++)
                {
                    payload[$"c{i + 1}"] = csv.GetString(i);
                }
            }

            builder.AppendRow(FileOutput.RawRow(payload, name, row, fetchedAt, config,
                $"{label}: file '{name}' line {csv.RowNumber + lineOffset}"));
            if (builder.TryTakeBatch(out var batch))
            {
                yield return batch!;
            }
        }

        if (builder.Flush() is { } tail)
        {
            yield return tail;
        }
    }

    /// <summary>A repeated header name would silently overwrite a value in the row's JSON object.</summary>
    private static string[] HeaderNames(CsvDataReader csv, string name, string label)
    {
        var names = new string[csv.FieldCount];
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < names.Length; i++)
        {
            names[i] = csv.GetName(i);
            if (!seen.Add(names[i]))
            {
                throw new PzConnectorException(
                    $"{label}: file '{name}': duplicate column '{names[i]}' in header", isTransient: false);
            }
        }

        return names;
    }
}
