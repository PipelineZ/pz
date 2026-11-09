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
/// header name (a short row pads with "", a row longer than the header is an error), or `c1..cN`
/// without a header.</summary>
internal sealed class HttpCsvFileReader : IFileFormatReader
{
    /// <summary>Sylvan refuses any row wider than its read buffer; this is a ceiling, not a
    /// preallocation, so narrow rows pay nothing for the headroom.</summary>
    private const int MaxRowBytes = 16 * 1024 * 1024;

    /// <summary>The for_each item this file was downloaded for; its fields land on every row.</summary>
    public ItemValues? Item { get; init; }

    public IAsyncEnumerable<RecordBatch> ReadAsync(FileSource file, HttpDatasetConfig config,
        DateTimeOffset fetchedAt, BatchOptions options, string label, CancellationToken ct) =>
        FileReaderErrors.WrapAsync(ReadCoreAsync(file, config, Item, fetchedAt, options, label, ct), label, file.Name, ct);

    private static CsvDataReaderOptions CsvOptions(char delimiter, bool header) =>
        new() { HasHeaders = header, MaxBufferSize = MaxRowBytes, Delimiter = delimiter };

    private static async IAsyncEnumerable<RecordBatch> ReadCoreAsync(FileSource file, HttpDatasetConfig config,
        ItemValues? itemValues, DateTimeOffset fetchedAt, BatchOptions options, string label,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var item = ItemCells.For(config, itemValues, label);
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
            ? ReadRawAsync(csv, file.Name, config, item, fetchedAt, options, label, lineOffset, ct)
            : fileConfig.Header
                ? ReadByHeaderAsync(csv, file.Name, config, item, options, label, lineOffset, ct)
                : ReadPositionalAsync(csv, file.Name, config, item, options, label, lineOffset, ct);

        await foreach (var batch in source.ConfigureAwait(false))
        {
            yield return batch;
        }
    }

    /// <summary>Only the columns the item does not fill are looked up in the header; the item's columns
    /// and `file_column` join each batch as constant columns at their declared positions.</summary>
    private static async IAsyncEnumerable<RecordBatch> ReadByHeaderAsync(CsvDataReader csv, string name,
        HttpDatasetConfig config, ItemCells? item, BatchOptions options, string label, long lineOffset,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var columns = item?.RecordColumns ?? config.Columns!;
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
        var hasFileColumn = config.File!.FileColumn is not null;
        var hasItemColumns = item is { Columns.Count: > 0 };
        var outputSchema = !hasItemColumns && !hasFileColumn ? null : FileOutput.Schema(config);
        List<(int Index, object? Value)>? constants = null;
        if (hasItemColumns)
        {
            constants = [.. item!.Columns];
            if (hasFileColumn)
            {
                constants.Add((outputSchema!.FieldsList.Count - 1, name));
            }
        }

        await foreach (var batch in CsvArrowReader
            .ReadAsync(csv, contractSchema, typeNames, ordinals, name, options, lineOffset, ct)
            .ConfigureAwait(false))
        {
            // The assembled batch owns the original's arrays; the original is never disposed.
            yield return outputSchema is null ? batch
                : constants is null ? FileOutput.AppendFileColumn(batch, outputSchema, name)
                : FileOutput.WithConstantColumns(batch, outputSchema, constants);
        }
    }

    /// <summary>The file's fields map in order onto the columns the item does not fill.</summary>
    private static async IAsyncEnumerable<RecordBatch> ReadPositionalAsync(CsvDataReader csv, string name,
        HttpDatasetConfig config, ItemCells? item, BatchOptions options, string label, long lineOffset,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var columns = item?.RecordColumns ?? config.Columns!;
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

            var row = new object?[item is null ? schema.FieldsList.Count : names.Length];
            for (var i = 0; i < names.Length; i++)
            {
                var cell = csv.GetString(i);
                row[i] = cell.Length == 0
                    ? null
                    : TextValueParser.Parse(cell, typeNames[i], $"{label}: file '{name}' line {line}: column '{names[i]}'");
            }

            if (item is not null)
            {
                row = item.Place(row, hasFileColumn ? 1 : 0);
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
        HttpDatasetConfig config, ItemCells? item, DateTimeOffset fetchedAt, BatchOptions options, string label, long lineOffset,
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
                // A short row pads with "" (Sylvan's own reading); a long one has values with no name
                // to land under, and dropping them would lose data silently.
                if (csv.RowFieldCount > header.Length)
                {
                    throw new PzConnectorException(
                        $"{label}: file '{name}' line {csv.RowNumber + lineOffset}: expected {header.Length} " +
                        $"fields, got {csv.RowFieldCount}", isTransient: false);
                }

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
                $"{label}: file '{name}' line {csv.RowNumber + lineOffset}", item));
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
