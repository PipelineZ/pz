using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Toolkit.Formats;
using Sylvan.Data.Csv;

namespace Pz.Connector.Sftp;

/// <summary>Universal-tier sftp source: SSH.NET has no DuckDB-native scan, so every read runs through
/// a managed format reader (csv/tsv/parquet/json) over a fresh <see cref="ISftpFileSystem"/> connection.
/// <paramref name="connect"/> is the unit-test seam (production wires <c>SftpClientFactory.Open</c>);
/// <see cref="GetSchemaAsync"/> opens exactly one transient connection for its peek and disposes it,
/// and each <see cref="SftpFilePartition"/> opens its OWN connection in <see cref="SftpFilePartition.ReadAsync"/>
/// and disposes it there — one <see cref="ISftpFileSystem"/> is never shared across concurrently-read
/// partitions (see that interface's doc comment for why).</summary>
internal sealed class SftpSource(SftpConnectionSettings settings, Func<SftpConnectionSettings, ISftpFileSystem> connect)
    : ISource, IOperationGateAware
{
    private IOperationGate? _gate;

    public void UseOperationGate(IOperationGate gate) => _gate = gate;

    /// <summary>csv: peeks the first matched file's header (Sylvan, no data rows read); a declared
    /// contract prunes to contract∩header IN CONTRACT ORDER (the LocalFiles CsvSource rule), a
    /// contract-less dataset reports every header column as varchar. parquet: peeks the first matched
    /// file's footer; a declared contract is verified column-by-column against the footer (every
    /// contract column must exist with a compatible v0 type) and the CONTRACT projection is reported,
    /// else the footer itself is the schema. json: the contract IS the schema — no file is opened —
    /// and a contract-less json dataset is a permanent error (there is no managed NDJSON schema
    /// inference).</summary>
    public async ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var format = ResolveFormat(spec);
        var declared = ExtractColumns(spec);

        if (format.Name == "json")
        {
            if (declared is not { Count: > 0 })
            {
                throw JsonContractRequiredError(spec);
            }

            return new DatasetSchema(ContractProjector.BuildSchema(declared));
        }

        var pattern = SftpPaths.ResolveReadPattern(settings.Root, spec, format.Extension);
        var fs = connect(settings);
        try
        {
            var matches = await SftpGate.ListMatchesAsync(_gate, fs, pattern, spec, ct).ConfigureAwait(false);
            if (matches.Count == 0)
            {
                throw NoMatchError(spec, pattern);
            }

            var path = matches[0];
            using var stream = await SftpGate.OpenReadAsync(_gate, fs, path, spec, ct).ConfigureAwait(false);

            if (format.Name == "parquet")
            {
                return await ParquetSchemaAsync(stream, declared, spec, path, ct).ConfigureAwait(false);
            }

            var delimiter = FileFormatCatalog.Delimiter(format, spec.Options, $"dataset '{spec.Dataset}'");
            return await CsvSchemaAsync(stream, declared, delimiter, ct).ConfigureAwait(false);
        }
        finally
        {
            fs.Dispose();
        }
    }

    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    /// <summary>ListMatches → groups the ordinally-sorted matches into <c>files_per_partition</c>-sized
    /// chunks → one <see cref="SftpFilePartition"/> per chunk, carrying everything a partition needs to
    /// open its own connection later: the files, format, contract, spec, connect factory, gate, and the
    /// pushdown hints (stored here so <see cref="SftpFilePartition.ReadAsync"/> can hand
    /// <see cref="ReadHints.Columns"/> to the parquet reader).</summary>
    public async ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        var format = ResolveFormat(spec);
        var declared = ExtractColumns(spec);
        if (format.Name == "json" && declared is not { Count: > 0 })
        {
            throw JsonContractRequiredError(spec);
        }

        var delimiter = FileFormatCatalog.Delimiter(format, spec.Options, $"dataset '{spec.Dataset}'");
        var groupSize = FilesPerPartition(spec);
        var pattern = SftpPaths.ResolveReadPattern(settings.Root, spec, format.Extension);
        var fs = connect(settings);
        IReadOnlyList<string> matches;
        try
        {
            matches = await SftpGate.ListMatchesAsync(_gate, fs, pattern, spec, ct).ConfigureAwait(false);
        }
        finally
        {
            fs.Dispose();
        }

        if (matches.Count == 0)
        {
            throw NoMatchError(spec, pattern);
        }

        var groups = matches.Chunk(groupSize).ToArray();
        var partitions = new IDatasetPartition[groups.Length];
        for (var i = 0; i < groups.Length; i++)
        {
            partitions[i] = new SftpFilePartition(settings, connect, groups[i], format.Name, delimiter, declared, spec, hints, _gate);
        }

        return partitions;
    }

    public ValueTask DisposeAsync() => default;

    private static async ValueTask<DatasetSchema> CsvSchemaAsync(
        Stream stream, IReadOnlyDictionary<string, string>? declared, char delimiter, CancellationToken ct)
    {
        using var textReader = new StreamReader(stream);
        using var csv = await CsvDataReader.CreateAsync(textReader, CsvOptions(delimiter), ct).ConfigureAwait(false);

        var header = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < csv.FieldCount; i++)
        {
            header.Add(csv.GetName(i));
        }

        if (declared is { Count: > 0 })
        {
            var fields = declared
                .Where(kv => header.Contains(kv.Key))
                .Select(kv => SftpTypeNameMap.ToArrowField(kv.Key, kv.Value))
                .ToArray();
            return new DatasetSchema(new Schema(fields, null));
        }

        var allFields = new Field[csv.FieldCount];
        for (var i = 0; i < csv.FieldCount; i++)
        {
            allFields[i] = SftpTypeNameMap.ToArrowField(csv.GetName(i), "varchar");
        }

        return new DatasetSchema(new Schema(allFields, null));
    }

    private static async ValueTask<DatasetSchema> ParquetSchemaAsync(
        Stream stream, IReadOnlyDictionary<string, string>? declared, DatasetSpec spec, string path, CancellationToken ct)
    {
        var context = FileContext(spec, path);
        var footer = await SftpParquetReader.ReadSchemaAsync(stream, context, ct).ConfigureAwait(false);
        if (declared is not { Count: > 0 })
        {
            return new DatasetSchema(footer);
        }

        var fields = new Field[declared.Count];
        var i = 0;
        foreach (var (name, typeName) in declared)
        {
            var footerField = FindField(footer, name) ?? throw new PzConnectorException(
                $"dataset '{spec.Dataset}': columns: contract declares '{name}', but the parquet footer " +
                $"'{path}' has no such column", isTransient: false);

            var expected = SftpTypeNameMap.ToArrowField(name, typeName);
            if (expected.DataType.TypeId != footerField.DataType.TypeId)
            {
                throw new PzConnectorException(
                    $"dataset '{spec.Dataset}': columns: contract declares '{name}' as '{typeName}', but the " +
                    $"parquet footer '{path}' has an incompatible type ({footerField.DataType.TypeId})",
                    isTransient: false);
            }

            fields[i++] = expected;
        }

        return new DatasetSchema(new Schema(fields, null));
    }

    private static Field? FindField(Schema schema, string name)
    {
        foreach (var field in schema.FieldsList)
        {
            if (field.Name == name)
            {
                return field;
            }
        }

        return null;
    }

    /// <summary>Sylvan's read buffer defaults to 16KiB and refuses any row wider than it -- sftp is
    /// universal-tier only (no DuckDB-native scan to fall back to, unlike LocalFiles' CsvSource), so
    /// that default would make any wide-row csv unreadable, failing the node with the library's own
    /// "Row N was too large. Try increasing the MaxBufferSize setting." -- advice naming a knob pz
    /// does not expose. Mirrors LocalFiles' CsvSource.MaxRowBytes: a CEILING, not a preallocation, so a
    /// project of narrow rows pays nothing for the headroom. Both the schema peek (<see
    /// cref="CsvSchemaAsync"/>) and the row read (<see cref="SftpFilePartition.ReadCsvAsync"/>) call
    /// this one place, so they can never disagree on the ceiling.</summary>
    private const int MaxRowBytes = 16 * 1024 * 1024;

    internal static CsvDataReaderOptions CsvOptions(char delimiter = ',') =>
        new() { HasHeaders = true, MaxBufferSize = MaxRowBytes, Delimiter = delimiter };

    private static int FilesPerPartition(DatasetSpec spec)
    {
        if (!spec.Options.TryGetValue("files_per_partition", out var raw) || raw is null)
        {
            return 1;
        }

        int value;
        try
        {
            value = Convert.ToInt32(raw, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is FormatException or OverflowException or InvalidCastException)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': files_per_partition '{raw}' is not a valid integer",
                isTransient: false, innerException: ex);
        }

        if (value <= 0)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': files_per_partition must be a positive integer (got {value})",
                isTransient: false);
        }

        return value;
    }

    private static PzConnectorException NoMatchError(DatasetSpec spec, string pattern) =>
        new($"dataset '{spec.Dataset}': no sftp files match pattern '{pattern}'", isTransient: false);

    private static PzConnectorException JsonContractRequiredError(DatasetSpec spec) =>
        new($"dataset '{spec.Dataset}': sftp json reads need a columns: contract (there is no managed " +
            "NDJSON schema inference) — declare the columns to read", isTransient: false);

    internal static string FileContext(DatasetSpec spec, string path) => $"dataset '{spec.Dataset}': file '{path}'";

    /// <summary>The entity names the file, and `format:` defaults csv. Resolved through the shared
    /// catalog like every other file-place connector; sftp has no native tier, so every resolved
    /// format must additionally pass <see cref="FileFormatCatalog.EnsureUniversalTierSupported"/>.
    /// Callers use <see cref="FileFormat.Name"/> to dispatch by format and
    /// <see cref="FileFormat.Extension"/> to resolve the default read path -- the same today (csv,
    /// json, parquet all name themselves after their extension) but distinct concerns.</summary>
    private static FileFormat ResolveFormat(DatasetSpec spec)
    {
        var context = $"dataset '{spec.Dataset}'";
        var format = FileFormatCatalog.Resolve(spec.Options, "csv", "sftp", context);
        FileFormatCatalog.EnsureUniversalTierSupported(format, spec.Options, "sftp", context);
        return format;
    }

    private static IReadOnlyDictionary<string, string>? ExtractColumns(DatasetSpec spec) =>
        spec.Options.TryGetValue("columns", out var value) &&
        value is IReadOnlyDictionary<string, string> { Count: > 0 } columns
            ? columns
            : null;
}

/// <summary>One group of remote files (<c>files_per_partition</c>-sized), read in order by opening a
/// fresh <see cref="ISftpFileSystem"/> here and disposing it when the enumeration ends — never sharing
/// one connection across concurrently-read partitions. Every emitted batch is routed through
/// <see cref="SftpWindowFilter"/> when the dataset is windowed; the filter is built lazily off the
/// first batch's own schema, so it works identically for all three formats without per-format
/// plumbing.
///
/// <paramref name="gate"/> is a snapshot of <c>SftpSource</c>'s gate field taken at
/// <c>PlanReadAsync</c> time. That is safe only because of the ABI's own ordering guarantee
/// (<c>IOperationGateAware.UseOperationGate</c>'s doc comment): the engine calls it exactly once,
/// before any plan/read call, so the gate can never change out from under a partition created
/// afterward.</summary>
internal sealed class SftpFilePartition(
    SftpConnectionSettings settings,
    Func<SftpConnectionSettings, ISftpFileSystem> connect,
    IReadOnlyList<string> files,
    string format,
    char delimiter,
    IReadOnlyDictionary<string, string>? columns,
    DatasetSpec spec,
    ReadHints hints,
    IOperationGate? gate) : IDatasetPartition
{
    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var fs = connect(settings);
        SftpWindowFilter? filter = null;
        try
        {
            foreach (var path in files)
            {
                await foreach (var batch in ReadFileAsync(fs, path, options, ct).ConfigureAwait(false))
                {
                    filter ??= BuildFilter(batch.Schema);
                    if (!filter.IsActive)
                    {
                        yield return batch;
                        continue;
                    }

                    var filtered = filter.Filter(batch);
                    if (filtered is null)
                    {
                        batch.Dispose();
                        continue;
                    }

                    if (!ReferenceEquals(filtered, batch))
                    {
                        batch.Dispose();
                    }

                    yield return filtered;
                }
            }
        }
        finally
        {
            fs.Dispose();
        }
    }

    private SftpWindowFilter BuildFilter(Schema schema)
    {
        var cursorType = spec.WatermarkCursor is { } cursor ? CursorTypeName(schema, cursor) : "varchar";
        return new SftpWindowFilter(spec, schema, cursorType);
    }

    /// <summary>The watermark cursor's v0 type name: from the declared contract when there is one
    /// (csv/json always have one by the time a windowed read reaches here; a declared parquet contract
    /// does too), else reverse-mapped from the actual Arrow field type — the footer-is-the-schema
    /// parquet case.</summary>
    private string CursorTypeName(Schema schema, string cursor)
    {
        if (columns is not null && columns.TryGetValue(cursor, out var typeName))
        {
            return typeName;
        }

        foreach (var field in schema.FieldsList)
        {
            if (field.Name == cursor)
            {
                return ReverseTypeName(field.DataType);
            }
        }

        // No match: SftpWindowFilter's own FindOrdinal throws the clear "cursor column not present"
        // error naming the dataset; the placeholder here is never actually consulted for a row.
        return "varchar";
    }

    private static string ReverseTypeName(IArrowType type) => type.TypeId switch
    {
        ArrowTypeId.Int32 => "int",
        ArrowTypeId.Int64 => "bigint",
        ArrowTypeId.Double => "double",
        ArrowTypeId.Decimal128 => "decimal",
        ArrowTypeId.Boolean => "boolean",
        ArrowTypeId.Date32 => "date",
        ArrowTypeId.Timestamp => "timestamp",
        _ => "varchar",
    };

    /// <summary>Opens the file, then drives the format-specific batch stream by hand (rather than an
    /// <c>await foreach</c>) so a mid-stream failure — a dropped SSH connection partway through a file,
    /// not just a failure to open one — can be caught and classified through
    /// <see cref="SftpErrors.Map"/> exactly like an open-time failure. C# forbids a <c>yield</c>
    /// anywhere inside a <c>try</c> block that has a <c>catch</c>, so the catch is scoped to the single
    /// <c>MoveNextAsync</c> call and the actual <c>yield return</c> sits outside it, in the surrounding
    /// try/finally-only block.</summary>
    private async IAsyncEnumerable<RecordBatch> ReadFileAsync(
        ISftpFileSystem fs, string path, BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var context = SftpSource.FileContext(spec, path);
        var stream = await SftpGate.OpenReadAsync(gate, fs, path, spec, ct).ConfigureAwait(false);
        try
        {
            var inner = FormatBatchesAsync(stream, path, context, options, ct);
            await using var enumerator = inner.GetAsyncEnumerator(ct);
            while (true)
            {
                bool hasNext;
                RecordBatch? batch = null;
                try
                {
                    hasNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
                    if (hasNext)
                    {
                        batch = enumerator.Current;
                    }
                }
                catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
                {
                    throw SftpErrors.Map(ex, context);
                }

                if (!hasNext)
                {
                    yield break;
                }

                yield return batch!;
            }
        }
        finally
        {
            await stream.DisposeAsync().ConfigureAwait(false);
        }
    }

    private IAsyncEnumerable<RecordBatch> FormatBatchesAsync(
        Stream stream, string path, string context, BatchOptions options, CancellationToken ct) => format switch
    {
        "json" => SftpJsonReader.ReadAsync(stream, columns!, context, options, ct),
        "parquet" => SftpParquetReader.ReadAsync(stream, ParquetProjection(), context, options, ct),
        "csv" or "tsv" => ReadCsvAsync(stream, path, options, ct),
        _ => throw new UnreachableException($"format '{format}' has no partition read path"),
    };

    /// <summary>The exact, ordered column list to hand the parquet reader — see
    /// <see cref="SftpParquetReader.ReadAsync"/>'s doc comment: a non-null list is honored
    /// POSITIONALLY, so it must already equal the order <c>SftpSource.GetSchemaAsync</c> reported for
    /// this spec. A declared contract's own key order already IS that reported order, so a column hint
    /// may safely narrow it (dropping entries, never reordering them). A contract-less schema's
    /// reported order is the footer's own physical order, which this partition cannot know without
    /// re-reading the footer — rather than risk emitting a hint-ordered (and therefore silently
    /// mis-columned) batch, a contract-less parquet read ignores <see cref="ReadHints.Columns"/>
    /// entirely and always reads the whole footer in footer order. SftpConnector does not declare
    /// <see cref="ConnectorCapabilities.ColumnPruning"/>, so this costs nothing but the (currently
    /// unused) optimization.</summary>
    private IReadOnlyList<string>? ParquetProjection()
    {
        if (columns is not { Count: > 0 })
        {
            return null;
        }

        if (hints.Columns is not { Count: > 0 })
        {
            return columns.Keys.ToArray();
        }

        var hinted = new HashSet<string>(hints.Columns, StringComparer.OrdinalIgnoreCase);
        return columns.Keys.Where(hinted.Contains).ToArray();
    }

    private async IAsyncEnumerable<RecordBatch> ReadCsvAsync(
        Stream stream, string path, BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        using var textReader = new StreamReader(stream);
        using var csv = await CsvDataReader.CreateAsync(textReader, SftpSource.CsvOptions(delimiter), ct).ConfigureAwait(false);

        string[] names;
        string[] typeNames;
        int[] ordinals;

        if (columns is { Count: > 0 })
        {
            names = columns.Keys.ToArray();
            typeNames = columns.Values.ToArray();
            ordinals = new int[names.Length];
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
                        $"sftp csv file '{path}': missing declared column '{names[i]}' in header",
                        isTransient: false);
                }

                ordinals[i] = ordinal;
            }
        }
        else
        {
            // Contract-less: every header column, as-is, typed varchar (mirrors GetSchemaAsync).
            names = new string[csv.FieldCount];
            typeNames = new string[csv.FieldCount];
            ordinals = new int[csv.FieldCount];
            for (var i = 0; i < csv.FieldCount; i++)
            {
                names[i] = csv.GetName(i);
                typeNames[i] = "varchar";
                ordinals[i] = i;
            }
        }

        var fields = new Field[names.Length];
        for (var i = 0; i < names.Length; i++)
        {
            fields[i] = SftpTypeNameMap.ToArrowField(names[i], typeNames[i]);
        }

        var schema = new Schema(fields, null);
        await foreach (var batch in CsvArrowReader
            .ReadAsync(csv, schema, typeNames, ordinals, path, options, rowNumberOffset: 0, ct)
            .ConfigureAwait(false))
        {
            yield return batch;
        }
    }
}

/// <summary>Routes the two discrete sftp operations (<c>sftp.list</c>, <c>sftp.open_read</c>) through
/// an engine-supplied gate when present, else calls straight through — the HttpPartition/AzureWriteSession
/// gate-or-direct idiom. Classification into <see cref="PzConnectorException"/> happens INSIDE the op
/// closure, so the gate always sees a fully-classified transient/permanent exception. Both operations
/// are idempotent: listing and opening a remote file for read have no side effect to replay.</summary>
internal static class SftpGate
{
    public static Task<IReadOnlyList<string>> ListMatchesAsync(
        IOperationGate? gate, ISftpFileSystem fs, string pattern, DatasetSpec spec, CancellationToken ct)
    {
        Task<IReadOnlyList<string>> Op(CancellationToken _)
        {
            try
            {
                return Task.FromResult(SftpPaths.ListMatches(fs, pattern, spec));
            }
            catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
            {
                throw SftpErrors.Map(ex, $"dataset '{spec.Dataset}'");
            }
        }

        return gate is null ? Op(ct) : gate.ExecuteAsync("sftp.list", idempotent: true, Op, ct);
    }

    public static Task<Stream> OpenReadAsync(
        IOperationGate? gate, ISftpFileSystem fs, string path, DatasetSpec spec, CancellationToken ct)
    {
        Task<Stream> Op(CancellationToken _)
        {
            try
            {
                return Task.FromResult(fs.OpenRead(path));
            }
            catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
            {
                throw SftpErrors.Map(ex, SftpSource.FileContext(spec, path));
            }
        }

        return gate is null ? Op(ct) : gate.ExecuteAsync("sftp.open_read", idempotent: true, Op, ct);
    }
}
