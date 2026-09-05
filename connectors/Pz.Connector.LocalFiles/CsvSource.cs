using System.Globalization;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Toolkit.Formats;
using Sylvan.Data.Csv;

namespace Pz.Connector.LocalFiles;

/// <summary>CSV source. On the universal tier Sylvan.Data.Csv reads the file with a mandatory declared
/// `columns:` contract (no type inference), one partition per readable byte range (see
/// <see cref="PlanReadAsync"/>). The native scan tier (<see cref="TryGetNativeScan"/>) goes through
/// DuckDB's <c>read_csv</c>: a fully contract-less dataset is inferred by DuckDB's own
/// <c>auto_detect</c>, while a declared contract -- partial or full -- prunes the read to exactly its
/// declared columns. tsv shares this whole reader with csv -- it is the same code with the field
/// delimiter fixed to a tab (<see cref="FileFormatCatalog.Delimiter"/>) rather than a class of its
/// own.</summary>
internal sealed class CsvSource(string baseDir) : ISource
{
    /// <summary>Sylvan's read buffer defaults to 16KiB and refuses any row wider than it, failing the
    /// node with the library's own "Row N was too large. Try increasing the
    /// MaxBufferSize setting." — advice naming a knob pz does not expose. The native tier (DuckDB's
    /// <c>read_csv</c>) reads rows far larger, and the planner is free to choose either tier for the same
    /// dataset, so a row size the universal path rejects and the native path accepts is a breach of the
    /// two tiers' behavioural-interchangeability contract, not a tuning preference.
    ///
    /// This is a CEILING, not a preallocation: Sylvan starts at its small default buffer and grows only
    /// as far as a row actually demands, so a project of narrow rows pays nothing for the headroom. The
    /// value clears DuckDB's own default <c>max_line_size</c> (2MB) by a wide margin, so the universal
    /// tier is the more permissive of the two rather than the more restrictive.</summary>
    internal const int MaxRowBytes = 16 * 1024 * 1024;

    /// <summary>The one place CSV reader options are constructed — every read site that resolves an
    /// actual field/schema/row parse (header peek, ordered header for contract validation, and the
    /// partition's row read) must agree on <see cref="MaxRowBytes"/>, or a file would parse its header
    /// and then fail on the same row's data. <paramref name="delimiter"/> is comma for csv, tab for tsv,
    /// or the validated <c>delimiter:</c> option (<see cref="FileFormatCatalog.Delimiter"/>) -- passing
    /// it disables Sylvan's own delimiter auto-detection, which is what makes the read honour a
    /// declared tsv/custom delimiter instead of guessing at one. Null (the split planner's own use,
    /// see <see cref="TryPlanSplits"/>) leaves auto-detection on, which is what that planner's comma
    /// proof needs: an oracle for the file's REAL delimiter, independent of what was declared.</summary>
    internal static CsvDataReaderOptions ReaderOptions(char? delimiter = null) =>
        delimiter is { } d
            ? new() { HasHeaders = true, MaxBufferSize = MaxRowBytes, Delimiter = d }
            : new() { HasHeaders = true, MaxBufferSize = MaxRowBytes };

    /// <summary>Resolves the dataset's field delimiter through the shared catalog -- comma for csv
    /// (unless <c>delimiter:</c> overrides it), tab for tsv. The one place every read site in this class
    /// derives it, so a dataset's csv/tsv choice and its <c>delimiter:</c> option are honoured uniformly.</summary>
    private static char DelimiterOf(DatasetSpec spec)
    {
        var context = $"dataset '{spec.Dataset}'";
        var format = FileFormatCatalog.Resolve(spec.Options, "csv", "localfiles", context);
        return FileFormatCatalog.Delimiter(format, spec.Options, context);
    }

    /// <summary>Peeks the file's actual header row (cheap: no data rows are read) and reports only the
    /// declared contract columns that are ALSO present there -- a declared column absent from the header
    /// is simply omitted, which is what lets tier 5 (<c>ConnectivityValidator</c>) detect it as schema
    /// drift (PZ0331 "missing from the fetched schema"). Types still come entirely from the contract
    /// (this path does no type inference, so there is never a type MISMATCH to detect here, only a
    /// missing/extra column). An extra header column not in the contract is silently dropped too --
    /// tolerated by design: contracts prune on read.
    ///
    /// Two production callers, each fine with the pruning above for a different reason:
    /// <list type="bullet">
    /// <item><description><c>ConnectivityValidator</c>'s drift phase (tier 5, <c>pz validate --connect</c>)
    /// calls this directly to compare the pruned schema against the declared contract -- the missing
    /// column IS the signal it is looking for.</description></item>
    /// <item><description><c>SourceLoadExecutor</c> (a real `pz run`) calls this once per dataset to size
    /// and type the Arrow schema handed to <c>IDuckSession.IngestArrowAsync</c>, THEN separately calls
    /// <c>PlanReadAsync</c>/<c>CsvPartition.ReadAsync</c>, which builds its own column list from the FULL
    /// declared contract (not this method's pruned result) and resolves each column's ordinal against the
    /// real header before reading a single row. If a declared column is missing from the header, that
    /// ordinal lookup fails and <see cref="CsvPartition"/> throws its own clear "missing declared column"
    /// <see cref="PzConnectorException"/> before yielding any batch -- so the executor never actually acts
    /// on the pruned (shorter) schema from THIS call for a missing column; the partition read fails first,
    /// cleanly, pre-yield. Pinned end-to-end by
    /// <c>HelloRunTests.Run_with_csv_missing_declared_column_fails_cleanly_at_load</c>.</description></item>
    /// </list></summary>
    public async ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var columns = GetColumnsContract(spec);
        var header = await ReadHeaderAsync(ResolvePath(spec), DelimiterOf(spec), ct).ConfigureAwait(false);
        var fields = columns
            .Where(kv => header.Contains(kv.Key))
            .Select(kv => TypeNameMap.ToArrowField(kv.Key, kv.Value))
            .ToArray();
        return new DatasetSchema(new Schema(fields, null));
    }

    private static async Task<HashSet<string>> ReadHeaderAsync(string path, char delimiter, CancellationToken ct)
    {
        if (!File.Exists(path))
        {
            throw new PzConnectorException($"csv file not found: '{path}'", isTransient: false);
        }

        using var textReader = new StreamReader(path);
        using var csv = await CsvDataReader.CreateAsync(
            textReader, ReaderOptions(delimiter), ct).ConfigureAwait(false);

        var names = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < csv.FieldCount; i++)
        {
            names.Add(csv.GetName(i));
        }

        return names;
    }

    /// <summary>Two-state model, not three. A declared `columns:` contract -- partial or full, native
    /// scan has no way to tell them apart without reading the file, which it deliberately never does --
    /// always renders the strict fragment (`auto_detect = false, columns = {...}`): DuckDB reads ONLY
    /// the named columns, pruning everything else, because a contract means "this is the schema, prune
    /// to it" (see the class doc comment). Only a fully contract-less dataset gets `auto_detect = true`
    /// with no `columns=`/`types=` map at all, letting DuckDB infer the whole schema as part of the real
    /// read. The `types=` + `auto_detect = true` combination works but KEEPS every column in the file
    /// instead of pruning to the declared ones, so it is deliberately not used; "declare some columns,
    /// infer the rest" is therefore out of native scan's scope. Json's
    /// `AzureSource.TryGetNativeScan` uses this same two-state shape for its own, unrelated reason
    /// (`read_json` has no `types=` parameter at all).</summary>
    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        var context = $"dataset '{spec.Dataset}'";
        var format = FileFormatCatalog.Resolve(spec.Options, "csv", "localfiles", context);
        var delimiter = FileFormatCatalog.Delimiter(format, spec.Options, context);
        var absPath = ResolvePath(spec); // existing helper resolving against base_dir
        var declared = ExtractColumns(spec); // null (contract-less) or declared (partial or full)
        if (declared is { Count: > 0 })
        {
            ValidateHeaderMatchesContract(absPath, spec.Dataset, declared, delimiter);
        }
        else if (File.Exists(absPath) && new FileInfo(absPath).Length == 0)
        {
            // A zero-byte file has no header row to infer a schema from, and DuckDB's auto_detect
            // fabricates a single `column0` VARCHAR column instead of failing -- a made-up schema that
            // would propagate silently to every downstream sink. Refuse loudly while the schema is still
            // undecided; same file-exists scoping as ValidateHeaderMatchesContract (an absent file at
            // plan time is legitimate, and a header-only file is a legitimate empty dataset).
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': csv file '{absPath}' is empty (0 bytes) -- there is no header " +
                "to auto-detect a schema from. Provide a file with at least a header row, or declare a " +
                "columns: contract if an empty feed is expected",
                isTransient: false);
        }

        var urlArg = $"'{EscapeSqlLiteral(absPath)}'";
        var request = new FormatReadRequest(urlArg, 1, declared, TypeNameMap.ToDuckDbName);
        var fragment = FileFormatCatalog.ReadFragment(format, spec.Options, request, context);
        var inferred = FileFormatCatalog.SchemaInferred(format, declared);
        scan = new NativeScan(WrapWindowed(fragment, spec), FileFormatCatalog.SetupStatements(format))
        {
            Mechanism = FileFormatCatalog.ReadMechanism(format),
            SchemaInferred = inferred,
            SniffFragment = inferred ? FileFormatCatalog.SniffFragment(format, spec.Options, urlArg, context) : null,
        };
        return true;
    }

    /// <summary>Wraps a native-scan fragment with the windowed bound predicate when both
    /// <see cref="DatasetSpec.WatermarkCursor"/> and
    /// <see cref="DatasetSpec.WatermarkUpperBound"/> are set (only ever true for a windowed dataset --
    /// <c>SpecBuilder.ForSourceLoad</c>'s three-arg overload, stamped only by <c>SourceLoadExecutor</c>) --
    /// DuckDB applies the filter as part of the native CTAS, so the "extraction" (which for local files IS
    /// DuckDB reading the file) only lands the windowed slice, not the whole file. Shared verbatim between
    /// <see cref="CsvSource"/> and <see cref="ParquetSource"/> (same seam, same contract, same escaping
    /// discipline) via <see cref="LocalFilesWindowSql"/>.
    ///
    /// Deliberately does NOT fire for plain (unwindowed) incremental -- <see
    /// cref="DatasetSpec.WatermarkCursor"/> set alone, no <see cref="DatasetSpec.WatermarkUpperBound"/>:
    /// ignoring the lower-bound watermark on the native tier is correct here (merge dedups; see
    /// <see cref="DatasetSpec.WatermarkCursor"/>'s doc comment) -- there is nothing to push down until a
    /// bound actually makes correctness depend on it. The fragment returned for that case, and for a spec
    /// with no watermark fields at all, is byte-identical to the unwrapped fragment.
    ///
    /// The universal (non-native) path -- <see cref="CsvPartition.ReadAsync"/> -- deliberately does NOT
    /// filter at all, windowed or not: windowed LocalFiles datasets require the native path (this method),
    /// and the engine's candidate-cap rule (a non-empty slice's watermark candidate is always
    /// <c>Min(landedMax, windowUpper)</c>) keeps watermark advancement correct even under
    /// <c>engine.force_universal</c>, where a connector that ignores the bound would over-extract but could
    /// never advance the cursor past the window it was supposed to extract.</summary>
    private static string WrapWindowed(string fragment, DatasetSpec spec) => LocalFilesWindowSql.Wrap(fragment, spec);

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");

    /// <summary><c>read_csv(..., auto_detect = false, columns = {...})</c> binds the contract to
    /// the file BY POSITION -- DuckDB ignores the header names entirely when auto_detect is off. A contract
    /// whose declared order disagrees with the file's actual header therefore loads each file column's
    /// values under a DIFFERENT declared column's name, silently (e.g. header <c>price,qty</c> with contract
    /// <c>qty,price</c> lands price values in <c>qty</c>). The universal tier (<see cref="CsvPartition"/>)
    /// binds by name and is immune, so the two tiers would silently disagree. When the file is present, this
    /// verifies every declared column, in declaration order, matches the header column at the same position,
    /// refusing loudly otherwise. The file may legitimately be absent at plan time (templated paths, a
    /// not-yet-produced upstream) -- validation is skipped then and the real read surfaces its own error.
    /// Only positions within the overlap are checked: a shorter/longer file column count is DuckDB's own
    /// (loud) count-mismatch error at scan time.</summary>
    private static void ValidateHeaderMatchesContract(
        string path, string dataset, IReadOnlyDictionary<string, string> declared, char delimiter)
    {
        if (!File.Exists(path))
        {
            return;
        }

        string[] header;
        try
        {
            header = ReadOrderedHeader(path, delimiter);
        }
        catch (Exception)
        {
            return; // unreadable header: let the actual read surface the real error rather than masking it
        }

        var declaredNames = declared.Keys.ToArray();
        for (var i = 0; i < declaredNames.Length && i < header.Length; i++)
        {
            if (!string.Equals(declaredNames[i], header[i], StringComparison.Ordinal))
            {
                throw new PzConnectorException(
                    $"dataset '{dataset}': columns: contract declares '{declaredNames[i]}' at position {i + 1}, " +
                    $"but the CSV header there is '{header[i]}' (file header: {string.Join(",", header)}). A native " +
                    $"csv read binds the contract to the file by position, so this would silently load '{header[i]}' " +
                    $"values under '{declaredNames[i]}' -- reorder the columns: contract to match the file header, " +
                    "or remove it to let DuckDB auto-detect the schema",
                    isTransient: false);
            }
        }
    }

    private static string[] ReadOrderedHeader(string path, char? delimiter = null)
    {
        using var textReader = new StreamReader(path);
        using var csv = CsvDataReader.Create(textReader, ReaderOptions(delimiter));
        var names = new string[csv.FieldCount];
        for (var i = 0; i < csv.FieldCount; i++)
        {
            names[i] = csv.GetName(i);
        }

        return names;
    }

    /// <summary>How many readers a large file may be split across. No YAML knob: this is not a property
    /// of the data the way postgres's `partitions:` is (which decides which ROWS each reader gets), just
    /// how many cores to point at one file, so it follows the machine — the same reasoning as the
    /// streaming fan-out's <c>Environment.ProcessorCount</c> gate in <c>SourceLoadExecutor</c>. Capped
    /// because the readers all funnel into one bounded channel and one serialized DuckDB ingest, so past
    /// a handful they queue rather than help.</summary>
    private static readonly int MaxSplitPartitions = Math.Min(Environment.ProcessorCount, 8);

    /// <summary>One partition per readable byte range: a single whole-file partition when the file
    /// is small or cannot be proven safe to split, otherwise one per <see cref="CsvSplit"/> (see
    /// <see cref="CsvSplitPlan"/> for what "proven" means and what it costs).
    ///
    /// Splitting makes row order across the node non-deterministic — the partitions race through the one
    /// bounded channel — where an unsplit read landed rows in file order. That is already true of this
    /// dataset's OTHER tier: DuckDB's <c>read_csv</c> parallelizes the same file and makes the same
    /// trade, and the engine has never guaranteed cross-partition order (see
    /// <c>SourceLoadExecutor.PumpPartitionsAsync</c>). Small files, which is every fixture and sample,
    /// stay single-partition and therefore stay byte-for-byte ordered.</summary>
    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        var columns = GetColumnsContract(spec);
        var format = GetFormat(spec);
        var path = ResolvePath(spec);
        var delimiter = DelimiterOf(spec);

        var plan = TryPlanSplits(path, delimiter);
        if (plan is null)
        {
            IReadOnlyList<IDatasetPartition> single = [new CsvPartition(path, columns, delimiter: delimiter)];
            return new ValueTask<IReadOnlyList<IDatasetPartition>>(single);
        }

        var partitions = new IDatasetPartition[plan.Splits.Count];
        for (var i = 0; i < partitions.Length; i++)
        {
            partitions[i] = new CsvPartition(path, columns, plan, plan.Splits[i], partitions.Length, delimiter);
        }

        return new ValueTask<IReadOnlyList<IDatasetPartition>>(partitions);
    }

    /// <summary>Reads the file's own header (the split planner needs it to prove the delimiter) and asks
    /// for a split plan. Every failure mode here — an absent file, an unreadable header, a file too small
    /// — means "don't split", never an error: the real read is what surfaces a genuine problem, with the
    /// message it always had. A non-comma DECLARED delimiter never splits: <see cref="CsvSplitPlanner"/>
    /// proves its boundaries by scanning for comma-quoted fields specifically, so it stays single-partition
    /// for tsv and any other delimiter, same as a file it cannot otherwise prove safe. The header read
    /// below deliberately still auto-detects (<see cref="ReadOrderedHeader"/>'s null default) rather than
    /// trusting the declared comma: that is the independent proof that the file's bytes really are comma
    /// csv, not just a mislabeled one.</summary>
    private static CsvSplitPlan? TryPlanSplits(string path, char delimiter)
    {
        if (delimiter != ',')
        {
            return null;
        }

        if (!File.Exists(path) || new FileInfo(path).Length < CsvSplitPlanner.MinBytesPerPartition * 2)
        {
            return null;
        }

        string[] header;
        try
        {
            header = ReadOrderedHeader(path);
        }
        catch (Exception)
        {
            return null;
        }

        return CsvSplitPlanner.TryPlan(path, header, MaxSplitPartitions);
    }

    public ValueTask DisposeAsync() => default;

    /// <summary>The entity names the file, and <c>path:</c> overrides that when the layout does not
    /// match the name. A source needs the extension its format implies; the sink writes a directory, so
    /// it does not. An absolute <c>path:</c> ignores the connection's location entirely.</summary>
    private string ResolvePath(DatasetSpec spec)
    {
        var relative = spec.Options.TryGetValue("path", out var value) && value?.ToString() is { Length: > 0 } p
            ? p
            : $"{spec.Dataset}.{GetFormat(spec)}";

        return Path.IsPathRooted(relative) ? relative : Path.Combine(baseDir, relative);
    }

    private static string GetFormat(DatasetSpec spec) =>
        FileFormatCatalog.Resolve(spec.Options, "csv", "localfiles", $"dataset '{spec.Dataset}'").Extension;

    /// <summary>The declared `columns:` contract, injected by <c>SourceLoadExecutor</c> into
    /// <see cref="DatasetSpec.Options"/>["columns"]. Missing contract is a permanent failure: this path
    /// does no type inference.</summary>
    private static IReadOnlyDictionary<string, string> GetColumnsContract(DatasetSpec spec) =>
        ExtractColumns(spec) ?? throw new PzConnectorException(
            $"dataset '{spec.Dataset}': localfiles csv requires a declared columns: contract in v0",
            isTransient: false);

    /// <summary>Same lookup as <see cref="GetColumnsContract"/> but returns null instead of throwing —
    /// used by <see cref="TryGetNativeScan"/> to tell no-contract-at-all from partial-or-full, since
    /// native scan does not require a contract at all. The universal path
    /// (<see cref="GetColumnsContract"/>) still requires a full contract.</summary>
    private static IReadOnlyDictionary<string, string>? ExtractColumns(DatasetSpec spec) =>
        spec.Options.TryGetValue("columns", out var value) && value is IReadOnlyDictionary<string, string> { Count: > 0 } columns
            ? columns
            : null;
}

/// <summary>One readable slice of a CSV dataset: the whole file, or — when <see cref="CsvSplitPlan"/>
/// proved the file safe to cut — one byte range of it, read through a <see cref="CsvSliceStream"/> that
/// splices the header in front so this code reads a normal headed CSV either way. This universal path
/// deliberately never consults <see cref="DatasetSpec.WatermarkCursor"/>/
/// <see cref="DatasetSpec.WatermarkUpperBound"/> at all -- windowed LocalFiles datasets require the
/// native-scan tier (<see cref="CsvSource.TryGetNativeScan"/>), which is the only place the bound is
/// pushed down. Under <c>engine.force_universal</c>, this path still lands the WHOLE file every run, but
/// that stays correct: the engine's candidate-cap rule always clamps the watermark
/// candidate to <c>Min(landedMax, windowUpper)</c>, so an over-extracting universal read can never advance
/// the cursor past the window it should have stopped at -- it merely re-reads more than necessary.</summary>
internal sealed class CsvPartition(
    string path,
    IReadOnlyDictionary<string, string> columns,
    CsvSplitPlan? plan = null,
    CsvSplit split = default,
    int partitionCount = 1,
    char delimiter = ',') : IDatasetPartition
{
    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        if (!File.Exists(path))
        {
            throw new PzConnectorException($"csv file not found: '{path}'", isTransient: false);
        }

        var names = columns.Keys.ToArray();
        var typeNames = columns.Values.ToArray();
        var fields = new Field[names.Length];
        for (var i = 0; i < names.Length; i++)
        {
            fields[i] = TypeNameMap.ToArrowField(names[i], typeNames[i]);
        }

        var schema = new Schema(fields, null);

        using var textReader = plan is null
            ? new StreamReader(path)
            : new StreamReader(new CsvSliceStream(path, plan.Header, split.Start, split.End));
        using var csv = await CsvDataReader.CreateAsync(
            textReader, CsvSource.ReaderOptions(delimiter), ct).ConfigureAwait(false);

        var ordinals = new int[names.Length];
        for (var i = 0; i < names.Length; i++)
        {
            // Sylvan's GetOrdinal throws IndexOutOfRangeException (rather than returning -1) for an
            // unknown column name; translate it into a named, permanent PzConnectorException.
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
                    $"csv file '{path}': missing declared column '{names[i]}' in header", isTransient: false);
            }

            ordinals[i] = ordinal;
        }

        // Split reads divide the batch-size target between them, so N concurrent readers hold about as
        // much in flight as the single reader did rather than N times as much -- the engine sizes that
        // target for the node, not per partition, and the ingest side is indifferent to batch size
        // (measured flat from 10k to 1M rows per batch).
        var batchOptions = plan is null
            ? options
            : options with { TargetBatchBytes = Math.Max(1, options.TargetBatchBytes / partitionCount) };

        // The per-cell parse/append lives in CsvArrowReader: one column writer per contract column reading
        // Sylvan's own char buffer, instead of a string plus a box per cell (see its doc comment). The
        // null policy it applies is this path's: both a missing/empty cell and an explicit
        // quoted empty string ("") are NULL for every column type, including varchar — so a varchar
        // column can never round-trip an actual empty string value, only NULL.
        await foreach (var batch in CsvArrowReader
            .ReadAsync(csv, schema, typeNames, ordinals, path, batchOptions, split.RowNumberOffset, ct)
            .ConfigureAwait(false))
        {
            yield return batch;
        }
    }
}
