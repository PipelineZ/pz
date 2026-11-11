using System.Globalization;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Paths;
using Pz.Core.Incremental;
using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Dag;

/// <summary>Compile-time checks on the loaded connections and their entities: merge keys, incremental
/// cursors and windows, templated paths, partitioning. Each aggregates its findings into the caller's list.</summary>
public static partial class DagCompiler
{
    private const string PathTokenHint =
        "use a coarse→fine, contiguous run of {yyyy} {MM} {dd} {HH} {mm} (e.g. {yyyy}/{MM}/{dd})";

    /// <summary>A merge output needs key columns; keys on any other strategy are a mistake.</summary>
    private static void CheckMergeKeys(PzProject project, List<PzError> errors)
    {
        foreach (var sink in project.Connections)
        {
            foreach (var output in sink.Outputs)
            {
                var isMerge = output.Mode == "merge";
                if (isMerge && output.Keys.Count == 0)
                {
                    errors.Add(new PzError(PzErrorCode.MergeRequiresKeys,
                        $"sink '{sink.Name}.{output.Name}' has strategy: 'merge' but declares no keys -- " +
                        "merge needs at least one key column to match rows by",
                        sink.FilePath, null,
                        $"sink('{sink.Name}', '{output.Name}', strategy: 'merge', keys: ['<column>'])"));
                }
                else if (!isMerge && output.Keys.Count > 0)
                {
                    errors.Add(new PzError(PzErrorCode.KeysWithoutMerge,
                        $"sink '{sink.Name}.{output.Name}' declares keys: [{string.Join(", ", output.Keys)}] " +
                        $"but strategy is '{output.Mode}', not merge -- keys only applies to strategy: 'merge'",
                        sink.FilePath, null,
                        $"sink('{sink.Name}', '{output.Name}', strategy: 'merge', " +
                        $"keys: ['{string.Join("', '", output.Keys)}']), or drop keys"));
                }
            }
        }
    }

    /// <summary>The incremental-cursor and bounded-window rules, source by source, dataset by dataset.
    /// Returns the connections with each successfully canonicalized window written back; the caller swaps
    /// them into the project once every check passes.</summary>
    private static List<ConnectionDef> CheckIncrementalSources(
        PzProject project, List<string>? notices, List<PzError> errors)
    {
        // The bounded-window rules (PZ0213) need the very same `declaredType` lookup the cursor check
        // (PZ0212) computes, so they share this pass. Datasets whose Incremental canonicalizes
        // successfully are collected into `rewrittenConnections`, which the caller swaps into `project`
        // only once every check is error-free, so every later stage -- including the SourceLoad node
        // builder -- sees canonical Initial/Until instead of raw user variants. NodeId hashing never
        // reads Incremental, so this rewrite cannot change any NodeId.
        var rewrittenConnections = new List<ConnectionDef>(project.Connections.Count);
        foreach (var source in project.Connections)
        {
            List<DatasetDef>? rewrittenDatasets = null;
            for (var di = 0; di < source.Datasets.Count; di++)
            {
                var dataset = source.Datasets[di];
                // SyncModeDef carries exactly one Mode, so declaring two read modes is not representable
                // at the loader and needs no check here (see PzErrorCode.SyncStateConflict).
                if (dataset.SyncMode?.Incremental is not { } incremental)
                {
                    continue;
                }

                // A declared-descending feed with a page
                // cap can truncate mid-slice, and MAX(cursor) advancement would skip the unfetched
                // middle -- refuse at compile. Undeclared ordering + a cap only warns: the runtime
                // guard (HttpPartition) decides by observation. Reads `cursor_order`/`max_pages`
                // through the same option convention CursorContract documents for cursor/cursor_type.
                var cursorOrder = dataset.Options.TryGetValue("cursor_order", out var cursorOrderObj)
                    ? cursorOrderObj?.ToString()
                    : null;
                if (dataset.Options.ContainsKey("max_pages"))
                {
                    if (cursorOrder == "desc")
                    {
                        errors.Add(new PzError(PzErrorCode.DescendingCursorTruncatable,
                            $"source '{source.Name}.{dataset.Name}': 'max_pages' on a descending " +
                            "incremental feed (cursor_order: desc) — a truncated crawl would advance " +
                            "the watermark past unfetched rows", source.FilePath, null,
                            "remove max_pages so slices run to completion, or point at an ascending endpoint"));
                    }
                    else if (cursorOrder is null)
                    {
                        notices?.Add($"source '{source.Name}.{dataset.Name}': 'max_pages' with " +
                            "undeclared cursor ordering — a descending feed fails at run time if a " +
                            "crawl truncates; declare cursor_order to check this at compile time");
                    }
                }

                var cursor = incremental.Cursor;
                string? declaredType = null;
                if (dataset.Columns is not { Count: > 0 } columns)
                {
                    notices?.Add(
                        $"source '{source.Name}.{dataset.Name}': cursor '{cursor}' unverified until --connect / first run");
                    // Raw-envelope connectors (http) type the cursor via `cursor`/`cursor_type`
                    // dataset options instead of a columns: contract — accept that route for the
                    // window math below (CursorContract documents the convention).
                    declaredType = CursorContract.ResolveDeclaredType(dataset);
                }
                else if (!columns.TryGetValue(cursor, out var cursorType))
                {
                    // A windowed dataset (max_window declared) hits the
                    // very same `declaredType is null` condition in the window rules below, which report PZ0213
                    // for this exact root cause -- PZ0213 subsumes PZ0212 here so the dataset gets ONE
                    // error, not two codes naming the same misconfiguration.
                    if (incremental.MaxWindow is null)
                    {
                        errors.Add(new PzError(PzErrorCode.CursorInvalid,
                            $"source '{source.Name}.{dataset.Name}' declares incremental cursor '{cursor}' but it is " +
                            $"not in the dataset's columns: contract -- allowed cursor types: {CursorContract.AllowedTypesText}",
                            source.FilePath, null,
                            $"add '{cursor}' to columns: with one of the allowed cursor types ({CursorContract.AllowedTypesText})"));
                    }
                }
                else if (Array.IndexOf(CursorContract.AllowedTypes, cursorType) < 0)
                {
                    // Same subsumption rationale as above.
                    if (incremental.MaxWindow is null)
                    {
                        errors.Add(new PzError(PzErrorCode.CursorInvalid,
                            $"source '{source.Name}.{dataset.Name}' declares incremental cursor '{cursor}' as " +
                            $"'{cursorType}', which cannot be used as a cursor -- allowed cursor types: {CursorContract.AllowedTypesText}",
                            source.FilePath, null,
                            $"declare '{cursor}' as one of the allowed cursor types ({CursorContract.AllowedTypesText})"));
                    }
                }
                else
                {
                    declaredType = cursorType;
                }

                // Only fires when the window trio is present; a plain `incremental: {cursor: ...}`
                // dataset is untouched. All errors aggregate as PZ0213 naming
                // the file + dataset. On success, Initial/Until are REWRITTEN into canonical form so
                // the engine never re-parses user variants.
                if (incremental.MaxWindow is null && incremental.Initial is null && incremental.Until is null)
                {
                    continue;
                }

                var windowErrors = new List<string>();

                if (incremental.MaxWindow is null)
                {
                    windowErrors.Add("'initial'/'until' require 'max_window' — alone they configure nothing");
                }

                if (incremental.MaxWindow is not null && incremental.Initial is null)
                {
                    windowErrors.Add(
                        "'max_window' requires 'initial' — a windowed dataset must declare where the backfill starts");
                }

                if (incremental.MaxWindow is not null && declaredType is null)
                {
                    windowErrors.Add($"'max_window' requires cursor '{cursor}' to be declared in columns: (or, for " +
                        "a raw-envelope dataset, via the connector's cursor:/cursor_type: options) with an " +
                        $"allowed cursor type ({CursorContract.AllowedTypesText}) — bounds must be computable before the first extraction");
                }

                // Scalar `query` = SQL-text query mode (postgres/sqlserver). A MAP-valued `query` is
                // the http connector's URL-parameter block — the very thing that pushes the window
                // bounds ({{ watermark }}/{{ window_upper }}) — and is fine to window.
                if (incremental.MaxWindow is not null
                    && dataset.Options.TryGetValue("query", out var queryOption) && queryOption is string)
                {
                    windowErrors.Add(
                        "'max_window' is not supported on a query-mode dataset — query mode ignores pushdown, " +
                        "so a windowed query dataset would silently extract everything; use table: or drop max_window");
                }

                string? canonicalInitial = null;
                string? canonicalUntil = null;
                if (declaredType is not null)
                {
                    if (incremental.MaxWindow is not null &&
                        !WindowMath.TryValidateWindow(declaredType, incremental.MaxWindow, out var windowError))
                    {
                        windowErrors.Add(windowError!);
                    }

                    if (incremental.Initial is not null &&
                        !WindowMath.TryCanonicalize(declaredType, incremental.Initial, out canonicalInitial))
                    {
                        // TryCanonicalize's `out` still assigns "" (not null) on failure -- null it back
                        // out so the Compare guard below correctly treats this as "no canonical value".
                        canonicalInitial = null;
                        windowErrors.Add($"'initial' value '{incremental.Initial}' is not a canonical {declaredType} value");
                    }

                    if (string.Equals(incremental.Until, WindowMath.UntilNow, StringComparison.Ordinal))
                    {
                        // Resolved per run to the time it started (SourceLoadExecutor), so nothing to canonicalize.
                        if (!WindowMath.SupportsNow(declaredType))
                        {
                            windowErrors.Add($"'until: now' needs a date or timestamp cursor, not {declaredType}");
                        }
                    }
                    else if (incremental.Until is not null &&
                        !WindowMath.TryCanonicalize(declaredType, incremental.Until, out canonicalUntil))
                    {
                        canonicalUntil = null;
                        windowErrors.Add($"'until' value '{incremental.Until}' is not a canonical {declaredType} value");
                    }

                    if (canonicalInitial is not null && canonicalUntil is not null &&
                        WindowMath.Compare(declaredType, canonicalUntil, canonicalInitial) <= 0)
                    {
                        windowErrors.Add("'until' must be greater than 'initial'");
                    }
                }

                foreach (var message in windowErrors)
                {
                    errors.Add(new PzError(PzErrorCode.WindowConfigInvalid,
                        $"source '{source.Name}.{dataset.Name}': {message}", source.FilePath, null,
                        "sync:\n  mode: incremental\n  cursor: <column>\n  max_window: 1d\n  initial: \"2020-01-01\"\n  until: \"<YYYY-MM-DD>\"  # optional"));
                }

                if (windowErrors.Count == 0 && (canonicalInitial is not null || canonicalUntil is not null))
                {
                    rewrittenDatasets ??= source.Datasets.ToList();
                    rewrittenDatasets[di] = dataset with
                    {
                        SyncMode = dataset.SyncMode! with
                        {
                            Incremental = incremental with
                            {
                                Initial = canonicalInitial ?? incremental.Initial,
                                Until = canonicalUntil ?? incremental.Until,
                            },
                        },
                    };
                }
            }

            rewrittenConnections.Add(rewrittenDatasets is null ? source : source with { Datasets = rewrittenDatasets });
        }
        return rewrittenConnections;
    }

    /// <summary>A date-templated source <c>path</c> must be well formed and backed by a bounded date cursor.</summary>
    private static void CheckSourcePathTemplates(PzProject project, List<PzError> errors)
    {
        // A dataset's `path` may embed calendar tokens ({yyyy}/{MM}/{dd}/{HH}/{mm}) substituted per-run
        // from the incremental watermark. Reads `dataset.Incremental` from `project.Connections` -- the
        // SAME pre-canonicalization view CheckIncrementalSources reads it from -- so this check's notion
        // of "bounded window" cannot disagree with PZ0213's. Source datasets only: write-side
        // `partition_by` renders per-row and needs no window (CheckPartitionedOutputs covers that config).
        foreach (var source in project.Connections)
        {
            foreach (var dataset in source.Datasets)
            {
                if (!dataset.Options.TryGetValue("path", out var pathObj) || pathObj?.ToString() is not { } path
                    || !PathTemplate.HasDateTokens(path))
                {
                    continue;
                }

                try
                {
                    PathTemplate.Validate(path, $"dataset '{dataset.Name}' in {source.FilePath}");
                }
                catch (PzConnectorException ex)
                {
                    errors.Add(new PzError(PzErrorCode.TemplatedPathTokensInvalid, ex.Message,
                        source.FilePath, null, PathTokenHint));
                    // Fall through: still check the cursor/window below -- aggregate-all-errors rule.
                }

                var cursorType = CursorContract.ResolveDeclaredType(dataset);
                if (cursorType is not ("date" or "timestamp"))
                {
                    errors.Add(new PzError(PzErrorCode.TemplatedPathCursorInvalid,
                        $"dataset '{dataset.Name}' in {source.FilePath}: a date-templated path requires an " +
                        "incremental date cursor (type date or timestamp); add `sync: {mode: incremental, cursor: <col>}` " +
                        "naming a date/timestamp column",
                        source.FilePath, null,
                        "sync:\n  mode: incremental\n  cursor: <column>  # date or timestamp"));
                }
                else if (!HasBoundedWindow(dataset.SyncMode?.Incremental))
                {
                    // Only fires once the cursor is confirmed date/timestamp -- otherwise
                    // TemplatedPathCursorInvalid above already names the one root cause (one error per
                    // root cause).
                    errors.Add(new PzError(PzErrorCode.TemplatedPathWindowRequired,
                        $"dataset '{dataset.Name}' in {source.FilePath}: a date-templated path requires a " +
                        "bounded window so both watermark bounds are stamped on every run (including the " +
                        "first); add `initial:` and `max_window:` under `sync:`",
                        source.FilePath, null,
                        "sync:\n  mode: incremental\n  cursor: <column>\n  max_window: 1d\n  initial: \"2020-01-01\""));
                }
            }
        }
    }

    /// <summary><c>files_per_partition</c> must be a positive integer when present.</summary>
    private static void CheckFilesPerPartition(PzProject project, List<PzError> errors)
    {
        // The Azure connector coalesces that many consecutive matched blobs into one partition,
        // amortizing per-file dispatch/stream-open overhead for many-tiny-files datasets.
        foreach (var source in project.Connections)
        {
            foreach (var dataset in source.Datasets)
            {
                if (!dataset.Options.TryGetValue("files_per_partition", out var fppObj) || fppObj is null)
                {
                    continue;
                }

                if (!TryParsePositiveInt(fppObj, out _))
                {
                    errors.Add(new PzError(PzErrorCode.FilesPerPartitionInvalid,
                        $"source '{source.Name}.{dataset.Name}' in {source.FilePath}: files_per_partition " +
                        $"'{fppObj}' must be a positive integer",
                        source.FilePath, null,
                        "set files_per_partition to a positive integer, or remove it"));
                }
            }
        }
    }

    /// <summary>The write-side counterpart of <see cref="CheckSourcePathTemplates"/>: <c>partition_by</c> and
    /// templated output paths must agree.</summary>
    private static void CheckPartitionedOutputs(PzProject project, List<PzError> errors)
    {
        // `partition_by:` names the columns an output is partitioned by. WHAT THAT PRODUCES is the
        // destination's business, and only two things are checkable connector-agnostically here.
        //
        // (1) The declaration is well formed -- a column name, or a list of them. Reading it as
        // `.ToString() is { Length: > 0 }` is the trap this parse closes: a deserialized YAML list
        // stringifies to its CLR type name, which is non-empty, so the presence check passes and
        // everything downstream reads a column named "System.Collections.Generic.List`1[System.Object]".
        //
        // (2) It agrees with `path:`. Calendar tokens in the path are pz's OWN layout rule -- one
        // timestamp column rendered into a folder -- so tokens with no partition_by have no column to
        // substitute from, and tokens with several columns have no way to choose. Both are PZ0219.
        //
        // partition_by WITHOUT tokens is deliberately NOT refused here: a format that records its own
        // partitioning (Delta, Iceberg, Hive-layout parquet) has no templated path to route into and is
        // correct as written. Whether the target connector can actually honour it is a capability
        // question, and only the planner holds the connector instance -- so that refusal is PZ0314
        // there, exactly as the source-side templating refusal already is.
        //
        // Whether a named column EXISTS is impossible to check here -- OutputDef carries no declared
        // column set at compile time (its schema is the upstream pipeline's runtime result) -- so that
        // check is deferred to runtime; PZ0220 is not claimed here. Malformed token sequences reuse
        // PZ0218, same as the source side, aggregated into the same errors list.
        foreach (var sink in project.Connections)
        {
            foreach (var output in sink.Outputs)
            {
                var path = output.Options.TryGetValue("path", out var pathObj) ? pathObj?.ToString() : null;
                var hasDateTokens = path is not null && PathTemplate.HasDateTokens(path);

                if (!PartitionColumns.TryRead(output.Options, out var partitionColumns, out var problem))
                {
                    errors.Add(new PzError(PzErrorCode.PartitionedOutputConfigInvalid,
                        $"sink '{sink.Name}.{output.Name}' in {sink.FilePath} declares partition_by: but " +
                        problem,
                        sink.FilePath, null,
                        "set partition_by: to a column name, or to a list of column names"));
                }
                else if (partitionColumns.Count == 0 && hasDateTokens)
                {
                    errors.Add(new PzError(PzErrorCode.PartitionedOutputConfigInvalid,
                        $"sink '{sink.Name}.{output.Name}' in {sink.FilePath} has a date-templated path but " +
                        "declares no partition_by: -- there is no column to substitute the tokens from at write time",
                        sink.FilePath, null,
                        "add `partition_by: <column>` naming the column to partition by"));
                }
                else if (partitionColumns.Count > 1 && hasDateTokens)
                {
                    errors.Add(new PzError(PzErrorCode.PartitionedOutputConfigInvalid,
                        $"sink '{sink.Name}.{output.Name}' in {sink.FilePath} declares partition_by: with " +
                        $"{partitionColumns.Count} columns and a date-templated path -- calendar tokens render " +
                        "from exactly one timestamp column, so there is no way to choose between them",
                        sink.FilePath, null,
                        "name a single timestamp column in partition_by:, or remove the calendar tokens from path:"));
                }

                if (!hasDateTokens)
                {
                    continue;
                }

                try
                {
                    PathTemplate.Validate(path!, $"output '{output.Name}' in {sink.FilePath}");
                }
                catch (PzConnectorException ex)
                {
                    errors.Add(new PzError(PzErrorCode.TemplatedPathTokensInvalid, ex.Message,
                        sink.FilePath, null, PathTokenHint));
                }
            }
        }
    }

    /// <summary>The exact "bounded window" PZ0213 recognizes as valid: <c>max_window</c> and
    /// <c>initial</c> both set (<c>until</c> is an optional additional upper bound -- PZ0213's rules 1/2
    /// require max_window+initial together for ANY window config to be error-free, so this must mirror
    /// that pairing exactly rather than treat until as a max_window substitute).</summary>
    private static bool HasBoundedWindow(IncrementalDef? incremental) =>
        incremental is { MaxWindow: not null, Initial: not null };

    /// <summary>Parses a `files_per_partition` option value (int, long, or string form) to a positive
    /// <c>int</c>, per PZ0222's rule -- rejects anything that doesn't parse to an integer AND anything
    /// that parses to zero or negative. Deliberately narrow: this is a compile-time gate only, mirroring
    /// no existing numeric-option parser elsewhere in DagCompiler (there is none to reuse).</summary>
    private static bool TryParsePositiveInt(object? value, out int result)
    {
        switch (value)
        {
            case int i:
                result = i;
                return i > 0;
            case long l when l is > 0 and <= int.MaxValue:
                result = (int)l;
                return true;
            case long:
                result = 0;
                return false;
            case string s when int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed):
                result = parsed;
                return parsed > 0;
            default:
                result = 0;
                return false;
        }
    }
}
