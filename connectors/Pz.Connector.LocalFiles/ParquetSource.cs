using Apache.Arrow;
using Parquet;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.LocalFiles;

/// <summary>Parquet source: native-only, mirroring the S3 sink's native-only shape.
/// <see cref="TryGetNativeScan"/> always succeeds -- parquet is self-describing, so
/// (unlike <see cref="CsvSource"/>) no declared <c>columns:</c> contract is required.
/// <see cref="PlanReadAsync"/> (the universal batch path) has no implementation at all: it always
/// throws, because there is nothing to fall back to.
///
/// Incremental interplay -- an honest limitation for PLAIN/unwindowed incremental; see
/// <see cref="TryGetNativeScan"/> below for the windowed case: a parquet dataset
/// MAY declare <c>incremental: {cursor}</c>. Capture and commit-gated advancement (<c>SourceLoadExecutor</c>'s
/// <c>CaptureWatermarkAsync</c>) run against whatever landed in the staging table, tier-agnostically --
/// so the watermark DOES advance correctly and downstream merge dedup still gives effectively-once
/// correctness. But for a PLAIN (unwindowed) incremental dataset, <see cref="TryGetNativeScan"/> never
/// looks at <see cref="DatasetSpec.WatermarkCursor"/>/<see cref="DatasetSpec.WatermarkValue"/> (parity
/// with every other connector's native scan for the unbounded case -- none of them pushes an unbounded
/// watermark down into the scan SQL), so every run re-reads the entire file. Net effect: correctness
/// without extraction savings. This is deliberate, not a bug -- see
/// <c>Parquet_incremental_captures_but_does_not_pushdown</c>. A WINDOWED dataset (<see
/// cref="DatasetSpec.WatermarkUpperBound"/> also set) is different -- see <see cref="TryGetNativeScan"/>.</summary>
internal sealed class ParquetSource(string baseDir) : ISource
{
    public async ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var path = ResolvePath(spec);
        if (!File.Exists(path))
        {
            throw new PzConnectorException($"parquet file not found: '{path}'", isTransient: false);
        }

        var footer = await ParquetReader.ReadSchemaAsync(path).ConfigureAwait(false);
        var fields = footer.GetDataFields()
            .Select(f => TypeNameMap.ToArrowField(f.Name, ParquetTypeMap.ToV0TypeName(f)))
            .ToArray();
        return new DatasetSchema(new Schema(fields, null));
    }

    /// <summary>Always succeeds: parquet is self-typing, so there is no contract to require and no
    /// reason to ever decline. A windowed dataset's <see cref="DatasetSpec.WatermarkCursor"/> +
    /// <see cref="DatasetSpec.WatermarkUpperBound"/> pair is pushed into the fragment via
    /// <see cref="LocalFilesWindowSql.Wrap"/> (the same seam <see cref="CsvSource.TryGetNativeScan"/>
    /// extends) -- but plain (unwindowed) incremental --
    /// <see cref="DatasetSpec.WatermarkCursor"/>/<see cref="DatasetSpec.WatermarkValue"/> set alone, no
    /// upper bound -- is deliberately never consulted: ignoring it is always correct (merge dedups), so
    /// every non-windowed run re-reads the entire file. Net effect: correctness without extraction
    /// savings UNLESS the dataset is windowed.</summary>
    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        var context = $"dataset '{spec.Dataset}'";
        var format = FileFormatCatalog.Resolve(spec.Options, "parquet", "localfiles", context);
        var absPath = ResolvePath(spec);
        var urlArg = $"'{EscapeSqlLiteral(absPath)}'";
        var request = new FormatReadRequest(urlArg, 1, null, TypeNameMap.ToDuckDbName);
        var fragment = FileFormatCatalog.ReadFragment(format, spec.Options, request, context);
        scan = new NativeScan(LocalFilesWindowSql.Wrap(fragment, spec), FileFormatCatalog.SetupStatements(format))
        {
            Mechanism = FileFormatCatalog.ReadMechanism(format),
        };
        return true;
    }

    /// <summary>The universal path: there is no managed parquet reader in v0, so this
    /// always throws -- reached whenever the planner has no native strategy for this edge, including
    /// under <c>engine.force_universal</c>. The "PZ0312" prefix follows the convention for surfacing a
    /// specific code through <c>KindDispatchingExecutor</c>'s generic PZ0501 node-failure wrap (as
    /// <c>Pz.Engine.Execution.NativeSetup</c> surfaces its PZ0311 through the message text). Unlike
    /// NativeSetup -- which interpolates the live <c>PzErrorCode</c> constant -- connector projects
    /// deliberately do not depend on Pz.Core, so the code is duplicated here as a BARE STRING: this is the
    /// same literal-duplication drift risk Pz.PackageManagement carries (documented in PzErrorCode.cs), so
    /// if PZ0312's value ever changes this string must be updated by hand.</summary>
    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: dataset '{spec.Dataset}': localfiles parquet source is native-scan only; it cannot run " +
            "on the universal tier (remove engine.force_universal, or use csv)", isTransient: false);

    public ValueTask DisposeAsync() => default;

    /// <summary>The entity names the file, and <c>path:</c> overrides that when the layout does not
    /// match the name. A source needs the extension its format implies; the sink writes a directory, so
    /// it does not. An absolute <c>path:</c> ignores the connection's location entirely.</summary>
    private string ResolvePath(DatasetSpec spec)
    {
        var relative = spec.Options.TryGetValue("path", out var value) && value?.ToString() is { Length: > 0 } p
            ? p
            : $"{spec.Dataset}.{"parquet"}";

        return Path.IsPathRooted(relative) ? relative : Path.Combine(baseDir, relative);
    }

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");
}
