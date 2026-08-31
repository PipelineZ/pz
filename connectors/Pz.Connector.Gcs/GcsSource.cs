using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Paths;

namespace Pz.Connector.Gcs;

/// <summary>Native-only gcs source: DuckDB's httpfs extension is the entire read
/// path — <c>read_parquet</c>/<c>read_csv</c>/<c>read_json</c> over <c>gs://</c> URLs with the
/// shared scoped secret (hmac interop keys — <see cref="GcsConnector"/> refuses to open a source on
/// any other auth method), the azure/s3 two-state contract model (declared `columns:` prunes via the
/// strict <c>columns = {…}</c> map; contract-less auto-detects with
/// <see cref="NativeScan.SchemaInferred"/>/<see cref="NativeScan.SniffFragment"/>),
/// <see cref="GcsWindowSql.Wrap"/> for windowed datasets, and the date-template watermark-window
/// cover (<see cref="PathTemplate.WindowCover"/>) emitting a DuckDB list literal so a query never
/// re-reads out-of-window partitions.
///
/// The native tier is SDK-free, so the control plane follows the s3/MySQL/sqlite precedent:
/// <see cref="GetSchemaAsync"/> answers from a declared contract or throws the clear native-only
/// refusal; <see cref="PlanReadAsync"/> is the PZ0312 refusal stub. Like the other file connectors,
/// the plain unwindowed watermark is deliberately NOT pushed down (re-reading a file set is merely
/// wasteful; the engine re-filters).</summary>
internal sealed class GcsSource(ConnectorConfig config) : ISource
{
    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var columns = ExtractColumns(spec) ?? throw new PzConnectorException(
            $"dataset '{spec.Dataset}': gcs is native-only with no offline schema probe -- " +
            "declare a columns: contract to validate shape, or skip --connect for this dataset",
            isTransient: false);

        var fields = columns.Select(kv => GcsTypeNameMap.ToArrowField(kv.Key, kv.Value)).ToArray();
        return new ValueTask<DatasetSchema>(new DatasetSchema(new Schema(fields, null)));
    }

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        var format = GetFormat(spec);
        var (bucket, key) = ResolveLocation(spec);
        var keyPatterns = CoverKeys(key, spec);
        var urlList = string.Join(", ", keyPatterns.Select(k => $"'gs://{GcsSql.Esc(bucket)}/{GcsSql.Esc(k)}'"));
        var urlArg = keyPatterns.Count == 1 ? urlList : $"[{urlList}]";
        string fragment;
        string mechanism;
        var schemaInferred = false;
        string? sniffFragment = null;

        if (format == "csv")
        {
            // The azure/localfiles two-state model: a declared columns: contract — partial or full,
            // this method cannot tell them apart without reading the file, which it deliberately
            // never does — renders the strict columns map (auto_detect = false); only a fully
            // contract-less dataset auto-detects.
            var declared = ExtractColumns(spec);
            if (declared is { Count: > 0 })
            {
                fragment = $"read_csv({urlArg}, header = true, auto_detect = false, columns = {{{ColumnsMap(declared)}}})";
            }
            else
            {
                fragment = $"read_csv({urlArg}, header = true, auto_detect = true)";
                schemaInferred = true;
                if (keyPatterns.Count == 1)
                {
                    // Single-key reads only (the azure rule): a multi-key window cover would sniff
                    // just one member file and claim a verdict for the set.
                    sniffFragment = $"sniff_csv({urlList})";
                }
            }

            mechanism = "read_csv";
        }
        else if (format == "json")
        {
            // read_json has no `types=` named parameter in the bundled DuckDB, so both states render
            // the same shapes azure/s3 do.
            var declared = ExtractColumns(spec);
            if (declared is { Count: > 0 })
            {
                fragment = $"read_json({urlArg}, columns = {{{ColumnsMap(declared)}}}, format = 'newline_delimited')";
            }
            else
            {
                fragment = $"read_json({urlArg}, auto_detect = true, format = 'newline_delimited')";
                schemaInferred = true;
            }

            mechanism = "read_json";
        }
        else
        {
            fragment = $"read_parquet({urlArg})";
            mechanism = "read_parquet";
        }

        scan = new NativeScan(
            GcsWindowSql.Wrap(fragment, spec),
            GcsSql.SetupStatements(config, GcsSql.SourceSecretName(spec.Source)))
        {
            Mechanism = mechanism,
            SchemaInferred = schemaInferred,
            SniffFragment = sniffFragment,
        };
        return true;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: dataset '{spec.Dataset}': gcs reads are native-scan only; they cannot run on the " +
            "universal tier (remove engine.force_universal / files_per_partition)", isTransient: false);

    public ValueTask DisposeAsync() => default;

    /// <summary>The dataset's (bucket, key) under the ratified `root:` composition (the same one the
    /// sink uses): bucket = the dataset's own `bucket:` else the root's; the key = root prefix +
    /// `path:` — and a read with no
    /// `path:` is <c>&lt;prefix&gt;/&lt;entity&gt;.&lt;format&gt;</c>. A dataset naming its OWN bucket
    /// (different from the root's) does not inherit the root prefix.</summary>
    private (string Bucket, string Key) ResolveLocation(DatasetSpec spec)
    {
        var (rootBucket, rootPrefix) = GcsSql.SplitRoot(config.GetString("root"));
        var bucket = spec.Options.TryGetValue("bucket", out var b) && b?.ToString() is { Length: > 0 } named
            ? named
            : rootBucket ?? throw new PzConnectorException(
                $"dataset '{spec.Dataset}': gcs needs a 'root' on the connection or a 'bucket' option",
                isTransient: false);
        var path = spec.Options.TryGetValue("path", out var p) && p?.ToString() is { Length: > 0 } given
            ? given.Trim('/')
            : $"{spec.Dataset}.{GetFormat(spec)}";
        var prefix = rootBucket is null || (spec.Options.ContainsKey("bucket") && rootBucket != bucket)
            ? ""
            : rootPrefix;
        return (bucket, GcsSql.Join(prefix, path));
    }

    /// <summary>The bucket-relative key pattern(s) to scan: the watermark-window minimal cover when
    /// the path is date-templated and both bounds are present, else the single literal key. Pure — no
    /// I/O (the AzureSource shape).</summary>
    private static IReadOnlyList<string> CoverKeys(string key, DatasetSpec spec)
    {
        if (!PathTemplate.HasDateTokens(key) || spec.WatermarkValue is null || spec.WatermarkUpperBound is null)
        {
            return [key];
        }

        var lo = PathTemplate.ParseCanonical(spec.WatermarkValue);
        var hi = PathTemplate.ParseCanonical(spec.WatermarkUpperBound);
        return PathTemplate.WindowCover(key, lo, hi);
    }

    private static string ColumnsMap(IReadOnlyDictionary<string, string> declared) =>
        string.Join(", ", declared.Select(c => $"'{GcsSql.Esc(c.Key)}': '{GcsTypeNameMap.ToDuckDbName(c.Value, c.Key)}'"));

    private static string GetFormat(DatasetSpec spec) =>
        spec.Options.TryGetValue("format", out var f) && f?.ToString() is { Length: > 0 } format
            ? format
            : "parquet";

    private static IReadOnlyDictionary<string, string>? ExtractColumns(DatasetSpec spec) =>
        spec.Options.TryGetValue("columns", out var value) &&
        value is IReadOnlyDictionary<string, string> { Count: > 0 } columns
            ? columns
            : null;
}
