using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Loading;

/// <summary>Pipeline SQL files and their sidecar configs (materialization, tags, checks, redacted columns).</summary>
public static partial class ProjectLoader
{
    /// <summary>Warns once, naming the file, when <paramref name="yamlPath"/> (a <c>.yaml</c> spelling
    /// of a location pz only ever reads as <c>.yml</c>) exists -- regardless of whether the <c>.yml</c>
    /// it was probably meant to be also exists. pz cannot tell a stray unrelated file (a CI config, a
    /// k8s manifest) from a typo'd extension, only flag that the file sits where it would never be
    /// read.</summary>
    private static void WarnIfYamlSiblingIgnored(string projectDir, string yamlPath, List<PzWarning> warnings)
    {
        if (!File.Exists(yamlPath))
        {
            return;
        }

        var relativePath = RelativePath(projectDir, yamlPath);
        warnings.Add(new PzWarning(PzErrorCode.YamlExtensionIgnored,
            $"{relativePath} exists, but pz only ever reads the '.yml' extension here -- this file is " +
            "never loaded.",
            relativePath, null,
            $"rename {relativePath} to end in .yml, or delete it if it is unrelated to pz"));
    }

    private static List<PipelineDef> LoadPipelines(string projectDir, List<PzError> errors, List<PzWarning> warnings)
    {
        var pipelines = new List<PipelineDef>();
        var pipelinesDir = Path.Combine(projectDir, "pipelines");
        if (!Directory.Exists(pipelinesDir))
        {
            return pipelines;
        }

        var sqlFiles = Directory.EnumerateFiles(pipelinesDir, "*.sql", SearchOption.AllDirectories)
            .Where(f => !RelativePath(projectDir, f).Split('/').Contains("configs"))
            .OrderBy(f => f, StringComparer.Ordinal);

        // Case-insensitive: DuckDB folds an unquoted identifier's case, so `Orders.sql` and `orders.sql`
        // would stage to the very same `staging.orders` relation on Linux (where both files coexist on
        // disk) -- the first one that runs silently overwrites the other's result, node status green
        // throughout. Comparing file stems case-insensitively here catches that pair as the SAME
        // PZ0110 this dictionary already exists to report, rather than a mismatched row count nobody
        // can explain.
        var byName = new Dictionary<string, PipelineDef>(StringComparer.OrdinalIgnoreCase);
        var firstFileByName = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var filePath in sqlFiles)
        {
            var relativePath = RelativePath(projectDir, filePath);
            var pipelineName = Path.GetFileNameWithoutExtension(filePath);
            var rawSql = File.ReadAllText(filePath);

            // The file stem becomes staging.<name> verbatim (PipelineExecutor), a raw unquoted DuckDB
            // identifier -- checked before the duplicate lookup below so an invalid name is reported
            // once, on its own terms, rather than also tripping over whatever else happens to share its
            // casing.
            if (PzIdentifier.Problem(pipelineName) is { } problem)
            {
                errors.Add(new PzError(PzErrorCode.InvalidIdentifierName,
                    $"pipeline file stem '{pipelineName}' {problem}.", relativePath, null,
                    $"rename {relativePath} to pipelines/{PzIdentifier.Suggest(pipelineName)}.sql"));
                continue;
            }

            if (firstFileByName.TryGetValue(pipelineName, out var firstFile))
            {
                errors.Add(new PzError(
                    PzErrorCode.DuplicateName,
                    $"Duplicate pipeline name '{pipelineName}' defined in {firstFile} and {relativePath}.",
                    relativePath,
                    null,
                    "rename one of the pipelines so names are unique within the project " +
                    "-- pz's staging tables fold names case-insensitively, so names differing only by " +
                    "case cannot be told apart either."));
                continue;
            }

            firstFileByName[pipelineName] = relativePath;
            byName[pipelineName] = new PipelineDef(pipelineName, rawSql, "table", [], [], relativePath);
        }

        ApplySidecars(projectDir, byName, errors, warnings);

        pipelines.AddRange(byName.Values);
        return pipelines;
    }

    private static readonly string[] KnownSidecarKeys = ["pipeline", "materialization", "tags", "checks", "columns"];
    private static readonly string[] Materializations = ["table", "view", "ephemeral"];

    /// <summary>Absent -> <c>table</c>. A value outside <see cref="Materializations"/> -- dbt's
    /// `incremental`, a near-miss like `ephemral`, or the British spelling under the WRONG key
    /// (`materialisation:`, caught by <see cref="KnownSidecarKeys"/>'s own near-miss instead) -- is an
    /// error naming the enum, with a near-miss suggestion when the value is one edit away from an
    /// allowed one.</summary>
    private static string ParseMaterialization(Dictionary<string, object?> yaml, string pipelineName,
        string relativePath, List<PzError> errors)
    {
        if (TryGetString(yaml, "materialization") is not { } raw)
        {
            return "table";
        }

        if (Materializations.Contains(raw, StringComparer.Ordinal))
        {
            return raw;
        }

        var nearMiss = NearMiss.Find(Materializations, raw);
        errors.Add(new PzError(PzErrorCode.MaterializationInvalid,
            $"{relativePath}: pipeline '{pipelineName}': 'materialization' must be one of: " +
            $"{string.Join(", ", Materializations)} (got '{raw}')" +
            (nearMiss is null ? "" : $" -- did you mean '{nearMiss}'") + ".",
            relativePath, null, "materialization: table"));
        return "table";
    }

    /// <summary>A list of strings, same as always. A bare scalar (`tags: daily`) is dbt-shorthand
    /// accepted as the one-element list `[daily]` rather than silently dropped -- the value the author
    /// wrote was never ambiguous, only the container shape was informal. Anything else (a mapping) is
    /// an error naming the pipeline.</summary>
    private static List<string> GetTags(Dictionary<string, object?> yaml, string pipelineName,
        string relativePath, List<PzError> errors)
    {
        if (!yaml.TryGetValue("tags", out var value) || value is null or "")
        {
            return [];
        }

        if (value is List<object?> list)
        {
            return list.Select(t => t?.ToString() ?? string.Empty).ToList();
        }

        if (value is Dictionary<string, object?>)
        {
            errors.Add(new PzError(PzErrorCode.YamlShape,
                $"{relativePath}: pipeline '{pipelineName}': 'tags' must be a list of strings, or a " +
                "single string (got a mapping).",
                relativePath, null, "tags: [daily, crm]"));
            return [];
        }

        return [value.ToString() ?? string.Empty];
    }

    private static void ApplySidecars(string projectDir, Dictionary<string, PipelineDef> pipelines,
        List<PzError> errors, List<PzWarning> warnings)
    {
        var configsDir = Path.Combine(projectDir, "pipelines", "configs");
        if (!Directory.Exists(configsDir))
        {
            return;
        }

        foreach (var yamlPath in Directory.EnumerateFiles(configsDir, "*.yaml").OrderBy(f => f, StringComparer.Ordinal))
        {
            WarnIfYamlSiblingIgnored(projectDir, yamlPath, warnings);
        }

        foreach (var filePath in Directory.EnumerateFiles(configsDir, "*.yml").OrderBy(f => f, StringComparer.Ordinal))
        {
            var relativePath = RelativePath(projectDir, filePath);
            Dictionary<string, object?> yaml;
            try
            {
                yaml = YamlMapper.LoadFile(filePath, relativePath);
            }
            catch (PzConfigException ex)
            {
                errors.Add(ex.Error);
                continue;
            }

            RefuseUnknownKeys(yaml, KnownSidecarKeys, PzErrorCode.YamlShape, relativePath, $"{relativePath}: ",
                errors);

            var pipelineName = TryGetString(yaml, "pipeline") ?? string.Empty;

            if (!pipelines.TryGetValue(pipelineName, out var pipeline))
            {
                errors.Add(new PzError(
                    PzErrorCode.SidecarUnknownPipeline,
                    $"Sidecar config {relativePath} references unknown pipeline '{pipelineName}'.",
                    relativePath,
                    null,
                    "fix the pipeline: key or remove the sidecar."));
                continue;
            }

            var materialization = ParseMaterialization(yaml, pipelineName, relativePath, errors);
            var tags = GetTags(yaml, pipelineName, relativePath, errors);
            var redacted = GetRedactedColumns(yaml, pipelineName, relativePath, errors);

            var checks = new List<CheckDef>();
            // Indices (into `checks`) whose `column:` value was already reported as malformed --
            // ValidateChecks skips its own "requires exactly one 'column'" rule for these so the one
            // root cause (a bad `column:` value) doesn't also surface as a second PZ0113: one error per
            // root cause.
            var columnShapeErrorIndices = new HashSet<int>();
            foreach (var checkEntry in GetList(yaml, "checks"))
            {
                if (ParseCheckEntry(checkEntry, pipelineName, relativePath, errors, out var columnShapeError)
                    is not { } check)
                {
                    continue;
                }

                if (columnShapeError)
                {
                    columnShapeErrorIndices.Add(checks.Count);
                }

                checks.Add(check);
            }

            ValidateChecks(checks, pipelineName, relativePath, errors, columnShapeErrorIndices);

            pipelines[pipelineName] = pipeline with
            {
                Materialization = materialization,
                Tags = tags,
                Checks = checks,
                RedactedColumns = redacted,
            };
        }
    }

    /// <summary>One <c>checks:</c> list item as a <see cref="CheckDef"/>, or null (after reporting) when
    /// the item is not a single-key mapping. <paramref name="columnShapeError"/> is set when a malformed
    /// <c>column:</c> value was already reported, so <see cref="ValidateChecks"/> does not raise a second
    /// error for the columns it consequently lacks.</summary>
    private static CheckDef? ParseCheckEntry(object? checkEntry, string pipelineName, string relativePath,
        List<PzError> errors, out bool columnShapeError)
    {
        columnShapeError = false;
        if (checkEntry is not Dictionary<string, object?> checkDict || checkDict.Count == 0)
        {
            errors.Add(new PzError(PzErrorCode.InvalidCheck,
                $"pipeline '{pipelineName}': check entry must be a mapping like `- not_null: [id]`.",
                relativePath, null, "each check is a single-key mapping: `- <type>: <options>`"));
            return null;
        }

        if (checkDict.Count > 1)
        {
            var keys = string.Join(", ", checkDict.Keys.Order(StringComparer.Ordinal));
            errors.Add(new PzError(PzErrorCode.InvalidCheck,
                $"pipeline '{pipelineName}': one check per list item, got keys: {keys}.",
                relativePath, null, "split into separate list items"));
            return null;
        }

        var (checkType, checkValue) = checkDict.First();
        var checkValueDict = checkValue as Dictionary<string, object?>;

        // A check declared in dict form (e.g. `row_count: { min: 10 }`) may also carry
        // `columns:`, so not_null/unique can combine columns with `sample_values:`, e.g.
        // `not_null: { columns: [id], sample_values: false }`. The bare-list shape
        // (`not_null: [id, email]`) carries no sample_values and inherits the project default.
        // `columns`/`sample_values` are reserved keys, stripped from Options below so
        // row_count's flat `{min, max}` shape is unaffected by the reservation.
        var columns = checkValue is List<object?> columnsList
            ? columnsList.Select(c => c?.ToString() ?? string.Empty).ToList()
            : checkValueDict is not null && checkValueDict.TryGetValue("columns", out var columnsRaw)
                && columnsRaw is List<object?> dictColumnsList
                ? dictColumnsList.Select(c => c?.ToString() ?? string.Empty).ToList()
                : new List<string>();
        // `column:` singular (dbt parity, used by freshness/accepted_values) normalizes into
        // Columns so node naming and canonical
        // hashing treat it exactly like `columns:`. Declaring both is refused below.
        if (checkValueDict is not null && checkValueDict.TryGetValue("column", out var columnRaw))
        {
            if (columns.Count > 0)
            {
                errors.Add(new PzError(PzErrorCode.InvalidCheck,
                    $"pipeline '{pipelineName}': {checkType} check declares both 'column' and 'columns'.",
                    relativePath, null, "keep exactly one of the two keys."));
            }
            else if (columnRaw is string { Length: > 0 } singleColumn)
            {
                columns = [singleColumn];
            }
            else
            {
                errors.Add(new PzError(PzErrorCode.InvalidCheck,
                    $"pipeline '{pipelineName}': {checkType} check field 'column' must be a single column name.",
                    relativePath, null, "'column' must be a single column name, e.g. `column: updated_at`"));
                columnShapeError = true;
            }
        }

        var sampleValues = checkValueDict is not null ? TryGetBool(checkValueDict, "sample_values") : null;
        var options = checkValueDict is not null
            ? checkValueDict.Where(kv => kv.Key is not ("column" or "columns" or "sample_values"))
                .ToDictionary(kv => kv.Key, kv => kv.Value)
            : new Dictionary<string, object?>();

        return new CheckDef(checkType, columns, options, sampleValues);
    }

    private const string SidecarColumnsHint = "columns:\n  - name: email\n    redact: true";

    /// <summary>A sidecar's <c>columns:</c> list. It only marks columns of the pipeline's output for
    /// redaction in <c>pz preview</c>; it declares no types and enforces nothing, so a <c>type:</c> key is
    /// refused rather than silently ignored.</summary>
    private static List<string> GetRedactedColumns(Dictionary<string, object?> yaml, string pipelineName,
        string relativePath, List<PzError> errors)
    {
        var redacted = new List<string>();
        if (!yaml.TryGetValue("columns", out var raw) || raw is null or "")
        {
            return redacted;
        }

        PzError Shape(string message) => new(PzErrorCode.YamlShape,
            $"{relativePath}: pipeline '{pipelineName}': {message}.", relativePath, null, SidecarColumnsHint);

        if (raw is not List<object?> entries)
        {
            errors.Add(Shape("'columns' must be a list of { name, redact } entries"));
            return redacted;
        }

        var seen = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var item in entries)
        {
            if (item is not Dictionary<string, object?> entry
                || TryGetString(entry, "name") is not { Length: > 0 } name)
            {
                errors.Add(Shape("each 'columns' entry needs a 'name'"));
                continue;
            }

            if (entry.Keys.FirstOrDefault(k => k is not ("name" or "redact")) is { } unknown)
            {
                errors.Add(Shape($"column '{name}': '{unknown}' is not allowed; columns: here only marks " +
                                 "columns with redact: true"));
                continue;
            }

            if (!seen.TryAdd(name, name))
            {
                errors.Add(Shape($"column '{name}' repeats '{seen[name]}' (column names ignore case)"));
                continue;
            }

            if (entry.TryGetValue("redact", out var mark) && mark is not bool)
            {
                errors.Add(Shape($"column '{name}': 'redact' must be true or false"));
                continue;
            }

            if (mark is true)
            {
                redacted.Add(name);
            }
        }

        return redacted;
    }

    /// <summary>Aggregate compile-time validation of every
    /// check definition — unknown types, per-type option shapes, and unknown option keys are all
    /// PZ0113, reported together, naming the sidecar file and pipeline. Guarantees the executor
    /// relies on (exactly-one-column, parseable max_age, scalar values, named custom_sql) are
    /// established here, never re-checked at runtime.</summary>
    private static void ValidateChecks(List<CheckDef> checks, string pipelineName, string relativePath,
        List<PzError> errors, HashSet<int> columnShapeErrorIndices)
    {
        var customSqlNames = new HashSet<string>(StringComparer.Ordinal);
        var identities = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < checks.Count; i++)
        {
            var check = checks[i];
            void Add(string message, string hint) => errors.Add(new PzError(PzErrorCode.InvalidCheck,
                $"pipeline '{pipelineName}': {message}.", relativePath, null, hint));

            // Identical checks compile to one node id, which the DAG cannot hold twice. custom_sql is
            // excluded because its duplicate is reported by name below, and a check whose `column:`
            // was already refused has lost the columns that would tell it apart — one root cause,
            // one error.
            if (check.Type != "custom_sql" && !columnShapeErrorIndices.Contains(i) &&
                !identities.Add(CheckIdentity.Canonical(check)))
            {
                var target = check.Columns.Count > 0 ? $" on [{string.Join(", ", check.Columns)}]" : "";
                Add($"duplicate {check.Type} check{target}",
                    "remove the repeated entry — it is identical to an earlier check on this pipeline");
                continue;
            }

            switch (check.Type)
            {
                case "not_null" or "unique":
                    if (check.Columns.Count == 0)
                    {
                        Add($"{check.Type} check declares no columns",
                            $"list at least one column, e.g. `{check.Type}: [id]`");
                    }

                    RefuseUnknownOptions(check, [], Add);
                    break;

                case "row_count":
                    var hasMin = check.Options.TryGetValue("min", out var minRaw);
                    var hasMax = check.Options.TryGetValue("max", out var maxRaw);
                    if (!hasMin && !hasMax)
                    {
                        Add("row_count check needs at least one of min/max", "e.g. `row_count: { min: 1 }`");
                    }

                    if (hasMin && minRaw is not long)
                    {
                        Add($"row_count 'min' must be an integer, got '{minRaw}'", "e.g. `min: 1`");
                    }

                    if (hasMax && maxRaw is not long)
                    {
                        Add($"row_count 'max' must be an integer, got '{maxRaw}'", "e.g. `max: 1000`");
                    }

                    if (minRaw is long minValue && maxRaw is long maxValue && minValue > maxValue)
                    {
                        Add($"row_count min ({minValue}) exceeds max ({maxValue})", "make min <= max");
                    }

                    RefuseUnknownOptions(check, ["min", "max"], Add);
                    break;

                case "freshness":
                    // Skip when a malformed `column:` value already produced a PZ0113 above -- that's
                    // the one root cause; this check having zero Columns is a downstream symptom, not
                    // a second distinct problem (see columnShapeErrorIndices comment above).
                    if (check.Columns.Count != 1 && !columnShapeErrorIndices.Contains(i))
                    {
                        Add("freshness check requires exactly one 'column'",
                            "e.g. `freshness: { column: updated_at, max_age: 24h }`");
                    }

                    var maxAgeRaw = check.Options.TryGetValue("max_age", out var ma) ? ma?.ToString() : null;
                    if (maxAgeRaw is null || !DurationParser.TryParse(maxAgeRaw, out var maxAge) ||
                        maxAge <= TimeSpan.Zero)
                    {
                        Add($"freshness 'max_age' must be a positive duration, got '{maxAgeRaw ?? "(absent)"}'",
                            "use `<integer><unit>` with unit ms|s|m|h|d, e.g. `max_age: 24h`");
                    }

                    RefuseUnknownOptions(check, ["max_age"], Add);
                    break;

                case "accepted_values":
                    // Skip when a malformed `column:` value already produced a PZ0113 above -- see
                    // columnShapeErrorIndices comment above.
                    if (check.Columns.Count != 1 && !columnShapeErrorIndices.Contains(i))
                    {
                        Add("accepted_values check requires exactly one 'column'",
                            "e.g. `accepted_values: { column: status, values: [a, b] }`");
                    }

                    if (!check.Options.TryGetValue("values", out var valuesRaw) ||
                        valuesRaw is not List<object?> { Count: > 0 } values)
                    {
                        Add("accepted_values 'values' must be a non-empty list",
                            "e.g. `values: [pending, shipped]`");
                    }
                    else
                    {
                        foreach (var value in values.Where(v => v is not (string or long or double or bool)))
                        {
                            Add($"accepted_values 'values' entries must be scalars, got '{value ?? "null"}'",
                                "use strings, integers, floats, or booleans");
                        }
                    }

                    RefuseUnknownOptions(check, ["values"], Add);
                    break;

                case "custom_sql":
                    var name = check.Options.TryGetValue("name", out var n) ? n as string : null;
                    if (name is null || !IsValidCheckName(name))
                    {
                        Add($"custom_sql requires 'name' matching [a-z][a-z0-9_]*, got '{name ?? "(absent)"}'",
                            "e.g. `name: no_negative_totals` — it becomes the node name check_<pipeline>_<name>");
                    }
                    else if (!customSqlNames.Add(name))
                    {
                        Add($"duplicate custom_sql name '{name}'",
                            "each custom_sql check on a pipeline needs a distinct name");
                    }

                    if (!(check.Options.TryGetValue("sql", out var s) && s is string { Length: > 0 }))
                    {
                        Add("custom_sql requires non-empty 'sql'",
                            "a query returning VIOLATING rows, e.g. `sql: select * from staging.p where total < 0`");
                    }

                    if (check.Columns.Count > 0)
                    {
                        Add("custom_sql does not take columns", "put the column logic in the sql itself");
                    }

                    RefuseUnknownOptions(check, ["name", "sql"], Add);
                    break;

                default:
                    Add($"unknown check type '{check.Type}'",
                        "accepted: not_null | unique | row_count | freshness | accepted_values | custom_sql");
                    break;
            }
        }
    }

    private static void RefuseUnknownOptions(CheckDef check, string[] known, Action<string, string> add)
    {
        foreach (var key in check.Options.Keys
                     .Where(k => !known.Contains(k, StringComparer.Ordinal)).Order(StringComparer.Ordinal))
        {
            add($"{check.Type} check has unknown option '{key}'", known.Length == 0
                ? $"{check.Type} takes no options"
                : $"accepted options: {string.Join(", ", known.Order(StringComparer.Ordinal))}");
        }
    }

    private static bool IsValidCheckName(string name) =>
        name.Length > 0 && char.IsAsciiLetterLower(name[0]) &&
        name.All(ch => char.IsAsciiLetterLower(ch) || char.IsAsciiDigit(ch) || ch == '_');
}
