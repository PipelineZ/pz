using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Loading;

/// <summary>Parses the project's single <c>connections.yml</c>.
///
/// Three levels, deliberately. The CONNECTION holds credentials and per-instance limits. An ENTITY is a
/// thing in that place, named the way that place names it. A DIRECTION -- <c>read:</c> or <c>write:</c> --
/// holds how to move data that way. Direction nests inside the entity rather than splitting into
/// <c>reads:</c>/<c>writes:</c> maps at the connection level, so an entity that is written and later read
/// back appears once.
///
/// Connector config is FLAT, not nested under a <c>config:</c> key: connectors already publish
/// <c>ConnectionConfigSchema</c> as JSON Schema, so an unknown key is caught at validation tier 3 without
/// a nesting level to carry it. Everything at connection level that is not reserved is that config.
///
/// An unrecognized key at any level pz owns is an aggregated error, never silently skipped.</summary>
internal static class ConnectionsLoader
{
    internal const string FileName = "connections.yml";

    /// <summary>Keys pz owns at connection level. A connector whose <c>ConnectionConfigSchema</c> declares
    /// one of these is refused at validation tier 3 (PZ0345) -- it could never receive it.</summary>
    internal static readonly string[] ReservedKeys =
        ["connector", "entities", "max_concurrency", "rate_limit", "retry", "allow_unsigned_extensions"];

    private static readonly string[] Directions = ["read", "write"];

    public static List<ConnectionDef> Load(string projectDir, IReadOnlyDictionary<string, string> env,
        List<PzError> errors)
    {
        var connections = new List<ConnectionDef>();
        var path = Path.Combine(projectDir, FileName);
        if (!File.Exists(path))
        {
            return connections;
        }

        Dictionary<string, object?> yaml;
        try
        {
            yaml = YamlMapper.LoadFile(path, FileName);
        }
        catch (PzConfigException ex)
        {
            errors.Add(ex.Error);
            return connections;
        }

        foreach (var (name, value) in yaml)
        {
            // Without this, a dbt-style `outputs:` block parses as a connection named "outputs" and the
            // error tells the author to ADD `connector:` to it -- the opposite of PZ0347's retirement.
            if (name == "outputs")
            {
                errors.Add(new PzError(PzErrorCode.RetiredOutputsBlock,
                    $"{FileName}: the 'outputs' block is retired -- there are no per-target output profiles.",
                    FileName, null,
                    "declare each place as a top-level connection instead"));
                continue;
            }

            if (value is not Dictionary<string, object?> block)
            {
                errors.Add(new PzError(PzErrorCode.YamlShape,
                    $"{FileName}: connection '{name}' must be a mapping.", FileName, null,
                    $"{name}:\n  connector: <connector>"));
                continue;
            }

            if (ProjectLoader.TryGetString(block, "connector") is not { Length: > 0 } connector)
            {
                errors.Add(new PzError(PzErrorCode.YamlShape,
                    $"{FileName}: connection '{name}' is missing required field 'connector'.", FileName, null,
                    $"{name}:\n  connector: postgres"));
                continue;
            }

            // Connection config is the one place ${VAR} references are interpolated.
            var config = (Dictionary<string, object?>)EnvInterpolator.InterpolateTree(
                block.Where(kv => !ReservedKeys.Contains(kv.Key, StringComparer.Ordinal))
                    .ToDictionary(kv => kv.Key, kv => kv.Value),
                env, FileName, errors)!;

            var (datasets, writes) = LoadEntities(block, name, errors);

            connections.Add(new ConnectionDef(name, connector, config, datasets, FileName,
                ProjectLoader.ParseRetry(block, FileName, errors, $"connection '{name}' "),
                ProjectLoader.ParseMaxConcurrency(block, FileName, errors),
                ProjectLoader.ParseRateLimit(block, FileName, errors),
                ProjectLoader.ParseAllowUnsignedExtensions(block, FileName, name, errors))
            {
                EntityWrites = writes,
            });
        }

        return connections;
    }

    private static (List<DatasetDef> Datasets, Dictionary<string, SinkWriteOptions> Writes) LoadEntities(
        Dictionary<string, object?> block, string connectionName, List<PzError> errors)
    {
        var datasets = new List<DatasetDef>();
        var writes = new Dictionary<string, SinkWriteOptions>(StringComparer.Ordinal);

        foreach (var (entity, value) in ProjectLoader.GetDict(block, "entities"))
        {
            var where = $"connection '{connectionName}' entity '{entity}'";
            if (EntityName.Problem(entity) is { } problem)
            {
                errors.Add(new PzError(PzErrorCode.EntityNameInvalid,
                    $"{FileName}: {where}: name {problem}.", FileName, null,
                    "name the entity exactly as the remote system spells it, e.g. 'dbo.orders'"));
                continue;
            }

            if (value is not Dictionary<string, object?> entityBlock || entityBlock.Count == 0)
            {
                errors.Add(new PzError(PzErrorCode.YamlShape,
                    $"{FileName}: {where} declares no direction.", FileName, null,
                    $"{entity}:\n    read:\n      <read options>\n    write:\n      strategy: 'merge'"));
                continue;
            }

            foreach (var unknown in entityBlock.Keys
                .Where(k => !Directions.Contains(k, StringComparer.Ordinal))
                .OrderBy(k => k, StringComparer.Ordinal))
            {
                errors.Add(new PzError(PzErrorCode.YamlShape,
                    $"{FileName}: {where}: unknown key '{unknown}'.", FileName, null,
                    "an entity holds 'read:' and/or 'write:', nothing else"));
            }

            if (entityBlock.TryGetValue("read", out var read))
            {
                if (ReadDataset(read, entity, where, errors) is { } dataset)
                {
                    datasets.Add(dataset);
                }
            }

            if (entityBlock.TryGetValue("write", out var write))
            {
                if (WriteOptionsLoader.Parse(write, entity, where, errors) is { } options)
                {
                    writes[entity] = options;
                }
            }
        }

        return (datasets, writes);
    }

    /// <summary>The <c>read:</c> block, mapped onto the <see cref="DatasetDef"/> every downstream stage
    /// already consumes. <c>columns</c>/<c>sync</c>/<c>retry</c> are lifted out by name; everything left
    /// is a connector read option.</summary>
    private static DatasetDef? ReadDataset(object? value, string entity, string where, List<PzError> errors)
    {
        // A bodyless `read:` parses as an empty scalar, not a null node -- and it is the common shape
        // for an entity that needs no read options at all.
        if (value is null or "")
        {
            return new DatasetDef(entity, new Dictionary<string, object?>(), null);
        }

        if (value is not Dictionary<string, object?> read)
        {
            errors.Add(new PzError(PzErrorCode.YamlShape,
                $"{FileName}: {where}: 'read' must be a mapping of options, or empty.", FileName, null,
                "read:\n      partitions: 4"));
            return null;
        }

        Dictionary<string, string>? columns = null;
        // A bodyless `columns:` parses as an empty scalar, not a null node; both mean "no contract".
        if (read.TryGetValue("columns", out var columnsValue) && columnsValue is not (null or ""))
        {
            if (columnsValue is not Dictionary<string, object?> columnsYaml)
            {
                // Silently dropping a mistyped contract would flip the dataset to a contract-less
                // auto-detect read — a semantic change, not a cosmetic one.
                errors.Add(new PzError(PzErrorCode.YamlShape,
                    $"{FileName}: {where}: 'columns' must be a mapping of column name to type.",
                    FileName, null, "columns:\n          id: bigint"));
            }
            else if (columnsYaml.FirstOrDefault(kv =>
                kv.Value is Dictionary<string, object?> or List<object?>) is { Key: not null } nonScalar)
            {
                errors.Add(new PzError(PzErrorCode.YamlShape,
                    $"{FileName}: {where}: column '{nonScalar.Key}' must map to a type name, " +
                    "not a nested structure.",
                    FileName, null, $"{nonScalar.Key}: bigint"));
            }
            else
            {
                columns = columnsYaml.ToDictionary(kv => kv.Key, kv => kv.Value?.ToString() ?? string.Empty);
            }
        }

        if (read.TryGetValue("incremental", out var oldIncremental) && oldIncremental is not null)
        {
            var oldCursor = oldIncremental is Dictionary<string, object?> oi
                ? ProjectLoader.TryGetString(oi, "cursor")
                : null;
            errors.Add(new PzError(PzErrorCode.RetiredReadSurface,
                $"{FileName}: {where}: the 'incremental:' block was replaced by the unified 'sync:' block.",
                FileName, null,
                $"sync:\n          mode: incremental\n          cursor: {oldCursor ?? "<column>"}"));
        }

        // The entity name IS the object name. Reported once for the pair, so the hint can show the
        // whole joined name the key should become.
        var qualifier = ProjectLoader.TryGetString(read, "schema");
        var qualified = ProjectLoader.TryGetString(read, "table");
        if (qualifier is not null || qualified is not null)
        {
            var replacement = qualifier is null ? qualified! : $"{qualifier}.{qualified ?? entity}";
            errors.Add(new PzError(PzErrorCode.RetiredEntityQualifier,
                $"{FileName}: {where}: 'schema'/'table' are retired -- the entity name is the object name.",
                FileName, null,
                string.Equals(replacement, entity, StringComparison.Ordinal)
                    ? $"delete those keys -- the entity name '{entity}' already names the object"
                    : $"rename the entity to '{replacement}' and delete those keys"));
        }

        // Instance-level only: a misplaced key is loud, not a no-op.
        if (read.ContainsKey("rate_limit"))
        {
            errors.Add(new PzError(PzErrorCode.RateLimitConfigInvalid,
                $"{FileName}: {where}: 'rate_limit' is instance-level; declare it on the connection.",
                FileName, null, "move rate_limit up to the connection mapping"));
        }

        var options = read
            .Where(kv => kv.Key is not ("columns" or "incremental" or "retry" or "sync" or "rate_limit"
                or "schema" or "table"))
            .ToDictionary(kv => kv.Key, kv => kv.Value);

        return new DatasetDef(entity, options, columns,
            ProjectLoader.ParseSyncMode(read, entity, FileName, errors),
            ProjectLoader.ParseRetry(read, FileName, errors, $"{where} read "));
    }
}
