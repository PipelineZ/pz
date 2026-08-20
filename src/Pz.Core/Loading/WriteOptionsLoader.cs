using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Loading;

/// <summary>The YAML twin of <c>SinkFunction</c>'s keyword arguments. Same names, same validity rules,
/// same PZ codes — only the hint is YAML-shaped, because
/// moving an option between the two surfaces has to be cut-and-paste.
///
/// Deliberately NOT shared with <c>SinkFunction</c>: that one reads Scriban syntax nodes and reports
/// against a pipeline file and line, this one reads a YAML map and reports against connections.yml.
/// The overlap is the rule table, which is short, and a shared abstraction over both argument shapes
/// would be longer than the duplication.</summary>
internal static class WriteOptionsLoader
{
    private static readonly string[] WriteStrategies = ["replace", "append", "merge"];

    /// <summary>Parses one <c>entities: &lt;e&gt;: write:</c> block, or null when it is malformed --
    /// errors are aggregated, never thrown, so one load reports every bad block.</summary>
    public static SinkWriteOptions? Parse(object? value, string entity, string where, List<PzError> errors)
    {
        // A bodyless `write:` parses as an empty scalar, not a null node; it means "the defaults".
        if (value is null or "")
        {
            return SinkWriteOptions.Default;
        }

        if (value is not Dictionary<string, object?> write)
        {
            errors.Add(new PzError(PzErrorCode.YamlShape,
                $"{ConnectionsLoader.FileName}: {where}: 'write' must be a mapping of options, or empty.",
                ConnectionsLoader.FileName, null, "write:\n      strategy: merge\n      keys: [id]"));
            return null;
        }

        var valid = true;

        var strategy = "append";
        if (write.TryGetValue("strategy", out var strategyValue) && strategyValue is not null)
        {
            if (strategyValue is string s && WriteStrategies.Contains(s, StringComparer.Ordinal))
            {
                strategy = s;
            }
            else
            {
                Error(errors, PzErrorCode.SyncModeInvalid, where,
                    $"'strategy' must be one of: replace, append, merge (got '{strategyValue}')",
                    "strategy: merge");
                valid = false;
            }
        }

        var keys = ParseKeys(write, where, errors, ref valid);
        var acceptDuplicates = ParseDuplicates(write, where, errors, ref valid);
        var onDelete = ParseOnDelete(write, where, strategy, errors, ref valid);

        var schemaPolicy = "fail_on_change";
        if (write.TryGetValue("schema_policy", out var policyValue) && policyValue is not null)
        {
            if (policyValue is string p)
            {
                schemaPolicy = p;
            }
            else
            {
                Error(errors, PzErrorCode.SyncModeInvalid, where,
                    $"'schema_policy' must be a string (got '{policyValue}')", "schema_policy: fail_on_change");
                valid = false;
            }
        }

        var retry = ProjectLoader.ParseRetry(write, ConnectionsLoader.FileName, errors, $"{where} write ");

        // Whatever is left is a connector write option. Ordered so CanonicalJson.Serialize -- which feeds
        // the SinkWrite NodeId -- sees the same dictionary whichever surface declared it.
        var options = write
            .Where(kv => kv.Key is not ("strategy" or "keys" or "duplicates" or "on_delete"
                or "schema_policy" or "retry"))
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);

        return valid
            ? new SinkWriteOptions(strategy, keys, schemaPolicy, acceptDuplicates, onDelete, retry, options)
            : null;
    }

    private static IReadOnlyList<string> ParseKeys(Dictionary<string, object?> write, string where,
        List<PzError> errors, ref bool valid)
    {
        if (!write.TryGetValue("keys", out var value) || value is null)
        {
            return [];
        }

        if (value is List<object?> list && list.All(i => i is string))
        {
            return [.. list.Cast<string>()];
        }

        Error(errors, PzErrorCode.YamlShape, where, "'keys' must be a list of strings", "keys: [id]");
        valid = false;
        return [];
    }

    private static bool ParseDuplicates(Dictionary<string, object?> write, string where,
        List<PzError> errors, ref bool valid)
    {
        if (!write.TryGetValue("duplicates", out var value) || value is null)
        {
            return false;
        }

        if (value as string == "accept")
        {
            return true;
        }

        Error(errors, PzErrorCode.SyncModeInvalid, where,
            $"'duplicates' must be the literal 'accept' (got '{value}')",
            "strategy: append\n      duplicates: accept");
        valid = false;
        return false;
    }

    private static string? ParseOnDelete(Dictionary<string, object?> write, string where, string strategy,
        List<PzError> errors, ref bool valid)
    {
        if (!write.TryGetValue("on_delete", out var value) || value is null)
        {
            return null;
        }

        if (value as string is not ("delete" or "soft" or "ignore"))
        {
            Error(errors, PzErrorCode.SyncModeInvalid, where,
                $"'on_delete' must be one of: delete, soft, ignore (got '{value}')",
                "strategy: merge\n      keys: [id]\n      on_delete: delete");
            valid = false;
            return null;
        }

        if (strategy != "merge")
        {
            Error(errors, PzErrorCode.SyncModeInvalid, where, "'on_delete' requires strategy: merge",
                $"strategy: merge\n      keys: [id]\n      on_delete: {value}");
            valid = false;
            return null;
        }

        return (string)value;
    }

    private static void Error(List<PzError> errors, string code, string where, string message, string hint) =>
        errors.Add(new PzError(code, $"{ConnectionsLoader.FileName}: {where} write: {message}.",
            ConnectionsLoader.FileName, null, hint));
}
