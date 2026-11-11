using System.Globalization;
using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Loading;

/// <summary>The per-connection and per-entity settings blocks (<c>sync:</c>, <c>retry:</c>, <c>rate_limit:</c>, <c>max_concurrency:</c>) that connections.yml and the <c>source()</c> kwarg surface parse through one set of rules.</summary>
public static partial class ProjectLoader
{
    private static readonly string[] IncrementalOnlyKeys = ["cursor", "max_window", "initial", "until"];

    /// <summary>The unified dataset <c>sync:</c> block (an entity's <c>read:</c> block or the
    /// <c>source()</c> kwarg). Absent entirely -> null (mode: auto, omitted form). Present but not a mapping, missing/unknown
    /// 'mode', 'mode: incremental' missing 'cursor', a non-scalar 'sync.slot', or an unknown
    /// sub-key under the resolved mode -> PZ0334 (SyncModeInvalid).
    /// 'mode: cdc' accepts only 'mode' and the optional 'slot'; whether the named
    /// cursor actually exists in -- and has an allowed type in -- a declared <c>columns:</c>
    /// contract remains a semantic question for DagCompiler (PZ0212), not the loader.</summary>
    internal static SyncModeDef? ParseSyncMode(
        Dictionary<string, object?> datasetYaml, string datasetName, string relativePath, List<PzError> errors)
    {
        if (!datasetYaml.TryGetValue("sync", out var value) || value is null)
        {
            return null; // mode: auto, omitted form
        }

        const string hint = "sync:\n  mode: incremental\n  cursor: <column>";
        if (value is not Dictionary<string, object?> syncYaml)
        {
            errors.Add(new PzError(PzErrorCode.SyncModeInvalid,
                $"{relativePath}: dataset '{datasetName}' field 'sync' must be a mapping with a 'mode' key.",
                relativePath, null, hint));
            return null;
        }

        var modeRaw = TryGetString(syncYaml, "mode");
        switch (modeRaw)
        {
            case "incremental":
                if (TryGetString(syncYaml, "cursor") is not { Length: > 0 } cursor)
                {
                    errors.Add(new PzError(PzErrorCode.SyncModeInvalid,
                        $"{relativePath}: dataset '{datasetName}': sync mode 'incremental' requires 'cursor'.",
                        relativePath, null, hint));
                    return null;
                }

                RefuseUnknownSyncKeys(syncYaml, ["mode", .. IncrementalOnlyKeys], datasetName, relativePath, errors);
                var maxWindow = ParseIncrementalScalar(syncYaml, "max_window", datasetName, relativePath, errors);
                var initial = ParseIncrementalScalar(syncYaml, "initial", datasetName, relativePath, errors);
                var until = ParseIncrementalScalar(syncYaml, "until", datasetName, relativePath, errors);
                return new SyncModeDef(SyncMode.Incremental, new IncrementalDef(cursor, maxWindow, initial, until));

            case "cdc":
                RefuseUnknownSyncKeys(syncYaml, ["mode", "slot"], datasetName, relativePath, errors);
                string? slot = null;
                if (syncYaml.TryGetValue("slot", out var slotRaw) && slotRaw is not null)
                {
                    if (slotRaw is List<object?> or Dictionary<string, object?>)
                    {
                        errors.Add(new PzError(PzErrorCode.SyncModeInvalid,
                            $"{relativePath}: dataset '{datasetName}' field 'sync.slot' must be a scalar value.",
                            relativePath, null, "sync:\n  mode: cdc\n  slot: <name>"));
                    }
                    else
                    {
                        slot = Convert.ToString(slotRaw, CultureInfo.InvariantCulture);
                    }
                }

                return new SyncModeDef(SyncMode.Cdc, null, slot);

            case "auto":
                RefuseUnknownSyncKeys(syncYaml, ["mode"], datasetName, relativePath, errors);
                return new SyncModeDef(SyncMode.Auto, null);

            case null:
                errors.Add(new PzError(PzErrorCode.SyncModeInvalid,
                    $"{relativePath}: dataset '{datasetName}': 'sync' is missing required field 'mode' " +
                    "(a bare marker sync block is retired — connector-managed feeds need no sync block at all: delete it).",
                    relativePath, null, hint));
                return null;

            default:
                errors.Add(new PzError(PzErrorCode.SyncModeInvalid,
                    $"{relativePath}: dataset '{datasetName}': unknown sync mode '{modeRaw}' (accepted: incremental, cdc, auto).",
                    relativePath, null, hint));
                return null;
        }
    }

    private static void RefuseUnknownSyncKeys(Dictionary<string, object?> syncYaml, string[] allowed,
        string datasetName, string relativePath, List<PzError> errors)
    {
        foreach (var key in syncYaml.Keys.Where(k => !allowed.Contains(k, StringComparer.Ordinal))
            .OrderBy(k => k, StringComparer.Ordinal))
        {
            var detail = allowed.Length == 1
                ? "mode 'auto' accepts no other keys"
                : $"unknown 'sync' key '{key}'";
            errors.Add(new PzError(PzErrorCode.SyncModeInvalid,
                $"{relativePath}: dataset '{datasetName}': {detail}.",
                relativePath, null, "sync:\n  mode: incremental\n  cursor: <column>"));
        }
    }

    /// <summary>The window trio must be YAML scalars; anything list/mapping-shaped is
    /// PZ0334 (mirroring the rest of ParseSyncMode). Values are surfaced as raw strings (numbers
    /// included — a numeric `initial: 100` round-trips as "100", which IS its canonical digit form)
    /// for DagCompiler's semantic pass.</summary>
    private static string? ParseIncrementalScalar(Dictionary<string, object?> syncYaml, string key,
        string datasetName, string relativePath, List<PzError> errors)
    {
        if (!syncYaml.TryGetValue(key, out var value) || value is null)
        {
            return null;
        }

        if (value is List<object?> or Dictionary<string, object?>)
        {
            errors.Add(new PzError(PzErrorCode.SyncModeInvalid,
                $"{relativePath}: dataset '{datasetName}' field 'sync.{key}' must be a scalar value.",
                relativePath, null, $"sync:\n  mode: incremental\n  cursor: <column>\n  {key}: <value>"));
            return null;
        }

        return Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    /// <summary>The <c>retry:</c> block — one parser for BOTH levels: the connection and an entity's
    /// read/write side. Absent -> null. All three keys optional; absent keys stay null and the engine
    /// cascades read/write side -> connection -> <c>RetryPolicy.Default</c> field-wise. The max_delay >= base_delay relation is checked only when
    /// BOTH are declared in the SAME block: a single declared key that crosses a coarser level is
    /// harmless (ComputeDelay caps at MaxDelay), and Pz.Core cannot see the engine default to compare
    /// against (layering). Errors are PZ0121, aggregate, and name the file — <paramref name="context"/>
    /// additionally names the entity block per the repo's error rule; connection-level call sites
    /// pass "".</summary>
    internal static RetryDef? ParseRetry(Dictionary<string, object?> yaml, string relativePath, List<PzError> errors,
        string context = "")
    {
        if (!yaml.TryGetValue("retry", out var value) || value is null)
        {
            return null;
        }

        const string hint = "retry:\n  max_attempts: 8\n  base_delay: 2s\n  max_delay: 5m";
        if (value is not Dictionary<string, object?> retryYaml)
        {
            errors.Add(new PzError(PzErrorCode.RetryConfigInvalid,
                $"{relativePath}: {context}'retry' must be a mapping with max_attempts/base_delay/max_delay.",
                relativePath, null, hint));
            return null;
        }

        // source() and sink() run this same parser over their retry: kwarg, so an unknown key fails
        // identically on every surface a retry: block can be cut-and-pasted between.
        var beforeUnknownKeys = errors.Count;
        RefuseUnknownKeys(retryYaml, ["max_attempts", "base_delay", "max_delay"], PzErrorCode.RetryConfigInvalid,
            relativePath, $"{context}retry: ", errors);
        var valid = errors.Count == beforeUnknownKeys;

        int? maxAttempts = null;
        if (retryYaml.TryGetValue("max_attempts", out var attemptsRaw) && attemptsRaw is not null)
        {
            maxAttempts = TryGetInt(retryYaml, "max_attempts");
            if (maxAttempts is null or < 1)
            {
                errors.Add(new PzError(PzErrorCode.RetryConfigInvalid,
                    $"{relativePath}: {context}retry.max_attempts must be an integer >= 1 (got '{attemptsRaw}').",
                    relativePath, null, hint));
                valid = false;
            }
        }

        var baseDelay = ParseRetryDelay(retryYaml, "base_delay", relativePath, errors, hint, context, ref valid);
        var maxDelay = ParseRetryDelay(retryYaml, "max_delay", relativePath, errors, hint, context, ref valid);

        if (baseDelay is { } b && maxDelay is { } m && m < b)
        {
            errors.Add(new PzError(PzErrorCode.RetryConfigInvalid,
                $"{relativePath}: {context}retry.max_delay must be >= retry.base_delay.",
                relativePath, null, hint));
            valid = false;
        }

        return valid ? new RetryDef(maxAttempts, baseDelay, maxDelay) : null;
    }

    private static TimeSpan? ParseRetryDelay(Dictionary<string, object?> retryYaml, string key,
        string relativePath, List<PzError> errors, string hint, string context, ref bool valid)
    {
        if (!retryYaml.TryGetValue(key, out var raw) || raw is null)
        {
            return null;
        }

        if (!DurationParser.TryParse(raw.ToString(), out var duration) || duration <= TimeSpan.Zero)
        {
            errors.Add(new PzError(PzErrorCode.RetryConfigInvalid,
                $"{relativePath}: {context}retry.{key} must be a positive duration like 500ms, 2s, 5m, 1h, or 1d (got '{raw}').",
                relativePath, null, hint));
            valid = false;
            return null;
        }

        return duration;
    }

    /// <summary>The instance-level `rate_limit:` block — mirrors <see cref="ParseRetry"/>'s
    /// skeleton, but simpler (two int fields, both bounded 1..1_000_000, no cross-field relation). Absent -> null.
    /// `requests_per_minute` is required; `burst` optional (absent -> engine derives it via
    /// <see cref="RateLimitDef.EffectiveBurst"/>). Errors are PZ0318, aggregate, and name the file. `rate_limit:`
    /// is connection-level only -- the entity-level callers don't invoke this parser at all; instead
    /// they refuse the key outright (see the `ContainsKey("rate_limit")` check in
    /// <see cref="ConnectionsLoader"/>'s read block).</summary>
    internal static RateLimitDef? ParseRateLimit(Dictionary<string, object?> yaml, string relativePath,
        List<PzError> errors)
    {
        if (!yaml.TryGetValue("rate_limit", out var value) || value is null)
        {
            return null;
        }

        const string hint = "rate_limit:\n  requests_per_minute: 60\n  burst: 10";
        if (value is not Dictionary<string, object?> rateLimitYaml)
        {
            errors.Add(new PzError(PzErrorCode.RateLimitConfigInvalid,
                $"{relativePath}: 'rate_limit' must be a mapping with requests_per_minute (and optional burst).",
                relativePath, null, hint));
            return null;
        }

        RefuseUnknownKeys(rateLimitYaml, ["requests_per_minute", "burst"], PzErrorCode.RateLimitConfigInvalid,
            relativePath, "rate_limit: ", errors);

        var valid = true;

        int? rpm = null;
        if (rateLimitYaml.TryGetValue("requests_per_minute", out var rpmRaw) && rpmRaw is not null)
        {
            rpm = TryGetInt(rateLimitYaml, "requests_per_minute");
            if (rpm is null or < 1 or > 1_000_000)
            {
                errors.Add(new PzError(PzErrorCode.RateLimitConfigInvalid,
                    $"{relativePath}: rate_limit.requests_per_minute must be an integer in 1..1000000 (got '{rpmRaw}').",
                    relativePath, null, hint));
                valid = false;
            }
        }
        else
        {
            errors.Add(new PzError(PzErrorCode.RateLimitConfigInvalid,
                $"{relativePath}: rate_limit.requests_per_minute is required.",
                relativePath, null, hint));
            valid = false;
        }

        int? burst = null;
        if (rateLimitYaml.TryGetValue("burst", out var burstRaw) && burstRaw is not null)
        {
            burst = TryGetInt(rateLimitYaml, "burst");
            if (burst is null or < 1 or > 1_000_000)
            {
                errors.Add(new PzError(PzErrorCode.RateLimitConfigInvalid,
                    $"{relativePath}: rate_limit.burst must be an integer in 1..1000000 (got '{burstRaw}').",
                    relativePath, null, hint));
                valid = false;
            }
        }

        return valid ? new RateLimitDef(rpm!.Value, burst) : null;
    }

    /// <summary>Connection-level `max_concurrency:` — caps how
    /// many of this instance's nodes the dispatcher runs concurrently. Loader validates shape+bound only;
    /// enforcement is RunOrchestrator's. Absent -> null (unbounded, engine.threads still governs).</summary>
    internal static int? ParseMaxConcurrency(Dictionary<string, object?> yaml, string relativePath, List<PzError> errors)
    {
        if (!yaml.TryGetValue("max_concurrency", out var raw) || raw is null)
        {
            return null;
        }

        var value = TryGetInt(yaml, "max_concurrency");
        if (value is null or < 1)
        {
            errors.Add(new PzError(PzErrorCode.ConcurrencyConfigInvalid,
                $"{relativePath}: 'max_concurrency' must be an integer >= 1 (got '{raw}').",
                relativePath, null, "max_concurrency: 2"));
            return null;
        }

        return value;
    }

    /// <summary>Connection-level `allow_unsigned_extensions:` -- opts a connection's native scans/copies
    /// out of the planner's unsigned-packaged-extension gate (PZ0359). Absent -> false (a value present
    /// but not a bool is a loud error, never a silent false, matching the engine.force_universal
    /// precedent in <see cref="RefuseUnreadableEngineValue"/>).</summary>
    internal static bool ParseAllowUnsignedExtensions(Dictionary<string, object?> yaml, string relativePath,
        string connectionName, List<PzError> errors)
    {
        if (!yaml.TryGetValue("allow_unsigned_extensions", out var raw) || raw is (null or ""))
        {
            return false;
        }

        if (raw is bool value)
        {
            return value;
        }

        errors.Add(new PzError(PzErrorCode.YamlShape,
            $"{relativePath}: connection '{connectionName}' allow_unsigned_extensions must be true or false (got '{raw}').",
            relativePath, null, "allow_unsigned_extensions: true"));
        return false;
    }
}
