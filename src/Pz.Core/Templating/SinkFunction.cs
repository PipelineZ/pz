using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Validation;
using Scriban;
using Scriban.Runtime;
using Scriban.Syntax;

namespace Pz.Core.Templating;

/// <summary>
/// The <c>sink(connection, entity, ...write options)</c> call.
///
/// Implemented as an <see cref="IScriptCustomFunction"/> rather than an imported delegate because a
/// delegate cannot see the keyword-argument NAMES an author typed. Scriban 7.2.5 binds an
/// unrecognized name into the next free positional slot instead of raising, so
/// <c>sink('m', 'x', keyz: ['id'])</c> would silently set the write STRATEGY to a list of column
/// names -- exactly the silent-failure class the error philosophy forbids, on the surface that
/// decides delivery semantics. A custom function receives the caller's <see cref="ScriptFunctionCall"/>
/// node, whose <c>Arguments</c> carry the real <see cref="ScriptNamedArgument"/> names, values, and
/// source spans, so pz owns the whole binding and every rejection is its own.
///
/// Errors accumulate into the list this instance was constructed with rather than throwing, so one
/// render reports every malformed kwarg across every sink() call at once (aggregate, never
/// fail-one-at-a-time). Kwargs ride <see cref="InlineSinkBinding"/>, never the marker text, so
/// DagCompiler's prefix extraction and the bracket fan-out form see a stable marker.
/// </summary>
internal sealed class SinkFunction : IScriptCustomFunction
{
    private const string CallHint =
        "sink('<connection>', '<entity>', strategy: 'merge', keys: ['<column>'])";

    private static readonly string[] WriteStrategies = ["replace", "append", "merge"];

    /// <summary>Kwargs pz owns. Everything else is a connector write option and rides
    /// <see cref="SinkWriteOptions.Options"/> unchecked, exactly as an unrecognized key under a YAML
    /// <c>write:</c> block does -- no connector publishes a write-option vocabulary to check against,
    /// and the Abstractions ABI is fixed.</summary>
    private static readonly string[] KnownKwargs =
        ["strategy", "keys", "duplicates", "on_delete", "schema_policy", "retry"];

    private readonly PipelineDef _pipeline;
    private readonly List<InlineSinkBinding> _bindings;
    private readonly List<PzError> _errors;

    public SinkFunction(PipelineDef pipeline, List<InlineSinkBinding> bindings, List<PzError> errors)
    {
        _pipeline = pipeline;
        _bindings = bindings;
        _errors = errors;
    }

    public int RequiredParameterCount => 2;

    public int ParameterCount => 2;

    /// <summary>Direct, not LastParameter: LastParameter collapses argument 2 into a ScriptArray and
    /// drops named arguments entirely. Direct leaves the two positional arguments
    /// alone and lets pz read the named ones off the caller node.</summary>
    public ScriptVarParamKind VarParamKind => ScriptVarParamKind.Direct;

    public Type ReturnType => typeof(string);

    public ScriptParameterInfo GetParameterInfo(int index) => index switch
    {
        0 => new ScriptParameterInfo(typeof(string), "connection"),
        1 => new ScriptParameterInfo(typeof(string), "entity"),
        _ => new ScriptParameterInfo(typeof(object), "options"),
    };

    public object? Invoke(TemplateContext context, ScriptNode? callerContext, ScriptArray arguments,
        ScriptBlockStatement? blockStatement)
    {
        var line = (callerContext?.Span.Start.Line ?? 0) + 1;
        var sink = arguments.Count > 0 ? arguments[0] as string : null;
        var output = arguments.Count > 1 ? arguments[1] as string : null;
        if (sink is null || output is null)
        {
            Error(PzErrorCode.InvalidSinkCall, line,
                "sink() expects (connection, entity) as two string arguments");
            return string.Empty;
        }

        if (EntityName.Problem(output) is { } problem)
        {
            Error(PzErrorCode.EntityNameInvalid, line,
                $"sink('{sink}', '{output}'): entity name {problem}",
                "sink('<connection>', 'schema.table')");
        }

        if (arguments.Count > 2)
        {
            Error(PzErrorCode.InvalidSinkCall, line,
                $"sink('{sink}', '{output}') was passed {arguments.Count - 2} extra positional " +
                "argument(s) — write options are keyword arguments");
        }

        var write = ParseWriteOptions(context, callerContext, sink, output, out var declaredAtCallSite);
        _bindings.Add(new InlineSinkBinding(sink, output, write, declaredAtCallSite));
        return $"__pz_sink__{sink}__{output}__";
    }

    public ValueTask<object?> InvokeAsync(TemplateContext context, ScriptNode? callerContext,
        ScriptArray arguments, ScriptBlockStatement? blockStatement) =>
        new(Invoke(context, callerContext, arguments, blockStatement));

    /// <summary><paramref name="declaredAtCallSite"/> is true when the author typed ANY keyword argument,
    /// recorded before the first Take() empties the map. DagCompiler needs the distinction, not the parsed
    /// value: `strategy: 'append'` is indistinguishable from the default once parsed, but declaring it at
    /// the call site while `entities: &lt;e&gt;: write:` also exists is PZ0341.</summary>
    private SinkWriteOptions ParseWriteOptions(TemplateContext context, ScriptNode? callerContext,
        string sink, string output, out bool declaredAtCallSite)
    {
        var kwargs = ScriptKwargs.Read(context, callerContext, (name, argLine) =>
            Error(PzErrorCode.InvalidSinkCall, argLine,
                $"sink('{sink}', '{output}') passes '{name}' more than once"));

        declaredAtCallSite = kwargs.Count > 0;

        RefuseMovedKwargs(kwargs, sink, output);

        var strategy = "append";
        if (Take(kwargs, "strategy") is { } strategyArg)
        {
            if (strategyArg.Value is string s && WriteStrategies.Contains(s, StringComparer.Ordinal))
            {
                strategy = s;
            }
            else
            {
                Error(PzErrorCode.SyncModeInvalid, strategyArg.Line,
                    $"sink('{sink}', '{output}'): 'strategy' must be one of: replace, append, merge " +
                    $"(got '{Show(strategyArg.Value)}')");
            }
        }

        var keys = ParseKeys(kwargs, sink, output);
        var acceptDuplicates = ParseDuplicates(kwargs, sink, output);
        var onDelete = ParseOnDelete(kwargs, sink, output, strategy);
        var retry = ParseRetry(kwargs, sink, output);

        var schemaPolicy = "fail_on_change";
        if (Take(kwargs, "schema_policy") is { } policyArg)
        {
            if (policyArg.Value is string p)
            {
                schemaPolicy = p;
            }
            else
            {
                Error(PzErrorCode.SyncModeInvalid, policyArg.Line,
                    $"sink('{sink}', '{output}'): 'schema_policy' must be a string " +
                    $"(got '{Show(policyArg.Value)}')");
            }
        }

        // Whatever is left is a connector write option. Ordered so CanonicalJson.Serialize -- which
        // feeds the SinkWrite NodeId -- sees a stable dictionary regardless of the order the author
        // typed the kwargs in.
        var options = kwargs
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .ToDictionary(kv => kv.Key, kv => kv.Value.Value, StringComparer.Ordinal);

        return new SinkWriteOptions(strategy, keys, schemaPolicy, acceptDuplicates, onDelete, retry, options);
    }

    /// <summary>Names pz owns that must NOT be silently accepted as connector options: the retired
    /// write surface (PZ0333), the instance-level keys, the removed <c>input:</c> binding (PZ0112), and
    /// the YAML <c>write:</c> wrapper an author might carry over verbatim.</summary>
    private void RefuseMovedKwargs(Dictionary<string, (object? Value, int Line)> kwargs, string sink, string output)
    {
        foreach (var (name, code, hint) in new[]
        {
            ("mode", PzErrorCode.RetiredWriteSurface, "strategy: 'merge'"),
            ("accept_duplicates", PzErrorCode.RetiredWriteSurface, "duplicates: 'accept'"),
            ("write", PzErrorCode.RetiredWriteSurface,
                "pass the write options directly: strategy: 'merge', keys: ['<column>']"),
            ("rate_limit", PzErrorCode.RateLimitConfigInvalid,
                "rate_limit is instance-level; declare it on the sink"),
            ("input", PzErrorCode.RemovedInputField,
                "the pipeline carrying this sink() call IS the input"),
            ("table", PzErrorCode.RetiredEntityQualifier,
                "the entity name is the table: sink('<connection>', 'schema.table')"),
            ("schema", PzErrorCode.RetiredEntityQualifier,
                "the entity name carries the schema: sink('<connection>', 'schema.table')"),
        })
        {
            if (Take(kwargs, name) is { } arg)
            {
                Error(code, arg.Line, $"sink('{sink}', '{output}'): '{name}' is not a sink() keyword argument",
                    hint);
            }
        }
    }

    private IReadOnlyList<string> ParseKeys(Dictionary<string, (object? Value, int Line)> kwargs,
        string sink, string output)
    {
        if (Take(kwargs, "keys") is not { } arg)
        {
            return [];
        }

        if (arg.Value is List<object?> list && list.All(i => i is string))
        {
            return [.. list.Cast<string>()];
        }

        Error(PzErrorCode.YamlShape, arg.Line,
            $"sink('{sink}', '{output}'): 'keys' must be a list of strings (got '{Show(arg.Value)}')",
            "keys: ['<column>']");
        return [];
    }

    private bool ParseDuplicates(Dictionary<string, (object? Value, int Line)> kwargs, string sink, string output)
    {
        if (Take(kwargs, "duplicates") is not { } arg)
        {
            return false;
        }

        if (arg.Value as string == "accept")
        {
            return true;
        }

        Error(PzErrorCode.SyncModeInvalid, arg.Line,
            $"sink('{sink}', '{output}'): 'duplicates' must be the literal 'accept' (got '{Show(arg.Value)}')",
            "strategy: 'append', duplicates: 'accept'");
        return false;
    }

    private string? ParseOnDelete(Dictionary<string, (object? Value, int Line)> kwargs, string sink,
        string output, string strategy)
    {
        if (Take(kwargs, "on_delete") is not { } arg)
        {
            return null;
        }

        var value = arg.Value as string;
        if (value is not ("delete" or "soft" or "ignore"))
        {
            Error(PzErrorCode.SyncModeInvalid, arg.Line,
                $"sink('{sink}', '{output}'): 'on_delete' must be one of: delete, soft, ignore " +
                $"(got '{Show(arg.Value)}')",
                "strategy: 'merge', keys: ['<column>'], on_delete: 'delete'");
            return null;
        }

        if (strategy != "merge")
        {
            Error(PzErrorCode.SyncModeInvalid, arg.Line,
                $"sink('{sink}', '{output}'): 'on_delete' requires strategy: 'merge'",
                $"strategy: 'merge', keys: ['<column>'], on_delete: '{value}'");
            return null;
        }

        return value;
    }

    /// <summary>The call-site twin of <c>ProjectLoader.ParseRetry</c> -- same fields, same validity
    /// rules, same PZ0301 code; only the hint is call-site shaped. Deliberately not shared with the
    /// loader: the loader's copy dies with the YAML surfaces it serves.</summary>
    private RetryDef? ParseRetry(Dictionary<string, (object? Value, int Line)> kwargs, string sink, string output)
    {
        if (Take(kwargs, "retry") is not { } arg)
        {
            return null;
        }

        const string hint = "retry: { max_attempts: 8, base_delay: '2s', max_delay: '5m' }";
        if (arg.Value is not Dictionary<string, object?> retryMap)
        {
            Error(PzErrorCode.RetryConfigInvalid, arg.Line,
                $"sink('{sink}', '{output}'): 'retry' must be a mapping with " +
                "max_attempts/base_delay/max_delay", hint);
            return null;
        }

        var valid = true;
        int? maxAttempts = null;
        if (retryMap.TryGetValue("max_attempts", out var attemptsRaw) && attemptsRaw is not null)
        {
            // Through the loader's own reader, not a local `is int`: a literal written at the call site
            // arrives as int (Scriban), the same value reached via var() as long (the YAML loader and
            // --vars both produce long), so a local single-shape check would refuse the other.
            if (ProjectLoader.TryGetInt(retryMap, "max_attempts") is { } i and >= 1)
            {
                maxAttempts = i;
            }
            else
            {
                Error(PzErrorCode.RetryConfigInvalid, arg.Line,
                    $"sink('{sink}', '{output}'): retry.max_attempts must be an integer >= 1 " +
                    $"(got '{Show(attemptsRaw)}')", hint);
                valid = false;
            }
        }

        var baseDelay = ParseDelay(retryMap, "base_delay", sink, output, arg.Line, hint, ref valid);
        var maxDelay = ParseDelay(retryMap, "max_delay", sink, output, arg.Line, hint, ref valid);
        if (baseDelay is { } b && maxDelay is { } m && m < b)
        {
            Error(PzErrorCode.RetryConfigInvalid, arg.Line,
                $"sink('{sink}', '{output}'): retry.max_delay must be >= retry.base_delay", hint);
            valid = false;
        }

        foreach (var unknown in retryMap.Keys
            .Where(k => k is not ("max_attempts" or "base_delay" or "max_delay"))
            .OrderBy(k => k, StringComparer.Ordinal))
        {
            Error(PzErrorCode.RetryConfigInvalid, arg.Line,
                $"sink('{sink}', '{output}'): unknown retry key '{unknown}'", hint);
            valid = false;
        }

        return valid ? new RetryDef(maxAttempts, baseDelay, maxDelay) : null;
    }

    private TimeSpan? ParseDelay(Dictionary<string, object?> retryMap, string key, string sink, string output,
        int line, string hint, ref bool valid)
    {
        if (!retryMap.TryGetValue(key, out var raw) || raw is null)
        {
            return null;
        }

        if (!DurationParser.TryParse(raw.ToString(), out var duration) || duration <= TimeSpan.Zero)
        {
            Error(PzErrorCode.RetryConfigInvalid, line,
                $"sink('{sink}', '{output}'): retry.{key} must be a positive duration like 500ms, 2s, " +
                $"5m, 1h, or 1d (got '{Show(raw)}')", hint);
            valid = false;
            return null;
        }

        return duration;
    }

    /// <summary>The pz-owned kwarg name <paramref name="option"/> is a near miss of, or null if it is
    /// plainly a connector option. Used by DagCompiler to warn rather than refuse.</summary>
    internal static string? NearMissKwarg(string option) => ScriptKwargs.NearMiss(KnownKwargs, option);

    private static (object? Value, int Line)? Take(Dictionary<string, (object? Value, int Line)> kwargs, string name) =>
        kwargs.Remove(name, out var arg) ? arg : null;

    private static string Show(object? value) => value switch
    {
        null => "null",
        List<object?> list => "[" + string.Join(", ", list.Select(Show)) + "]",
        Dictionary<string, object?> map => "{" + string.Join(", ", map.Select(kv => $"{kv.Key}: {Show(kv.Value)}")) + "}",
        _ => value.ToString() ?? "",
    };

    private void Error(string code, int line, string message, string? hint = null) =>
        _errors.Add(new PzError(code, $"{_pipeline.FilePath}: {message}.", _pipeline.FilePath, line,
            hint ?? CallHint));
}
