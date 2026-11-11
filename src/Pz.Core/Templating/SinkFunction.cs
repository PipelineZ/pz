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
internal sealed class SinkFunction : KeywordCallFunction
{
    /// <summary>Kwargs pz owns. Everything else is a connector write option and rides
    /// <see cref="SinkWriteOptions.Options"/> unchecked, exactly as an unrecognized key under a YAML
    /// <c>write:</c> block does -- no connector publishes a write-option vocabulary to check against,
    /// and the Abstractions ABI is fixed.</summary>
    private static readonly string[] KnownKwargs =
        ["strategy", "keys", "duplicates", "on_delete", "schema_policy", "retry"];

    private readonly List<InlineSinkBinding> _bindings;

    public SinkFunction(PipelineDef pipeline, List<InlineSinkBinding> bindings, List<PzError> errors)
        : base(pipeline, errors)
    {
        _bindings = bindings;
    }

    protected override string FunctionName => "sink";

    protected override string OptionKind => "write";

    protected override string MalformedCallCode => PzErrorCode.InvalidSinkCall;

    protected override string CallHint =>
        "sink('<connection>', '<entity>', strategy: 'merge', keys: ['<column>'])";

    public override object? Invoke(TemplateContext context, ScriptNode? callerContext, ScriptArray arguments,
        ScriptBlockStatement? blockStatement)
    {
        var line = (callerContext?.Span.Start.Line ?? 0) + 1;
        if (!TryReadTarget(arguments, line, out var sink, out var output))
        {
            return string.Empty;
        }

        var write = ParseWriteOptions(context, callerContext, sink, output, out var declaredAtCallSite);
        var binding = new InlineSinkBinding(sink, output, write, declaredAtCallSite);
        _bindings.Add(binding);
        return binding.Marker;
    }

    /// <summary><paramref name="declaredAtCallSite"/> is true when the author typed ANY keyword argument,
    /// recorded before the first Take() empties the map. DagCompiler needs the distinction, not the parsed
    /// value: `strategy: 'append'` is indistinguishable from the default once parsed, but declaring it at
    /// the call site while `entities: &lt;e&gt;: write:` also exists is PZ0341.</summary>
    private SinkWriteOptions ParseWriteOptions(TemplateContext context, ScriptNode? callerContext,
        string sink, string output, out bool declaredAtCallSite)
    {
        var kwargs = ScriptKwargs.Read(context, callerContext, (name, argLine) =>
            ReportDuplicateKwarg(sink, output, name, argLine));

        declaredAtCallSite = kwargs.Count > 0;

        RefuseMovedKwargs(kwargs, sink, output);

        var strategy = "append";
        if (Take(kwargs, "strategy") is { } strategyArg)
        {
            if (strategyArg.Value is string s &&
                WriteOptionsLoader.WriteStrategies.Contains(s, StringComparer.Ordinal))
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
            if (policyArg.Value is string p &&
                WriteOptionsLoader.SchemaPolicies.Contains(p, StringComparer.Ordinal))
            {
                schemaPolicy = p;
            }
            else
            {
                var nearMiss = policyArg.Value is string bad
                    ? NearMiss.Find(WriteOptionsLoader.SchemaPolicies, bad)
                    : null;
                Error(PzErrorCode.SyncModeInvalid, policyArg.Line,
                    $"sink('{sink}', '{output}'): 'schema_policy' must be one of: " +
                    $"{string.Join(", ", WriteOptionsLoader.SchemaPolicies)} (got '{Show(policyArg.Value)}')" +
                    (nearMiss is null ? "" : $" -- did you mean '{nearMiss}'"));
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
        RefuseKwargs(kwargs, sink, output,
            ("mode", PzErrorCode.RetiredWriteSurface, "strategy: 'merge'"),
            ("accept_duplicates", PzErrorCode.RetiredWriteSurface, "duplicates: 'accept'"),
            ("write", PzErrorCode.RetiredWriteSurface,
                "pass the write options directly: strategy: 'merge', keys: ['<column>']"),
            ("rate_limit", PzErrorCode.RateLimitConfigInvalid,
                "rate_limit is instance-level; declare it on the connection in connections.yml"),
            ("max_concurrency", PzErrorCode.ConcurrencyConfigInvalid,
                "max_concurrency is instance-level; declare it on the connection in connections.yml"),
            ("input", PzErrorCode.RemovedInputField,
                "the pipeline carrying this sink() call IS the input"),
            ("table", PzErrorCode.RetiredEntityQualifier,
                "the entity name is the table: sink('<connection>', 'schema.table')"),
            ("schema", PzErrorCode.RetiredEntityQualifier,
                "the entity name carries the schema: sink('<connection>', 'schema.table')"));
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

    /// <summary>The loader's own <c>retry:</c> parser, run against the kwarg's value, so a block reads and
    /// fails identically whether it sits under <c>write:</c>, in <c>source()</c> or here. The loader reports
    /// against the file with no line; its errors are re-stamped with this call's line.</summary>
    private RetryDef? ParseRetry(Dictionary<string, (object? Value, int Line)> kwargs, string sink, string output)
    {
        if (Take(kwargs, "retry") is not { } arg)
        {
            return null;
        }

        var loaderErrors = new List<PzError>();
        var retry = ProjectLoader.ParseRetry(
            new Dictionary<string, object?>(StringComparer.Ordinal) { ["retry"] = arg.Value },
            Pipeline.FilePath, loaderErrors, $"sink('{sink}', '{output}') ");
        AddAtLine(loaderErrors, arg.Line);
        return retry;
    }

    /// <summary>The pz-owned kwarg name <paramref name="option"/> is a near miss of, or null if it is
    /// plainly a connector option. Used by DagCompiler to warn rather than refuse.</summary>
    internal static string? NearMissKwarg(string option) => NearMiss.Find(KnownKwargs, option);

    private static (object? Value, int Line)? Take(Dictionary<string, (object? Value, int Line)> kwargs, string name) =>
        kwargs.Remove(name, out var arg) ? arg : null;

    private static string Show(object? value) => value switch
    {
        null => "null",
        List<object?> list => "[" + string.Join(", ", list.Select(Show)) + "]",
        Dictionary<string, object?> map => "{" + string.Join(", ", map.Select(kv => $"{kv.Key}: {Show(kv.Value)}")) + "}",
        _ => value.ToString() ?? "",
    };
}
