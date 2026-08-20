using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Validation;
using Scriban;
using Scriban.Runtime;
using Scriban.Syntax;

namespace Pz.Core.Templating;

/// <summary>
/// The <c>source(connection, entity, ...read options)</c> call — the read-side twin of
/// <see cref="SinkFunction"/>, and an <see cref="IScriptCustomFunction"/> for the same reason: Scriban
/// 7.2.5 binds an unrecognized named argument into the next free positional slot rather than raising,
/// so an imported delegate cannot tell a typo from a value. See <see cref="SinkFunction"/> for the
/// full rationale.
///
/// The sub-blocks (<c>sync:</c>, <c>retry:</c>, <c>columns:</c>) are parsed by the SAME loader code the
/// YAML surface uses. Scriban object literals convert to the plain CLR shapes YamlMapper produces, so
/// <c>ProjectLoader.ParseSyncMode</c>/<c>ParseRetry</c> accept them unchanged — one rule table, one set
/// of messages, and no chance of the two surfaces drifting. Only the location is re-stamped: a loader
/// error names a file with no line, and this call site has one.
/// </summary>
internal sealed class SourceFunction : IScriptCustomFunction
{
    private const string CallHint =
        "source('<connection>', '<entity>', partitions: 8, sync: { mode: 'incremental', cursor: '<column>' })";

    /// <summary>Kwargs pz owns. Everything else is a connector read option and rides
    /// <see cref="SourceReadOptions.Options"/> unchecked, exactly as an unrecognized key under a YAML
    /// <c>read:</c> block does. <c>partition_column</c>/<c>partitions</c> are listed because they are
    /// pz-documented names worth a near-miss warning, but they are NOT lifted out — the connectors read
    /// them straight off the options bag.</summary>
    internal static readonly string[] KnownKwargs =
        ["columns", "sync", "retry", "partition_column", "partitions"];

    private readonly PipelineDef _pipeline;
    /// <summary>The renderer's dependency SET. Two bare calls to one entity still dedup --
    /// SourceReadOptions.Default is a singleton -- while two kwarg-bearing calls to one entity do not,
    /// because the record's collection members compare by reference. Harmless: DagCompiler groups by
    /// (connection, entity), and PZ0349 already refuses a dataset read by more than one pipeline.</summary>
    private readonly ISet<DepRef> _dependencies;
    private readonly List<PzError> _errors;

    public SourceFunction(PipelineDef pipeline, ISet<DepRef> dependencies, List<PzError> errors)
    {
        _pipeline = pipeline;
        _dependencies = dependencies;
        _errors = errors;
    }

    public int RequiredParameterCount => 2;

    public int ParameterCount => 2;

    /// <summary>Direct, not LastParameter — see <see cref="SinkFunction.VarParamKind"/>.</summary>
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
        var connection = arguments.Count > 0 ? arguments[0] as string : null;
        var entity = arguments.Count > 1 ? arguments[1] as string : null;
        if (connection is null || entity is null)
        {
            Error(PzErrorCode.UnresolvedRef, line,
                "source() expects (connection, entity) as two string arguments");
            return string.Empty;
        }

        if (EntityName.Problem(entity) is { } problem)
        {
            Error(PzErrorCode.EntityNameInvalid, line,
                $"source('{connection}', '{entity}'): entity name {problem}",
                "source('<connection>', 'schema.table')");
        }

        if (arguments.Count > 2)
        {
            Error(PzErrorCode.UnresolvedRef, line,
                $"source('{connection}', '{entity}') was passed {arguments.Count - 2} extra positional " +
                "argument(s) — read options are keyword arguments");
        }

        var read = ParseReadOptions(context, callerContext, connection, entity, line, out var declared);
        _dependencies.Add(new DepRef.Source(connection, entity, read, declared));
        return $"staging.{StagingName.ForSourceLoad(connection, entity)}";
    }

    public ValueTask<object?> InvokeAsync(TemplateContext context, ScriptNode? callerContext,
        ScriptArray arguments, ScriptBlockStatement? blockStatement) =>
        new(Invoke(context, callerContext, arguments, blockStatement));

    private SourceReadOptions ParseReadOptions(TemplateContext context, ScriptNode? callerContext,
        string connection, string entity, int line, out bool declaredAtCallSite)
    {
        var kwargs = ScriptKwargs.Read(context, callerContext, (name, argLine) =>
            Error(PzErrorCode.UnresolvedRef, argLine,
                $"source('{connection}', '{entity}') passes '{name}' more than once"));

        declaredAtCallSite = kwargs.Count > 0;
        RefuseMovedKwargs(kwargs, connection, entity);

        // The loader's own parsers, run against the kwarg map: same keys, same rules, same codes. They
        // report against a file with no line, so their errors are re-stamped with this call's line.
        var subErrors = new List<PzError>();
        var plain = kwargs.ToDictionary(kv => kv.Key, kv => kv.Value.Value, StringComparer.Ordinal);
        var sync = ProjectLoader.ParseSyncMode(plain, entity, _pipeline.FilePath, subErrors);
        var retry = ProjectLoader.ParseRetry(plain, _pipeline.FilePath, subErrors,
            $"source('{connection}', '{entity}') ");
        foreach (var error in subErrors)
        {
            _errors.Add(error with { Line = line });
        }

        var columns = ParseColumns(kwargs, connection, entity);

        var options = kwargs
            .Where(kv => kv.Key is not ("columns" or "sync" or "retry"))
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .ToDictionary(kv => kv.Key, kv => kv.Value.Value, StringComparer.Ordinal);

        return declaredAtCallSite
            ? new SourceReadOptions(columns, sync, retry, options)
            : SourceReadOptions.Default;
    }

    /// <summary>Names pz owns that must NOT ride through as connector read options: the instance-level
    /// keys, the retired entity qualifiers, and the removed <c>input:</c> binding.</summary>
    private void RefuseMovedKwargs(Dictionary<string, (object? Value, int Line)> kwargs,
        string connection, string entity)
    {
        foreach (var (name, code, hint) in new[]
        {
            ("rate_limit", PzErrorCode.RateLimitConfigInvalid,
                "rate_limit is instance-level; declare it on the connection in connections.yml"),
            ("max_concurrency", PzErrorCode.RateLimitConfigInvalid,
                "max_concurrency is instance-level; declare it on the connection in connections.yml"),
            ("table", PzErrorCode.RetiredEntityQualifier,
                "the entity name is the table: source('<connection>', 'schema.table')"),
            ("schema", PzErrorCode.RetiredEntityQualifier,
                "the entity name carries the schema: source('<connection>', 'schema.table')"),
            ("incremental", PzErrorCode.RetiredReadSurface,
                "sync: { mode: 'incremental', cursor: '<column>' }"),
        })
        {
            if (kwargs.Remove(name, out var arg))
            {
                Error(code, arg.Line,
                    $"source('{connection}', '{entity}'): '{name}' is not a source() keyword argument", hint);
            }
        }
    }

    private IReadOnlyDictionary<string, string>? ParseColumns(
        Dictionary<string, (object? Value, int Line)> kwargs, string connection, string entity)
    {
        if (!kwargs.TryGetValue("columns", out var arg))
        {
            return null;
        }

        if (arg.Value is Dictionary<string, object?> map)
        {
            return map.ToDictionary(kv => kv.Key, kv => kv.Value?.ToString() ?? string.Empty);
        }

        Error(PzErrorCode.YamlShape, arg.Line,
            $"source('{connection}', '{entity}'): 'columns' must be a mapping of column to type",
            "columns: { id: 'bigint', email: 'varchar' }");
        return null;
    }

    /// <summary>A pz-owned kwarg name <paramref name="option"/> is a near miss of, or null if it is
    /// plainly a connector option. The read-side twin of <see cref="SinkFunction.NearMissKwarg"/>.</summary>
    internal static string? NearMissKwarg(string option) =>
        ScriptKwargs.NearMiss(KnownKwargs, option);

    private void Error(string code, int line, string message, string? hint = null) =>
        _errors.Add(new PzError(code, $"{_pipeline.FilePath}: {message}.", _pipeline.FilePath, line,
            hint ?? CallHint));
}
