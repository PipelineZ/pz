using Pz.Core.Model;
using Pz.Core.Validation;
using Scriban;
using Scriban.Runtime;
using Scriban.Syntax;

namespace Pz.Core.Templating;

/// <summary>
/// What <see cref="SinkFunction"/> and <see cref="SourceFunction"/> share: the <c>(connection, entity)</c>
/// positional signature, the error sink, and the refusal of kwargs that belong somewhere else. Both are
/// <see cref="IScriptCustomFunction"/>s so pz owns the binding of keyword arguments (see
/// <see cref="ScriptKwargs"/>); they differ only in their rule tables.
/// </summary>
internal abstract class KeywordCallFunction(PipelineDef pipeline, List<PzError> errors) : IScriptCustomFunction
{
    /// <summary>The template function name as the author types it, used to label every message.</summary>
    protected abstract string FunctionName { get; }

    /// <summary>"write" or "read": names the option family in the extra-positional-argument message.</summary>
    protected abstract string OptionKind { get; }

    /// <summary>The code for a malformed call (wrong positional arguments, repeated kwarg).</summary>
    protected abstract string MalformedCallCode { get; }

    /// <summary>Example call shown when an error supplies no hint of its own.</summary>
    protected abstract string CallHint { get; }

    protected PipelineDef Pipeline { get; } = pipeline;

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

    public abstract object? Invoke(TemplateContext context, ScriptNode? callerContext, ScriptArray arguments,
        ScriptBlockStatement? blockStatement);

    public ValueTask<object?> InvokeAsync(TemplateContext context, ScriptNode? callerContext,
        ScriptArray arguments, ScriptBlockStatement? blockStatement) =>
        new(Invoke(context, callerContext, arguments, blockStatement));

    /// <summary>Reads the two positional arguments and reports a bad entity name or surplus positionals.
    /// Returns false (after reporting) when either positional is missing or not a string.</summary>
    protected bool TryReadTarget(ScriptArray arguments, int line, out string connection, out string entity)
    {
        var connectionArg = arguments.Count > 0 ? arguments[0] as string : null;
        var entityArg = arguments.Count > 1 ? arguments[1] as string : null;
        if (connectionArg is null || entityArg is null)
        {
            Error(MalformedCallCode, line,
                $"{FunctionName}() expects (connection, entity) as two string arguments");
            connection = entity = string.Empty;
            return false;
        }

        connection = connectionArg;
        entity = entityArg;

        if (EntityName.Problem(entity) is { } problem)
        {
            Error(PzErrorCode.EntityNameInvalid, line,
                $"{FunctionName}('{connection}', '{entity}'): entity name {problem}",
                $"{FunctionName}('<connection>', 'schema.table')");
        }

        if (arguments.Count > 2)
        {
            Error(MalformedCallCode, line,
                $"{FunctionName}('{connection}', '{entity}') was passed {arguments.Count - 2} extra positional " +
                $"argument(s) — {OptionKind} options are keyword arguments");
        }

        return true;
    }

    protected void ReportDuplicateKwarg(string connection, string entity, string name, int line) =>
        Error(MalformedCallCode, line, $"{FunctionName}('{connection}', '{entity}') passes '{name}' more than once");

    /// <summary>Removes every kwarg pz owns but this call does not accept, reporting each under the
    /// code and hint supplied alongside its name.</summary>
    protected void RefuseKwargs(Dictionary<string, (object? Value, int Line)> kwargs, string connection,
        string entity, params (string Name, string Code, string Hint)[] refused)
    {
        foreach (var (name, code, hint) in refused)
        {
            if (kwargs.Remove(name, out var arg))
            {
                Error(code, arg.Line,
                    $"{FunctionName}('{connection}', '{entity}'): '{name}' is not a {FunctionName}() keyword argument",
                    hint);
            }
        }
    }

    /// <summary>Adds errors produced by a loader parser, which names a file but has no line, stamped
    /// with this call's line.</summary>
    protected void AddAtLine(IEnumerable<PzError> loaderErrors, int line)
    {
        foreach (var error in loaderErrors)
        {
            errors.Add(error with { Line = line });
        }
    }

    protected void Error(string code, int line, string message, string? hint = null) =>
        errors.Add(new PzError(code, $"{Pipeline.FilePath}: {message}.", Pipeline.FilePath, line,
            hint ?? CallHint));
}
