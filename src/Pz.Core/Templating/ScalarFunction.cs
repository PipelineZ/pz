using Scriban;
using Scriban.Runtime;
using Scriban.Syntax;

namespace Pz.Core.Templating;

/// <summary>A whitelisted scalar template function (<c>ref</c>/<c>var</c>/<c>env</c>/<c>watermark</c>)
/// backed by a plain delegate, implemented as an <see cref="IScriptCustomFunction"/> directly:
/// <c>ScriptObjectExtensions.Import(string, Delegate)</c> builds its binder via reflection over the
/// delegate's MethodInfo, which trimming/AOT cannot see through. Every parameter is a string and the
/// count is exact — the four functions this serves take nothing else.</summary>
internal sealed class ScalarFunction(string name, string[] parameters, Func<string[], object?> invoke)
    : IScriptCustomFunction
{
    public int RequiredParameterCount => parameters.Length;

    public int ParameterCount => parameters.Length;

    /// <summary>Direct, not LastParameter — see <see cref="SinkFunction.VarParamKind"/>.</summary>
    public ScriptVarParamKind VarParamKind => ScriptVarParamKind.Direct;

    public Type ReturnType => typeof(object);

    public ScriptParameterInfo GetParameterInfo(int index) =>
        new(typeof(string), index < parameters.Length ? parameters[index] : "arg");

    public object? Invoke(TemplateContext context, ScriptNode? callerContext, ScriptArray arguments,
        ScriptBlockStatement? blockStatement)
    {
        var span = callerContext?.Span ?? default;
        if (arguments.Count != parameters.Length)
        {
            throw new ScriptRuntimeException(span,
                $"{name}() expects {parameters.Length} argument(s) ({string.Join(", ", parameters)}), got {arguments.Count}");
        }

        var values = new string[parameters.Length];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = arguments[i] as string ?? throw new ScriptRuntimeException(span,
                $"{name}(): argument '{parameters[i]}' must be a string");
        }

        return invoke(values);
    }

    public ValueTask<object?> InvokeAsync(TemplateContext context, ScriptNode? callerContext,
        ScriptArray arguments, ScriptBlockStatement? blockStatement) =>
        new(Invoke(context, callerContext, arguments, blockStatement));
}
