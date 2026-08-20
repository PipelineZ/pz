using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Scriban;
using Scriban.Parsing;
using Scriban.Runtime;
using Scriban.Syntax;

namespace Pz.Core.Templating;

/// <summary>
/// Renders a pipeline's SQL through a sandboxed Scriban template context: only the
/// whitelisted <c>source</c>/<c>ref</c>/<c>sink</c>/<c>var</c>/<c>env</c> functions and
/// <c>this</c>/<c>run_id</c>/<c>run_started_at</c> constants are reachable. All Scriban
/// builtin objects (date, string, object, array, io, ...) are stripped so they cannot be
/// invoked, and strict-variable mode turns any other unknown identifier into an error
/// rather than silently rendering empty output.
/// </summary>
public static class TemplateRenderer
{
    public static RenderResult Render(PipelineDef pipeline, RenderContext ctx)
    {
        var template = Template.Parse(pipeline.RawSql, pipeline.FilePath);
        if (template.HasErrors)
        {
            throw new PzValidationException([ParseErrorsToPzError(pipeline, template)]);
        }

        var dependencies = new HashSet<DepRef>();
        var inlineBindings = new List<InlineSinkBinding>();
        var watermarkRefs = new List<WatermarkRef>();
        var globals = new SandboxGlobals($"staging.{pipeline.Name}");

        // A custom function for the same reason sink() is one -- see ScriptKwargs. Its errors join
        // sink()'s in one post-render throw, so a single pass reports every malformed call on both
        // surfaces.
        var callErrors = new List<PzError>();
        globals.Inner.SetValue("source", new SourceFunction(pipeline, dependencies, callErrors), readOnly: true);
        // Records the binding (validated/exclusivity-checked later, in DagCompiler) and
        // renders a marker DagCompiler's prefix-extraction stage recognizes verbatim — no name
        // validation here, matching source()/ref()'s "record now, resolve later" pattern.
        // A custom function rather than an imported delegate, so pz sees the real keyword-argument names
        // instead of letting Scriban misbind them (see SinkFunction).
        // Its kwarg errors accumulate in `sinkErrors` and are thrown together AFTER the render, so one
        // pass reports every malformed call rather than stopping at the first.
        globals.Inner.SetValue("sink", new SinkFunction(pipeline, inlineBindings, callErrors), readOnly: true);
        // Records the reference (shape-validated later, in DagCompiler/WatermarkInference) and
        // renders a deterministic quoted sentinel — "record now, resolve later", same as sink().
        globals.Inner.Import("watermark", new Func<string, string, string>((sourceName, dataset) =>
        {
            var wmRef = new WatermarkRef(sourceName, dataset);
            watermarkRefs.Add(wmRef);
            return $"'{wmRef.Sentinel}'";
        }));
        globals.Inner.Import("ref", new Func<string, string>(pipelineName =>
        {
            dependencies.Add(new DepRef.Pipeline(pipelineName));
            var target = ctx.Project.Pipelines.FirstOrDefault(p => p.Name == pipelineName);
            return target is { Materialization: "ephemeral" }
                ? $"__pz_cte__{pipelineName}"
                : $"staging.{pipelineName}";
        }));
        globals.Inner.Import("var", new Func<string, object?>(name =>
            ctx.Project.Vars.TryGetValue(name, out var value)
                ? value
                : throw new ScriptRuntimeException(default, $"unknown var '{name}'")));
        globals.Inner.Import("env", new Func<string, string>(name =>
            ctx.Env.TryGetValue(name, out var value)
                ? value
                : throw new ScriptRuntimeException(default, $"environment variable '{name}' is not set")));
        globals.Inner.SetValue("run_id", ctx.RunId, readOnly: true);
        globals.Inner.SetValue("run_started_at", ctx.RunStartedAt.ToString("O"), readOnly: true);

        var templateContext = new TemplateContext { StrictVariables = true };
        templateContext.BuiltinObject.Clear();
        templateContext.PushGlobal(globals);

        string sql;
        try
        {
            sql = template.Render(templateContext);
        }
        catch (ScriptRuntimeException ex)
        {
            throw new PzValidationException([RuntimeErrorToPzError(pipeline, ex)]);
        }

        if (callErrors.Count > 0)
        {
            throw new PzValidationException(callErrors);
        }

        return new RenderResult(sql, dependencies) { InlineBindings = inlineBindings, WatermarkRefs = watermarkRefs };
    }

    private static PzError ParseErrorsToPzError(PipelineDef pipeline, Template template)
    {
        var message = string.Join("; ", template.Messages.Select(m => m.Message));
        var line = template.Messages.Count > 0 ? template.Messages[0].Span.Start.Line + 1 : (int?)null;
        return new PzError(PzErrorCode.TemplateError, message, pipeline.FilePath, line, null);
    }

    private static PzError RuntimeErrorToPzError(PipelineDef pipeline, ScriptRuntimeException ex)
    {
        var message = ex.OriginalMessage;
        var isUndeclaredEnvVar = message.Contains("environment variable", StringComparison.Ordinal)
            && message.Contains("is not set", StringComparison.Ordinal);
        var code = isUndeclaredEnvVar ? PzErrorCode.UndeclaredEnvVar : PzErrorCode.TemplateError;
        return new PzError(code, message, pipeline.FilePath, ex.Span.Start.Line + 1, null);
    }

    /// <summary>
    /// The Scriban global object pushed for rendering. <c>this</c> is a reserved Scriban
    /// keyword that always evaluates to <see cref="TemplateContext.CurrentGlobal"/> (the
    /// pushed global object itself) rather than a lookup by the key "this" — Scriban
    /// stringifies that value with <see cref="object.ToString"/> when writing output, so
    /// overriding <see cref="ToString"/> here is what makes <c>{{ this }}</c> render as
    /// <c>staging.&lt;pipeline&gt;</c> instead of a dump of the global object's members.
    /// All other member access (source/ref/var/env/run_id/run_started_at) is delegated to
    /// an inner <see cref="ScriptObject"/>.
    /// </summary>
    private sealed class SandboxGlobals : IScriptObject
    {
        private readonly string _thisValue;

        public SandboxGlobals(string thisValue) => _thisValue = thisValue;

        public ScriptObject Inner { get; } = new();

        public override string ToString() => _thisValue;

        public int Count => Inner.Count;

        public bool IsReadOnly
        {
            get => Inner.IsReadOnly;
            set => Inner.IsReadOnly = value;
        }

        public IEnumerable<string> GetMembers() => Inner.GetMembers();

        public bool Contains(string member) => Inner.Contains(member);

        public bool TryGetValue(TemplateContext context, SourceSpan span, string member, out object? value) =>
            Inner.TryGetValue(context, span, member, out value);

        public bool CanWrite(string member) => Inner.CanWrite(member);

        public bool TrySetValue(TemplateContext context, SourceSpan span, string member, object? value, bool readOnly) =>
            Inner.TrySetValue(context, span, member, value, readOnly);

        public bool Remove(string member) => Inner.Remove(member);

        public void SetReadOnly(string member, bool readOnly) => Inner.SetReadOnly(member, readOnly);

        public IScriptObject Clone(bool deep) => this;
    }
}
