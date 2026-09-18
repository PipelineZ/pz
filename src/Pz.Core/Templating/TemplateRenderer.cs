using System.Text.RegularExpressions;
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
        globals.Inner.SetValue("watermark", new ScalarFunction("watermark", ["source", "dataset"], args =>
        {
            var wmRef = new WatermarkRef(args[0], args[1]);
            watermarkRefs.Add(wmRef);
            return $"'{wmRef.Sentinel}'";
        }), readOnly: true);
        globals.Inner.SetValue("ref", new ScalarFunction("ref", ["pipeline"], args =>
        {
            var pipelineName = args[0];
            dependencies.Add(new DepRef.Pipeline(pipelineName));
            var target = ctx.Project.Pipelines.FirstOrDefault(p => p.Name == pipelineName);
            return target is { Materialization: "ephemeral" }
                ? $"__pz_cte__{pipelineName}"
                : $"staging.{pipelineName}";
        }), readOnly: true);
        globals.Inner.SetValue("var", new ScalarFunction("var", ["name"], args =>
            ctx.Project.Vars.TryGetValue(args[0], out var value)
                ? value
                : throw new ScriptRuntimeException(default, $"unknown var '{args[0]}'")), readOnly: true);
        globals.Inner.SetValue("env", new ScalarFunction("env", ["name"], args =>
            ctx.Env.TryGetValue(args[0], out var value)
                ? value
                : throw new ScriptRuntimeException(default, $"environment variable '{args[0]}' is not set")), readOnly: true);
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
            throw new PzValidationException([RuntimeErrorToPzError(pipeline, ex, ctx)]);
        }

        if (callErrors.Count > 0)
        {
            throw new PzValidationException(callErrors);
        }

        return new RenderResult(sql, dependencies) { InlineBindings = inlineBindings, WatermarkRefs = watermarkRefs };
    }

    private static readonly Regex UnknownVarPattern = new("^unknown var '(?<name>[^']+)'$", RegexOptions.Compiled);

    private static readonly Regex UndeclaredEnvVarPattern =
        new("^environment variable '(?<name>[^']+)' is not set$", RegexOptions.Compiled);

    private const string SandboxHint =
        "only source()/ref()/sink()/var()/env() and the this/run_id/run_started_at constants are " +
        "reachable inside {{ }} -- check this expression for a typo or an unsupported function/variable";

    /// <summary>A <c>source()</c>/<c>sink()</c> call is documented (<c>authoring-for-agents.md</c>) to
    /// fit on one line -- Scriban's own statement grammar is what actually enforces that, so a call an
    /// author split across lines fails as a raw, several-messages-deep parse error nobody unfamiliar
    /// with Scriban's grammar can act on. Detected against the raw SQL text (parsing itself already
    /// failed, so there is no parsed AST to inspect) rather than by pattern-matching the parse
    /// messages, which vary with exactly where the line break falls.</summary>
    private static PzError ParseErrorsToPzError(PipelineDef pipeline, Template template)
    {
        var line = template.Messages.Count > 0 ? template.Messages[0].Span.Start.Line + 1 : (int?)null;

        if (FindMultilineCall(pipeline.RawSql) is { } call)
        {
            return new PzError(PzErrorCode.TemplateError,
                $"pipeline '{pipeline.Name}' calls {call}() split across more than one line",
                pipeline.FilePath, line,
                $"put the whole {call}(...) call on one line -- whitespace/comments before it are fine, " +
                "but the call itself must not span a line break");
        }

        var message = string.Join("; ", template.Messages.Select(m => m.Message));
        return new PzError(PzErrorCode.TemplateError, message, pipeline.FilePath, line, SandboxHint);
    }

    /// <summary>The name of a <c>source</c>/<c>sink</c> call whose parenthesized argument list spans a
    /// line break in <paramref name="sql"/>, or null when neither does. Scans by balancing parentheses
    /// (skipping quoted string contents, so a literal containing '(' or a newline inside a string
    /// argument is not mistaken for the call's own structure) rather than a single regex, since the
    /// argument list itself is free-form.</summary>
    private static string? FindMultilineCall(string sql)
    {
        foreach (var name in new[] { "source", "sink" })
        {
            var searchFrom = 0;
            int start;
            while ((start = sql.IndexOf(name + "(", searchFrom, StringComparison.Ordinal)) >= 0)
            {
                var precededByIdentifierChar = start > 0 &&
                    (char.IsAsciiLetterOrDigit(sql[start - 1]) || sql[start - 1] == '_');
                var open = start + name.Length;
                if (!precededByIdentifierChar && InsideTemplateBlock(sql, start) &&
                    TryFindMatchingParen(sql, open, out var close) &&
                    sql.AsSpan(open, close - open).Contains('\n'))
                {
                    return name;
                }

                searchFrom = open + 1;
            }
        }

        return null;
    }

    /// <summary>Whether <paramref name="index"/> sits inside an open <c>{{</c> block. Everything outside
    /// one is SQL the template engine never parses, so a line break there cannot be what failed.</summary>
    private static bool InsideTemplateBlock(string sql, int index)
    {
        var opened = sql.LastIndexOf("{{", index, StringComparison.Ordinal);
        return opened >= 0 && sql.IndexOf("}}", opened, index - opened, StringComparison.Ordinal) < 0;
    }

    private static bool TryFindMatchingParen(string sql, int openIndex, out int closeIndex)
    {
        var depth = 0;
        var inString = false;
        var quote = '\0';
        for (var i = openIndex; i < sql.Length; i++)
        {
            var c = sql[i];
            if (inString)
            {
                if (c == quote) { inString = false; }
                continue;
            }

            switch (c)
            {
                case '\'' or '"':
                    inString = true;
                    quote = c;
                    break;
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    if (depth == 0)
                    {
                        closeIndex = i;
                        return true;
                    }

                    break;
            }
        }

        closeIndex = -1;
        return false;
    }

    private static PzError RuntimeErrorToPzError(PipelineDef pipeline, ScriptRuntimeException ex, RenderContext ctx)
    {
        var message = ex.OriginalMessage;
        var line = ex.Span.Start.Line + 1;

        if (UndeclaredEnvVarPattern.Match(message) is { Success: true } envMatch)
        {
            var name = envMatch.Groups["name"].Value;
            return new PzError(PzErrorCode.UndeclaredEnvVar, message, pipeline.FilePath, line,
                $"set the {name} environment variable before running pz, or remove env('{name}') from this pipeline");
        }

        if (UnknownVarPattern.Match(message) is { Success: true } varMatch)
        {
            var name = varMatch.Groups["name"].Value;
            var nearMiss = ScriptKwargs.NearMiss(ctx.Project.Vars.Keys, name);
            var hint = nearMiss is null
                ? $"declare '{name}' under project.yml's vars:, or pass it with --vars"
                : $"did you mean '{nearMiss}'?";
            return new PzError(PzErrorCode.TemplateError, message, pipeline.FilePath, line, hint);
        }

        return new PzError(PzErrorCode.TemplateError, message, pipeline.FilePath, line, SandboxHint);
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
