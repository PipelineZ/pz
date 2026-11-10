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
/// so an imported delegate cannot tell a typo from a value. See <see cref="ScriptKwargs"/> for the
/// full rationale.
///
/// The sub-blocks (<c>sync:</c>, <c>retry:</c>, <c>columns:</c>) are parsed by the SAME loader code the
/// YAML surface uses. Scriban object literals convert to the plain CLR shapes YamlMapper produces, so
/// <c>ProjectLoader.ParseSyncMode</c>/<c>ParseRetry</c> accept them unchanged — one rule table, one set
/// of messages, and no chance of the two surfaces drifting. Only the location is re-stamped: a loader
/// error names a file with no line, and this call site has one.
/// </summary>
internal sealed class SourceFunction : KeywordCallFunction
{
    /// <summary>Kwargs pz owns. Everything else is a connector read option and rides
    /// <see cref="SourceReadOptions.Options"/> unchecked, exactly as an unrecognized key under a YAML
    /// <c>read:</c> block does. <c>partition_column</c>/<c>partitions</c> are listed because they are
    /// pz-documented names worth a near-miss warning, but they are NOT lifted out — the connectors read
    /// them straight off the options bag.</summary>
    internal static readonly string[] KnownKwargs =
        ["columns", "sync", "retry", "partition_column", "partitions"];

    /// <summary>The renderer's dependency SET. Two bare calls to one entity still dedup --
    /// SourceReadOptions.Default is a singleton -- while two kwarg-bearing calls to one entity do not,
    /// because the record's collection members compare by reference. Harmless: DagCompiler groups by
    /// (connection, entity), and PZ0349 already refuses a dataset read by more than one pipeline.</summary>
    private readonly ISet<DepRef> _dependencies;

    public SourceFunction(PipelineDef pipeline, ISet<DepRef> dependencies, List<PzError> errors)
        : base(pipeline, errors)
    {
        _dependencies = dependencies;
    }

    protected override string FunctionName => "source";

    protected override string OptionKind => "read";

    protected override string MalformedCallCode => PzErrorCode.UnresolvedRef;

    protected override string CallHint =>
        "source('<connection>', '<entity>', partitions: 8, sync: { mode: 'incremental', cursor: '<column>' })";

    public override object? Invoke(TemplateContext context, ScriptNode? callerContext, ScriptArray arguments,
        ScriptBlockStatement? blockStatement)
    {
        var line = (callerContext?.Span.Start.Line ?? 0) + 1;
        if (!TryReadTarget(arguments, line, out var connection, out var entity))
        {
            return string.Empty;
        }

        var read = ParseReadOptions(context, callerContext, connection, entity, line, out var declared);
        _dependencies.Add(new DepRef.Source(connection, entity, read, declared));
        return $"staging.{StagingName.ForSourceLoad(connection, entity)}";
    }

    private SourceReadOptions ParseReadOptions(TemplateContext context, ScriptNode? callerContext,
        string connection, string entity, int line, out bool declaredAtCallSite)
    {
        var kwargs = ScriptKwargs.Read(context, callerContext, (name, argLine) =>
            ReportDuplicateKwarg(connection, entity, name, argLine));

        declaredAtCallSite = kwargs.Count > 0;
        RefuseMovedKwargs(kwargs, connection, entity);

        // The loader's own parsers, run against the kwarg map: same keys, same rules, same codes. They
        // report against a file with no line, so their errors are re-stamped with this call's line.
        var subErrors = new List<PzError>();
        var plain = kwargs.ToDictionary(kv => kv.Key, kv => kv.Value.Value, StringComparer.Ordinal);
        var sync = ProjectLoader.ParseSyncMode(plain, entity, Pipeline.FilePath, subErrors);
        var retry = ProjectLoader.ParseRetry(plain, Pipeline.FilePath, subErrors,
            $"source('{connection}', '{entity}') ");
        AddAtLine(subErrors, line);

        var columns = ParseColumns(kwargs, connection, entity, line);
        if (ColumnContract.RefuseHandWritten(kwargs.Keys, $"source('{connection}', '{entity}')", Pipeline.FilePath)
            is { } handWritten)
        {
            AddAtLine([handWritten], line);
        }

        var options = kwargs
            .Where(kv => kv.Key is not ("columns" or "sync" or "retry" or ColumnContract.PointersOption))
            .ToDictionary(kv => kv.Key, kv => kv.Value.Value, StringComparer.Ordinal);
        columns?.AddPointersTo(options);
        options = options.OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);

        return declaredAtCallSite
            ? new SourceReadOptions(columns?.Types, sync, retry, options) { RedactedColumns = columns?.Redacted ?? [] }
            : SourceReadOptions.Default;
    }

    /// <summary>Names pz owns that must NOT ride through as connector read options: the instance-level
    /// keys, the retired entity qualifiers, and the removed <c>input:</c> binding.</summary>
    private void RefuseMovedKwargs(Dictionary<string, (object? Value, int Line)> kwargs,
        string connection, string entity)
    {
        RefuseKwargs(kwargs, connection, entity,
            ("rate_limit", PzErrorCode.RateLimitConfigInvalid,
                "rate_limit is instance-level; declare it on the connection in connections.yml"),
            ("max_concurrency", PzErrorCode.ConcurrencyConfigInvalid,
                "max_concurrency is instance-level; declare it on the connection in connections.yml"),
            ("table", PzErrorCode.RetiredEntityQualifier,
                "the entity name is the table: source('<connection>', 'schema.table')"),
            ("schema", PzErrorCode.RetiredEntityQualifier,
                "the entity name carries the schema: source('<connection>', 'schema.table')"),
            ("incremental", PzErrorCode.RetiredReadSurface,
                "sync: { mode: 'incremental', cursor: '<column>' }"));
    }

    private ColumnContract? ParseColumns(
        Dictionary<string, (object? Value, int Line)> kwargs, string connection, string entity, int line)
    {
        if (!kwargs.TryGetValue("columns", out var arg))
        {
            return null;
        }

        if (arg.Value is Dictionary<string, object?> map)
        {
            // The loader's parser, so both surfaces share the long form and its rules; its errors carry
            // no line, so they are re-stamped with this call's.
            var subErrors = new List<PzError>();
            var contract = ColumnContractParser.Parse(map, $"source('{connection}', '{entity}')",
                Pipeline.FilePath, subErrors);
            AddAtLine(subErrors, line);

            return contract;
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
}
