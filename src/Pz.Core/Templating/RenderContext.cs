using Pz.Core.Model;

namespace Pz.Core.Templating;

public sealed record RenderContext(PzProject Project, string RunId, DateTimeOffset RunStartedAt)
{
    public IReadOnlyDictionary<string, string> Env { get; init; } = new Dictionary<string, string>();
}

/// <summary>
/// <see cref="InlineBindings"/> carries every <c>sink(&lt;sink&gt;, &lt;output&gt;)</c> call a pipeline's
/// template recorded — separate from <see cref="Dependencies"/> because a sink binding is
/// not a DAG dependency edge, it's a claim on a sink output that <c>DagCompiler</c> resolves/validates
/// (PZ0201/PZ0206/PZ0207/PZ0208) once every pipeline has rendered.
/// </summary>
public sealed record RenderResult(string Sql, IReadOnlySet<DepRef> Dependencies)
{
    public IReadOnlyList<InlineSinkBinding> InlineBindings { get; init; } = [];
    public IReadOnlyList<WatermarkRef> WatermarkRefs { get; init; } = [];
}

/// <summary>One <c>sink(&lt;sink&gt;, &lt;output&gt;)</c> call recorded by <see cref="TemplateRenderer"/>.
/// Names are carried verbatim (not re-derived from the rendered marker text) so sink/output names
/// containing underscores round-trip exactly — see <c>DagCompiler</c>'s prefix-extraction stage.
///
/// <see cref="Write"/> carries the call's keyword arguments, which is where every write option lives.
/// It rides this record rather than the rendered marker so the marker text — and therefore prefix
/// extraction and the bracket fan-out form — stays independent of the write options. Defaulted to
/// <see cref="SinkWriteOptions.Default"/>, so a call that passes no kwargs takes the write
/// defaults.</summary>
public sealed record InlineSinkBinding(string Sink, string Output, SinkWriteOptions Write,
    bool DeclaredAtCallSite = false)
{
    public InlineSinkBinding(string Sink, string Output)
        : this(Sink, Output, SinkWriteOptions.Default)
    {
    }
}

/// <summary>One watermark(<source>, <dataset>) call recorded by TemplateRenderer. The
/// sentinel is the deterministic string literal the call renders to — DagCompiler's inference and
/// the executors' substitution both key on it verbatim.</summary>
public sealed record WatermarkRef(string SourceName, string Dataset)
{
    public string Sentinel => $"__pz_watermark__{SourceName}__{Dataset}__";
}

public abstract record DepRef
{
    private DepRef() { }
    /// <summary><paramref name="Read"/> carries the read options the call site declared, and
    /// <paramref name="DeclaredAtCallSite"/> records whether the author typed ANY kwarg -- DagCompiler
    /// needs the distinction, not the parsed value, because a kwarg equal to the default is still a
    /// declaration and colliding with `entities: &lt;e&gt;: read:` is PZ0341.</summary>
    public sealed record Source(string SourceName, string Dataset, SourceReadOptions Read,
        bool DeclaredAtCallSite) : DepRef
    {
        public Source(string SourceName, string Dataset)
            : this(SourceName, Dataset, SourceReadOptions.Default, false)
        {
        }
    }
    public sealed record Pipeline(string Name) : DepRef;
}
