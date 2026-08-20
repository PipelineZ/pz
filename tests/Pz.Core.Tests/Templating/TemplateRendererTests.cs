using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;

namespace Pz.Core.Tests.Templating;

public class TemplateRendererTests
{
    private static PipelineDef Pipe(string name, string sql, string materialization = "table") =>
        new(name, sql, materialization, [], [], $"pipelines/{name}.sql");

    private static RenderContext Ctx(params PipelineDef[] pipelines)
    {
        var project = new PzProject("t", "0.0.0", new EngineConfig(),
            new Dictionary<string, object?>
            {
                ["min_amount"] = 10L,
                ["statuses"] = new List<object?> { "shipped", "returned" },
            },
            [], [], pipelines);
        return new RenderContext(project, "run-1",
            new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero))
        {
            Env = new Dictionary<string, string> { ["REGION"] = "eu-west-1" },
        };
    }

    [Fact]
    public void Source_call_renders_staging_name_and_records_dependency()
    {
        var p = Pipe("x", "select * from {{ source('crm', 'orders') }}");
        var r = TemplateRenderer.Render(p, Ctx(p));
        Assert.Contains("staging.src_crm__orders", r.Sql);
        Assert.Contains(new DepRef.Source("crm", "orders"), r.Dependencies);
    }

    [Fact]
    public void Ref_call_records_pipeline_dependency()
    {
        var p = Pipe("x", "select * from {{ ref('stg_orders') }}");
        var r = TemplateRenderer.Render(p, Ctx(p));
        Assert.Contains("staging.stg_orders", r.Sql);
        Assert.Contains(new DepRef.Pipeline("stg_orders"), r.Dependencies);
    }

    [Fact]
    public void Ephemeral_ref_renders_cte_alias_not_staging()
    {
        var eph = Pipe("eph", "select 1 as x", materialization: "ephemeral");
        var p = Pipe("consumer", "select * from {{ ref('eph') }}");
        var r = TemplateRenderer.Render(p, Ctx(eph, p));
        Assert.Equal("select * from __pz_cte__eph", r.Sql);
        Assert.Contains(new DepRef.Pipeline("eph"), r.Dependencies);
    }

    [Fact]
    public void Sink_call_renders_marker_and_records_binding()
    {
        var p = Pipe("x", "INSERT INTO {{ sink('lake', 'totals', strategy: 'replace', format: 'parquet') }} select 1");
        var r = TemplateRenderer.Render(p, Ctx(p));
        Assert.Contains("__pz_sink__lake__totals__", r.Sql);
        // Compared field-by-field, not by record equality: InlineSinkBinding now carries
        // SinkWriteOptions, whose collection members compare by reference (see SinkFunctionTests for
        // the option-level assertions).
        var binding = Assert.Single(r.InlineBindings);
        Assert.Equal("lake", binding.Sink);
        Assert.Equal("totals", binding.Output);
    }

    [Fact]
    public void Sink_is_not_reachable_as_variable()
    {
        var p = Pipe("x", "select {{ sink }}");
        Assert.Throws<PzValidationException>(() => TemplateRenderer.Render(p, Ctx(p)));
    }

    [Fact]
    public void Var_substitutes_value()
    {
        var p = Pipe("x", "select * from t where amount >= {{ var('min_amount') }}");
        var r = TemplateRenderer.Render(p, Ctx(p));
        Assert.Contains(">= 10", r.Sql);
    }

    [Fact]
    public void Env_function_resolves_declared_variable()
    {
        var p = Pipe("x", "select '{{ env('REGION') }}' as region");
        var r = TemplateRenderer.Render(p, Ctx(p));
        Assert.Contains("'eu-west-1'", r.Sql);
    }

    [Fact]
    public void Unknown_function_is_validation_error()
    {
        var p = Pipe("x", "select {{ date.now }}");
        var ex = Assert.Throws<PzValidationException>(() => TemplateRenderer.Render(p, Ctx(p)));
        var error = Assert.Single(ex.Errors);
        Assert.Equal(PzErrorCode.TemplateError, error.Code);
        Assert.Contains("pipelines/x.sql", error.File);
    }

    [Fact]
    public void Loops_over_vars_render_deterministically()
    {
        var sql = "select 1 as x{{ for s in var('statuses') }} union select '{{ s }}'{{ end }}";
        var p = Pipe("x", sql);
        var first = TemplateRenderer.Render(p, Ctx(p));
        var second = TemplateRenderer.Render(p, Ctx(p));
        Assert.Contains("'shipped'", first.Sql);
        Assert.Contains("'returned'", first.Sql);
        Assert.Equal(first.Sql, second.Sql);
    }

    [Fact]
    public void Same_context_renders_identical_output_twice()
    {
        var p = Pipe("x",
            "select '{{ run_id }}', '{{ run_started_at }}', * from {{ this }}");
        var first = TemplateRenderer.Render(p, Ctx(p));
        var second = TemplateRenderer.Render(p, Ctx(p));
        Assert.Equal(first.Sql, second.Sql);
        Assert.Contains("staging.x", first.Sql);
        Assert.Contains("run-1", first.Sql);
        Assert.Contains("2026-01-01T00:00:00.0000000+00:00", first.Sql);
    }

    [Fact]
    public void Watermark_call_renders_sentinel_and_records_ref()
    {
        var pipeline = new PipelineDef("p", "select * from {{ source('crm','orders') }} o where o.u > {{ watermark('crm','orders') }}",
            "table", [], [], "pipelines/p.sql");
        var result = TemplateRenderer.Render(pipeline, Ctx());

        Assert.Contains("'__pz_watermark__crm__orders__'", result.Sql, StringComparison.Ordinal);
        var wmRef = Assert.Single(result.WatermarkRefs);
        Assert.Equal("crm", wmRef.SourceName);
        Assert.Equal("orders", wmRef.Dataset);
    }

    [Fact]
    public void Watermark_call_repeats_are_each_recorded()
    {
        var pipeline = new PipelineDef("p",
            "select * from {{ source('a','x') }} where c1 > {{ watermark('a','x') }} and c1 > {{ watermark('a','x') }} - 5",
            "table", [], [], "pipelines/p.sql");
        var result = TemplateRenderer.Render(pipeline, Ctx());
        Assert.Equal(2, result.WatermarkRefs.Count);
    }
}
