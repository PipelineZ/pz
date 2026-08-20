using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;

namespace Pz.Core.Tests.Templating;

/// <summary>Every sink write option is a keyword argument on
/// the sink() call. These tests pin the two properties the surface rests on -- that pz reads the
/// REAL kwarg names (an unknown one can never be misbound to strategy) and that a call passing no
/// kwargs means exactly what an output with no write: block means.</summary>
public class SinkFunctionTests
{
    private static RenderResult Render(string sql)
    {
        var pipeline = new PipelineDef("p", sql, "table", [], [], "pipelines/p.sql");
        var project = new PzProject("t", "0.0.0", new EngineConfig(), new Dictionary<string, object?>(),
            [], [], [pipeline]);
        return TemplateRenderer.Render(pipeline,
            new RenderContext(project, "run-1", new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)));
    }

    private static IReadOnlyList<PzError> Errors(string sql) =>
        Assert.Throws<PzValidationException>(() => Render(sql)).Errors;

    // A numeric write option supplied by var() rather than written as a literal: vars come from the
    // YAML loader (or --vars) as long, literals from Scriban as int, so the retry parser must accept
    // both or `max_attempts: var('n')` is refused as "not an integer" whatever the var holds.
    [Fact]
    public void A_numeric_option_supplied_by_var_binds()
    {
        var pipeline = new PipelineDef("p",
            "INSERT INTO {{ sink('lake', 'c', retry: { max_attempts: var('n') }) }} select 1",
            "table", [], [], "pipelines/p.sql");
        var project = new PzProject("t", "0.0.0", new EngineConfig(),
            new Dictionary<string, object?> { ["n"] = 3L }, [], [], [pipeline]);

        var binding = Assert.Single(TemplateRenderer.Render(pipeline,
            new RenderContext(project, "run-1", DateTimeOffset.UnixEpoch)).InlineBindings);

        Assert.Equal(3, binding.Write.Retry!.MaxAttempts);
    }

    [Fact]
    public void No_kwargs_yields_the_retired_yaml_defaults()
    {
        var binding = Assert.Single(Render("INSERT INTO {{ sink('lake', 'orders') }} select 1").InlineBindings);
        Assert.Equal("lake", binding.Sink);
        Assert.Equal("orders", binding.Output);
        Assert.Equal("append", binding.Write.Mode);
        Assert.Equal("fail_on_change", binding.Write.SchemaPolicy);
        Assert.Empty(binding.Write.Keys);
        Assert.False(binding.Write.AcceptDuplicates);
        Assert.Null(binding.Write.OnDelete);
        Assert.Null(binding.Write.Retry);
        Assert.Empty(binding.Write.Options);
    }

    [Fact]
    public void Marker_text_is_unchanged_so_prefix_extraction_still_matches()
    {
        var r = Render("INSERT INTO {{ sink('lake', 'orders', strategy: 'replace') }} select 1");
        Assert.Contains("__pz_sink__lake__orders__", r.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Known_kwargs_bind_by_name()
    {
        var binding = Assert.Single(Render(
            "INSERT INTO {{ sink('mart', 'orders_current', strategy: 'merge', keys: ['order_id'], " +
            "on_delete: 'delete', schema_policy: 'allow_additive') }} select 1").InlineBindings);
        Assert.Equal("merge", binding.Write.Mode);
        Assert.Equal(["order_id"], binding.Write.Keys);
        Assert.Equal("delete", binding.Write.OnDelete);
        Assert.Equal("allow_additive", binding.Write.SchemaPolicy);
    }

    [Fact]
    public void A_kwarg_that_skips_an_earlier_one_still_binds_to_its_own_name()
    {
        // The delegate form bound by position once a name went unmatched; this is the case that proves
        // it no longer can.
        var binding = Assert.Single(Render(
            "INSERT INTO {{ sink('lake', 'o', duplicates: 'accept') }} select 1").InlineBindings);
        Assert.Equal("append", binding.Write.Mode);
        Assert.True(binding.Write.AcceptDuplicates);
    }

    [Fact]
    public void Unknown_kwargs_become_connector_options()
    {
        var binding = Assert.Single(Render(
            "INSERT INTO {{ sink('lake', 'curated', format: 'parquet', path: 'out/curated/') }} select 1")
            .InlineBindings);
        Assert.Equal("parquet", binding.Write.Options["format"]);
        Assert.Equal("out/curated/", binding.Write.Options["path"]);
        Assert.Equal("append", binding.Write.Mode); // NOT misbound into strategy
    }

    [Fact]
    public void List_and_map_option_values_convert_to_plain_clr_shapes()
    {
        var binding = Assert.Single(Render(
            "INSERT INTO {{ sink('lake', 'c', partition_by: ['dt'], extra: { a: 1 }) }} select 1")
            .InlineBindings);
        Assert.Equal(new List<object?> { "dt" }, Assert.IsType<List<object?>>(binding.Write.Options["partition_by"]));
        Assert.Equal(1, Assert.IsType<Dictionary<string, object?>>(binding.Write.Options["extra"])["a"]);
    }

    [Fact]
    public void Nested_retry_object_binds()
    {
        var binding = Assert.Single(Render(
            "INSERT INTO {{ sink('lake', 'c', retry: { max_attempts: 5, base_delay: '2s' }) }} select 1")
            .InlineBindings);
        Assert.Equal(5, binding.Write.Retry!.MaxAttempts);
        Assert.Equal(TimeSpan.FromSeconds(2), binding.Write.Retry.BaseDelay);
    }

    [Fact]
    public void Fan_out_records_one_binding_per_call_with_its_own_options()
    {
        var bindings = Render(
            "INSERT INTO [ {{ sink('lake', 'snap', format: 'parquet', strategy: 'replace') }}, " +
            "{{ sink('mart', 'cur', strategy: 'merge', keys: ['id']) }} ]\nselect 1").InlineBindings;
        Assert.Equal(2, bindings.Count);
        Assert.Equal("replace", bindings[0].Write.Mode);
        Assert.Equal("parquet", bindings[0].Write.Options["format"]);
        Assert.Equal("merge", bindings[1].Write.Mode);
        Assert.Equal(["id"], bindings[1].Write.Keys);
    }

    [Theory]
    [InlineData("{{ sink('lake', 'a', 'b') }}", "extra positional")]
    [InlineData("{{ sink('lake', 'a', strategy: 'upsert') }}", "replace, append, merge")]
    [InlineData("{{ sink('lake', 'a', keys: 'order_id') }}", "list of strings")]
    [InlineData("{{ sink('lake', 'a', duplicates: 'ignore') }}", "literal 'accept'")]
    [InlineData("{{ sink('lake', 'a', on_delete: 'purge') }}", "delete, soft, ignore")]
    [InlineData("{{ sink('lake', 'a', strategy: 'append', on_delete: 'delete') }}", "requires strategy: 'merge'")]
    [InlineData("{{ sink('lake', 'a', mode: 'merge') }}", "'mode' is not a sink() keyword argument")]
    [InlineData("{{ sink('lake', 'a', accept_duplicates: true) }}", "'accept_duplicates' is not a sink()")]
    [InlineData("{{ sink('lake', 'a', write: { strategy: 'merge' }) }}", "'write' is not a sink()")]
    [InlineData("{{ sink('lake', 'a', rate_limit: { requests_per_minute: 60 }) }}", "instance-level")]
    [InlineData("{{ sink('lake', 'a', input: 'p') }}", "'input' is not a sink()")]
    [InlineData("{{ sink('lake', 'a', retry: 3) }}", "'retry' must be a mapping")]
    [InlineData("{{ sink('lake', 'a', retry: { max_attempts: 0 }) }}", "max_attempts must be an integer >= 1")]
    [InlineData("{{ sink('lake', 'a', retry: { base_delay: 'soon' }) }}", "positive duration")]
    [InlineData("{{ sink('lake', 'a', retry: { bogus: 1 }) }}", "unknown retry key 'bogus'")]
    [InlineData("{{ sink('lake', 'a', strategy: 'merge', strategy: 'append') }}", "more than once")]
    public void Malformed_call_is_rejected(string call, string expected)
    {
        var joined = string.Join("\n", Errors($"INSERT INTO {call} select 1").Select(e => e.ToString()));
        Assert.Contains(expected, joined, StringComparison.Ordinal);
    }

    [Fact]
    public void Every_malformed_kwarg_is_reported_not_just_the_first()
    {
        var errors = Errors(
            "INSERT INTO {{ sink('lake', 'a', strategy: 'upsert', duplicates: 'ignore') }} select 1");
        Assert.Equal(2, errors.Count);
    }

    [Theory]
    [InlineData("table")]
    [InlineData("schema")]
    public void A_qualifier_kwarg_is_PZ0348_naming_the_entity_to_use(string kwarg)
    {
        var error = Assert.Single(Errors(
            $"INSERT INTO {{{{ sink('mart', 'orders', {kwarg}: 'dbo') }}}} select 1"));
        Assert.Equal(PzErrorCode.RetiredEntityQualifier, error.Code);
        Assert.Contains($"'{kwarg}'", error.Message, StringComparison.Ordinal);
        Assert.Contains("'schema.table'", error.Hint!, StringComparison.Ordinal);
    }

    [Fact]
    public void A_dotted_entity_is_the_output_name_verbatim()
    {
        var binding = Assert.Single(
            Render("INSERT INTO {{ sink('mart', 'mart.orders_current') }} select 1").InlineBindings);
        Assert.Equal("mart.orders_current", binding.Output);
    }

    [Theory]
    [InlineData("mart..orders")]
    [InlineData(".orders")]
    [InlineData("mart.")]
    public void A_malformed_entity_argument_is_PZ0344(string entity)
    {
        var error = Assert.Single(Errors($"INSERT INTO {{{{ sink('mart', '{entity}') }}}} select 1"));
        Assert.Equal(PzErrorCode.EntityNameInvalid, error.Code);
        Assert.Contains("empty dotted segment", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Errors_name_the_pipeline_file_and_the_line_of_the_call()
    {
        var error = Assert.Single(Errors(
            "-- leading comment\nINSERT INTO {{ sink('lake', 'a', strategy: 'upsert') }}\nselect 1"));
        Assert.Equal("pipelines/p.sql", error.File);
        Assert.Equal(2, error.Line);
    }
}
