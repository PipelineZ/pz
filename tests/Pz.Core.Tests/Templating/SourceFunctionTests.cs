using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;

namespace Pz.Core.Tests.Templating;

/// <summary>Every read option is a keyword argument on the
/// source() call. The read-side twin of <see cref="SinkFunctionTests"/>, pinning the same two
/// properties: that pz reads the REAL kwarg names, and that a call passing none means exactly what an
/// entity with no `read:` block means.</summary>
public class SourceFunctionTests
{
    private static RenderResult Render(string sql)
    {
        var pipeline = new PipelineDef("p", sql, "table", [], [], "pipelines/p.sql");
        var project = new PzProject("t", "0.0.0", new EngineConfig(), new Dictionary<string, object?>(),
            [], [], [pipeline]);
        return TemplateRenderer.Render(pipeline,
            new RenderContext(project, "run-1", new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)));
    }

    private static DepRef.Source Source(string sql) =>
        Assert.IsType<DepRef.Source>(Assert.Single(Render(sql).Dependencies));

    private static IReadOnlyList<PzError> Errors(string sql) =>
        Assert.Throws<PzValidationException>(() => Render(sql)).Errors;

    [Fact]
    public void No_kwargs_means_no_call_site_declaration()
    {
        var dep = Source("select 1 from {{ source('crm', 'orders') }}");

        Assert.False(dep.DeclaredAtCallSite);
        Assert.Same(SourceReadOptions.Default, dep.Read);
    }

    [Fact]
    public void The_rendered_relation_is_unchanged()
    {
        var r = Render("select 1 from {{ source('crm', 'dbo.orders') }}");
        Assert.Contains("staging.src_crm__dbo_orders", r.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Known_kwargs_bind_by_name()
    {
        var dep = Source(
            "select 1 from {{ source('crm', 'orders', partition_column: 'id', partitions: 8, " +
            "columns: { id: 'bigint' }, sync: { mode: 'incremental', cursor: 'updated_at' }) }}");

        Assert.True(dep.DeclaredAtCallSite);
        Assert.Equal("id", dep.Read.Options["partition_column"]);
        Assert.Equal(8, dep.Read.Options["partitions"]);
        Assert.Equal("bigint", dep.Read.Columns!["id"]);
        Assert.Equal("updated_at", dep.Read.Sync!.Incremental!.Cursor);
        Assert.DoesNotContain("columns", dep.Read.Options.Keys);
        Assert.DoesNotContain("sync", dep.Read.Options.Keys);
    }

    [Fact]
    public void An_unrecognized_kwarg_rides_through_as_a_connector_option()
    {
        var dep = Source("select 1 from {{ source('lake', 'orders', format: 'csv', path: 'o.csv') }}");

        Assert.Equal("csv", dep.Read.Options["format"]);
        Assert.Equal("o.csv", dep.Read.Options["path"]);
    }

    [Fact]
    public void A_kwarg_that_skips_an_earlier_one_still_binds_to_its_own_name()
    {
        // The property the whole IScriptCustomFunction exists for: Scriban would have bound `partitions`
        // into the next free POSITIONAL slot.
        var dep = Source("select 1 from {{ source('crm', 'orders', partitions: 4) }}");

        Assert.Equal(4, dep.Read.Options["partitions"]);
        Assert.Null(dep.Read.Columns);
    }

    [Theory]
    [InlineData("rate_limit: { requests_per_minute: 60 }", PzErrorCode.RateLimitConfigInvalid)]
    [InlineData("max_concurrency: 4", PzErrorCode.RateLimitConfigInvalid)]
    [InlineData("table: 'orders'", PzErrorCode.RetiredEntityQualifier)]
    [InlineData("schema: 'dbo'", PzErrorCode.RetiredEntityQualifier)]
    [InlineData("incremental: { cursor: 'id' }", PzErrorCode.RetiredReadSurface)]
    public void A_moved_or_retired_kwarg_is_refused_by_its_own_code(string kwarg, string code) =>
        Assert.Single(Errors($"select 1 from {{{{ source('crm', 'orders', {kwarg}) }}}}"), e => e.Code == code);

    [Fact]
    public void A_malformed_entity_argument_is_PZ0344() =>
        Assert.Single(Errors("select 1 from {{ source('crm', 'raw..orders') }}"),
            e => e.Code == PzErrorCode.EntityNameInvalid);

    [Fact]
    public void Columns_must_be_a_mapping()
    {
        var error = Assert.Single(Errors("select 1 from {{ source('crm', 'orders', columns: ['id']) }}"));
        Assert.Equal(PzErrorCode.YamlShape, error.Code);
    }

    // The sub-blocks reuse the loader's own parsers, so their rules and codes are identical across the
    // two surfaces -- only the reported location differs, and it must be the call's line.
    [Fact]
    public void A_malformed_sync_kwarg_is_reported_against_the_call_line()
    {
        var error = Assert.Single(Errors(
            "-- lead\nselect 1 from {{ source('crm', 'orders', sync: { mode: 'bogus' }) }}"));

        Assert.Equal(PzErrorCode.SyncModeInvalid, error.Code);
        Assert.Equal("pipelines/p.sql", error.File);
        Assert.Equal(2, error.Line);
    }

    // Regression, found writing samples/mssql-mart against this surface: YAML scalars reach the loader's
    // parsers as long and Scriban kwargs as int, and the shared integer reader matched only long -- so
    // every well-formed integer written inside a call-site `retry:`/`sync:` block was refused.
    [Fact]
    public void A_call_site_retry_binds_its_integer()
    {
        var dep = Source("select 1 from {{ source('crm', 'orders', retry: { max_attempts: 3 }) }}");

        Assert.Equal(3, dep.Read.Retry!.MaxAttempts);
    }

    [Fact]
    public void A_malformed_retry_kwarg_reuses_the_loader_rule()
    {
        var error = Assert.Single(Errors(
            "select 1 from {{ source('crm', 'orders', retry: { max_attempts: 0 }) }}"));

        Assert.Equal(PzErrorCode.RetryConfigInvalid, error.Code);
    }

    [Fact]
    public void Every_malformed_call_is_reported_not_just_the_first()
    {
        var errors = Errors(
            "select 1 from {{ source('crm', 'a', sync: { mode: 'bogus' }) }} " +
            "join {{ source('crm', 'b', table: 'x') }} on true");

        Assert.Equal(2, errors.Count);
    }

    [Fact]
    public void Source_and_sink_errors_are_reported_in_one_pass()
    {
        var errors = Errors(
            "INSERT INTO {{ sink('lake', 'out', strategy: 'upsert') }}\n" +
            "select 1 from {{ source('crm', 'orders', table: 'x') }}");

        Assert.Single(errors, e => e.Code == PzErrorCode.SyncModeInvalid);
        Assert.Single(errors, e => e.Code == PzErrorCode.RetiredEntityQualifier);
    }
}
