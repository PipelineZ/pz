using System.Text.Json;
using Pz.Cli.Commands;
using Pz.Core.Validation;
using Pz.Engine.Preview;
using Pz.Engine.Staging;

namespace Pz.Cli.Tests;

/// <summary>`pz staging query`'s option rules (PZ0544) and what it prints; the query itself is covered by
/// StagingQueryRunnerTests.</summary>
public class StagingCommandTests
{
    private static StagingQueryArgs Args(string? run = "20261123T120000Z-ab12", string? runDir = null,
        string? sql = "select 1", int limit = 50) => new(run, runDir, sql, limit, Json: true, "/project");

    [Fact]
    public void A_run_id_and_a_query_are_valid() => Assert.Null(StagingCommand.ValidateOptions(Args()));

    [Theory]
    [InlineData(null, null, "select 1", "exactly one of --run")]
    [InlineData("r1", "/runs/r1", "select 1", "exactly one of --run")]
    [InlineData("../other", null, "select 1", "plain directory name")]
    [InlineData("r1", null, null, "--sql")]
    [InlineData("r1", null, "  ;  ", "--sql")]
    public void Option_rules(string? run, string? runDir, string? sql, string expected)
    {
        var error = StagingCommand.ValidateOptions(Args(run, runDir, sql));
        Assert.Equal(PzErrorCode.StagingQueryOptionsInvalid, error!.Code);
        Assert.Contains(expected, error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(501)]
    public void Limit_must_be_1_to_500(int limit) =>
        Assert.Contains("--limit", StagingCommand.ValidateOptions(Args(limit: limit))!.Message, StringComparison.Ordinal);

    private static StagingQueryResult Result(bool truncated = false) => new("r1",
        [new PreviewColumn("name", "VARCHAR", false), new PreviewColumn("total", "DOUBLE", false)],
        [[new PreviewCell(PreviewCellKind.Text, "ann"), new PreviewCell(PreviewCellKind.Number, "6.5")],
         [new PreviewCell(PreviewCellKind.Null, null), new PreviewCell(PreviewCellKind.Number, "1")]],
        50, truncated);

    [Fact]
    public void Json_is_one_object_in_contract_order()
    {
        var json = StagingQueryOutput.ToJson(Result());

        Assert.Equal(
            """{"runId":"r1","columns":[{"name":"name","type":"VARCHAR"},{"name":"total","type":"DOUBLE"}],"rows":[["ann",6.5],[null,1]],"limit":50,"truncated":false}""",
            json);
        using var _ = JsonDocument.Parse(json);
    }

    [Fact]
    public void The_table_says_when_rows_were_cut()
    {
        Assert.EndsWith("2 rows\n", StagingQueryOutput.ToTable(Result()), StringComparison.Ordinal);
        Assert.Contains("the query returned more", StagingQueryOutput.ToTable(Result(truncated: true)), StringComparison.Ordinal);
        Assert.Contains("NULL", StagingQueryOutput.ToTable(Result()), StringComparison.Ordinal);
    }
}
