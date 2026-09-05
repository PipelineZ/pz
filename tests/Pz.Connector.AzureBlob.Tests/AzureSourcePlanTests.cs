using Pz.Connectors.Abstractions;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary>Offline, pure test of <see cref="AzureSource.MatchGlob"/> -- the wildcard-matching logic
/// factored out so glob→partition mapping can be verified without a network/Azurite dependency (the SDK's
/// own server-side prefix narrowing is exercised only by the Azurite e2e suite).</summary>
public sealed class AzureSourcePlanTests
{
    private static readonly string[] Names = ["in/a.parquet", "in/b.parquet", "in/c.csv", "other/d.parquet"];

    [Fact]
    public void CoverPrefixes_templated_returns_per_element_static_prefixes()
    {
        var spec = new DatasetSpec("s", "e", new Dictionary<string, object?>())
        {
            WatermarkCursor = "t", WatermarkValue = "2026-07-11T00:00:00.000000",
            WatermarkUpperBound = "2026-07-12T00:00:00.000000",
        };
        var prefixes = AzureSource.CoverPrefixesForTest("events/{yyyy}/{MM}/{dd}/*.parquet", spec);
        Assert.Equal(["events/2026/07/11/", "events/2026/07/12/"], prefixes);
    }

    [Fact]
    public void CoverPrefixes_non_templated_returns_single_static_prefix()
    {
        var spec = new DatasetSpec("s", "e", new Dictionary<string, object?>());
        var prefixes = AzureSource.CoverPrefixesForTest("in/*.parquet", spec);
        Assert.Equal(["in/"], prefixes);
    }

    [Fact]
    public void MatchGlob_selects_matching_blob_names()
    {
        var matched = AzureSource.MatchGlob(Names, "in/*.parquet");
        Assert.Equal(["in/a.parquet", "in/b.parquet"], matched.OrderBy(x => x, StringComparer.Ordinal));
    }

    [Fact]
    public void MatchGlob_single_star_does_not_cross_slash_boundaries()
    {
        var names = new[] { "in/a.parquet", "in/nested/b.parquet" };
        var matched = AzureSource.MatchGlob(names, "in/*.parquet");
        Assert.Equal(["in/a.parquet"], matched);
    }

    [Fact]
    public void MatchGlob_double_star_crosses_slash_boundaries()
    {
        var names = new[] { "in/a.parquet", "in/nested/b.parquet", "other/c.parquet" };
        var matched = AzureSource.MatchGlob(names, "in/**.parquet");
        Assert.Equal(["in/a.parquet", "in/nested/b.parquet"], matched.OrderBy(x => x, StringComparer.Ordinal));
    }

    [Fact]
    public void MatchGlob_question_mark_matches_exactly_one_non_slash_character()
    {
        var names = new[] { "in/a.csv", "in/ab.csv", "in/a/b.csv" };
        var matched = AzureSource.MatchGlob(names, "in/?.csv");
        Assert.Equal(["in/a.csv"], matched);
    }

    [Fact]
    public void MatchGlob_exact_pattern_with_no_wildcard_matches_only_itself()
    {
        var names = new[] { "in/a.parquet", "in/ab.parquet" };
        var matched = AzureSource.MatchGlob(names, "in/a.parquet");
        Assert.Equal(["in/a.parquet"], matched);
    }

    [Fact]
    public void MatchGlob_escapes_regex_metacharacters_in_literal_segments()
    {
        var names = new[] { "in/a(1).parquet", "in/a1.parquet" };
        var matched = AzureSource.MatchGlob(names, "in/a(1).parquet");
        Assert.Equal(["in/a(1).parquet"], matched);
    }

    [Fact]
    public void MatchGlob_no_match_returns_empty()
    {
        var matched = AzureSource.MatchGlob(Names, "nowhere/*.parquet");
        Assert.Empty(matched);
    }

    // PlanReadAsync is a refusal stub that preserves error quality — csv/tsv/json without a
    // columns: contract still gets the "declare a columns contract" error
    // (native scan DECLINES that case, so this stub is the only owner of the message), everything else
    // names the native-only refusal (ParquetSource precedent).

    [Fact]
    public async Task PlanRead_csv_without_columns_contract_names_the_contract_error()
    {
        var source = new AzureSource(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["auth"] = "connection_string", ["connection_string"] = "UseDevelopmentStorage=true",
        }));
        var spec = new DatasetSpec("src", "orders", new Dictionary<string, object?>
        {
            ["container"] = "lake", ["path"] = "in/*.csv", ["format"] = "csv",
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.PlanReadAsync(spec, new ReadHints(), CancellationToken.None));

        Assert.Contains("columns", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task PlanRead_tsv_without_columns_contract_names_the_same_contract_error_as_csv()
    {
        var source = new AzureSource(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["auth"] = "connection_string", ["connection_string"] = "UseDevelopmentStorage=true",
        }));
        var spec = new DatasetSpec("src", "orders", new Dictionary<string, object?>
        {
            ["container"] = "lake", ["path"] = "in/*.tsv", ["format"] = "tsv",
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.PlanReadAsync(spec, new ReadHints(), CancellationToken.None));

        Assert.Contains("columns", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("PZ0312", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task PlanRead_parquet_names_the_native_only_refusal()
    {
        var source = new AzureSource(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["auth"] = "connection_string", ["connection_string"] = "UseDevelopmentStorage=true",
        }));
        var spec = new DatasetSpec("src", "orders", new Dictionary<string, object?>
        {
            ["container"] = "lake", ["path"] = "in/*.parquet",
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.PlanReadAsync(spec, new ReadHints(), CancellationToken.None));

        Assert.Contains("PZ0312", ex.Message, StringComparison.Ordinal);
        Assert.Contains("native-scan only", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }
}
