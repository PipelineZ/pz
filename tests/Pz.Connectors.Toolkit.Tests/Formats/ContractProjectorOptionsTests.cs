using System.Text.Json.Nodes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connectors.Toolkit.Tests.Formats;

public class ContractProjectorOptionsTests
{
    private static readonly DateTimeOffset Epoch1700000000 = DateTimeOffset.FromUnixTimeSeconds(1_700_000_000);

    private static object? Convert(string json, ContractReadOptions options, string type = "timestamp")
        => ContractProjector.ConvertScalar(JsonNode.Parse(json), type, "created", "dataset 'x'", options);

    [Fact]
    public void Epoch_seconds_number_is_a_timestamp()
    {
        var options = new ContractReadOptions { EpochUnit = EpochUnit.Seconds };
        Assert.Equal(Epoch1700000000, Convert("1700000000", options));
    }

    [Fact]
    public void Epoch_milliseconds_number_is_a_timestamp()
    {
        var options = new ContractReadOptions { EpochUnit = EpochUnit.Milliseconds };
        Assert.Equal(Epoch1700000000.AddMilliseconds(123), Convert("1700000000123", options));
    }

    [Fact]
    public void Fractional_epoch_keeps_sub_second_precision()
    {
        var options = new ContractReadOptions { EpochUnit = EpochUnit.Seconds };
        Assert.Equal(Epoch1700000000.AddTicks(5_000_000), Convert("1700000000.5", options));
        Assert.Equal(Epoch1700000000.AddTicks(1_234_567), Convert("1700000000.1234567", options));
    }

    [Fact]
    public void Exponent_and_negative_epochs_convert()
    {
        var options = new ContractReadOptions { EpochUnit = EpochUnit.Seconds };
        Assert.Equal(Epoch1700000000, Convert("1.7e9", options));
        Assert.Equal(DateTimeOffset.UnixEpoch.AddSeconds(-86400), Convert("-86400", options));
    }

    [Fact]
    public void Epoch_unit_leaves_timestamp_strings_as_text()
    {
        var options = new ContractReadOptions { EpochUnit = EpochUnit.Seconds };
        Assert.Equal(new DateTimeOffset(2026, 7, 1, 10, 0, 0, TimeSpan.Zero),
            Convert("\"2026-07-01T10:00:00Z\"", options));
        // A digit string is text, never reinterpreted as epoch time.
        var ex = Assert.Throws<PzConnectorException>(() => Convert("\"1700000000\"", options));
        Assert.Contains("not a valid timestamp", ex.Message);
    }

    [Fact]
    public void Out_of_range_seconds_suggests_milliseconds()
    {
        var options = new ContractReadOptions { EpochUnit = EpochUnit.Seconds, EpochUnitOption = "epoch_unit" };
        var ex = Assert.Throws<PzConnectorException>(() => Convert("1700000000123000", options));
        Assert.False(ex.IsTransient);
        Assert.Contains("column 'created'", ex.Message);
        Assert.Contains("out of range", ex.Message);
        Assert.Contains("ms", ex.Message);
        Assert.DoesNotContain("1700000000123000", ex.Message);
    }

    [Fact]
    public void Out_of_range_milliseconds_is_permanent()
    {
        var options = new ContractReadOptions { EpochUnit = EpochUnit.Milliseconds };
        var ex = Assert.Throws<PzConnectorException>(() => Convert("1e300", options));
        Assert.Contains("out of range", ex.Message);
    }

    [Fact]
    public void Number_without_epoch_unit_names_the_option()
    {
        var options = new ContractReadOptions { EpochUnitOption = "epoch_unit" };
        var ex = Assert.Throws<PzConnectorException>(() => Convert("1700000000", options));
        Assert.Contains("JSON number", ex.Message);
        Assert.Contains("'epoch_unit: s'", ex.Message);
    }

    [Fact]
    public void Number_without_any_options_says_it_is_a_number()
    {
        var ex = Assert.Throws<PzConnectorException>(
            () => ContractProjector.ConvertScalar(JsonNode.Parse("1700000000"), "timestamp", "created", "ctx"));
        Assert.Contains("JSON number", ex.Message);
        Assert.Contains("column 'created'", ex.Message);
    }

    [Fact]
    public void Epoch_unit_does_not_touch_other_types()
    {
        var options = new ContractReadOptions { EpochUnit = EpochUnit.Seconds };
        Assert.Equal(1700000000L, Convert("1700000000", options, "bigint"));
        Assert.Throws<PzConnectorException>(() => Convert("1700000000", options, "date"));
    }

    [Fact]
    public void Pointer_reads_a_nested_value()
    {
        var columns = new Dictionary<string, string> { ["id"] = "bigint", ["status"] = "varchar", ["first_tag"] = "varchar" };
        var options = new ContractReadOptions
        {
            Pointers = new Dictionary<string, string> { ["status"] = "/fields/status/name", ["first_tag"] = "/tags/0" },
        };
        var record = JsonNode.Parse("""{ "id": 1, "status": "top-level-ignored", "fields": { "status": { "name": "Done" } }, "tags": ["a", "b"] }""");

        var row = ContractProjector.ProjectRow(record, columns, "ctx", options);

        Assert.Equal([1L, "Done", "a"], row);
    }

    [Fact]
    public void Pointer_missing_step_or_null_is_null()
    {
        var columns = new Dictionary<string, string> { ["status"] = "varchar", ["owner"] = "varchar" };
        var options = new ContractReadOptions
        {
            Pointers = new Dictionary<string, string> { ["status"] = "/fields/status/name", ["owner"] = "/owner/name" },
        };
        var row = ContractProjector.ProjectRow(JsonNode.Parse("""{ "fields": {}, "owner": null }"""), columns, "ctx", options);
        Assert.Equal([null, null], row);

        row = ContractProjector.ProjectRow(JsonNode.Parse("""{ "fields": { "status": { "name": null } }, "owner": "x" }"""),
            columns, "ctx", options);
        Assert.Equal([null, null], row);
    }

    [Fact]
    public void Pointer_to_an_object_lands_as_json_text_in_varchar()
    {
        var columns = new Dictionary<string, string> { ["status"] = "varchar" };
        var options = new ContractReadOptions { Pointers = new Dictionary<string, string> { ["status"] = "/fields/status" } };
        var row = ContractProjector.ProjectRow(JsonNode.Parse("""{ "fields": { "status": { "name": "Done" } } }"""),
            columns, "ctx", options);
        Assert.Equal("""{"name":"Done"}""", row[0]);
    }

    [Fact]
    public void Pointer_and_epoch_compose()
    {
        var columns = new Dictionary<string, string> { ["updated_at"] = "timestamp" };
        var options = new ContractReadOptions
        {
            Pointers = new Dictionary<string, string> { ["updated_at"] = "/attributes/updated_at" },
            EpochUnit = EpochUnit.Seconds,
        };
        var row = ContractProjector.ProjectRow(JsonNode.Parse("""{ "attributes": { "updated_at": 1700000000 } }"""),
            columns, "ctx", options);
        Assert.Equal(Epoch1700000000, row[0]);
    }

    [Fact]
    public void Pointer_type_mismatch_names_the_column_not_the_value()
    {
        var columns = new Dictionary<string, string> { ["n"] = "bigint" };
        var options = new ContractReadOptions { Pointers = new Dictionary<string, string> { ["n"] = "/a/b" } };
        var ex = Assert.Throws<PzConnectorException>(() =>
            ContractProjector.ProjectRow(JsonNode.Parse("""{ "a": { "b": "SECRET" } }"""), columns, "ctx", options));
        Assert.Contains("column 'n'", ex.Message);
        Assert.DoesNotContain("SECRET", ex.Message);
    }

    [Theory]
    [InlineData("1700000000")]      // seconds read as ms: 1970-01-20
    [InlineData("99999999999")]     // just below 1e11 ms (1973-03-03)
    [InlineData("-5000")]
    public void Milliseconds_before_1973_look_like_seconds(string json)
    {
        var options = new ContractReadOptions { EpochUnit = EpochUnit.Milliseconds, EpochUnitOption = "epoch_unit" };
        var ex = Assert.Throws<PzConnectorException>(() => Convert(json, options));
        Assert.False(ex.IsTransient);
        Assert.Contains("before 1973-03-03", ex.Message);
        Assert.Contains("'epoch_unit: s'", ex.Message);
        Assert.DoesNotContain(json, ex.Message);
    }

    [Fact]
    public void Milliseconds_from_1973_are_accepted()
    {
        var options = new ContractReadOptions { EpochUnit = EpochUnit.Milliseconds };
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(100_000_000_000), Convert("100000000000", options));
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(100_000_000_000).AddTicks(5_000), Convert("100000000000.5", options));
    }

    [Theory]
    [InlineData("32503680001")]      // just after 3000-01-01
    [InlineData("1700000000123")]    // milliseconds read as seconds
    public void Seconds_after_year_3000_look_like_milliseconds(string json)
    {
        var options = new ContractReadOptions { EpochUnit = EpochUnit.Seconds, EpochUnitOption = "epoch_unit" };
        var ex = Assert.Throws<PzConnectorException>(() => Convert(json, options));
        Assert.Contains("after 3000-01-01", ex.Message);
        Assert.Contains("'epoch_unit: ms'", ex.Message);
    }

    [Fact]
    public void Seconds_up_to_year_3000_and_before_1970_are_accepted()
    {
        var options = new ContractReadOptions { EpochUnit = EpochUnit.Seconds };
        Assert.Equal(new DateTimeOffset(3000, 1, 1, 0, 0, 0, TimeSpan.Zero), Convert("32503680000", options));
        Assert.Equal(new DateTimeOffset(1900, 1, 1, 0, 0, 0, TimeSpan.Zero), Convert("-2208988800", options));
    }

    [Theory]
    [InlineData("1e400")]
    [InlineData("1000000000000000000000000000000000")]
    public void Huge_numbers_are_out_of_range_under_either_unit(string json)
    {
        foreach (var unit in new[] { EpochUnit.Seconds, EpochUnit.Milliseconds })
        {
            var ex = Assert.Throws<PzConnectorException>(() => Convert(json, new ContractReadOptions { EpochUnit = unit }));
            Assert.False(ex.IsTransient);
        }
    }
}
