using Pz.Cli.Commands;
using Pz.Core.Validation;

namespace Pz.Cli.Tests;

/// <summary>`pz staging serve`'s option rules (PZ0544) and what it prints; the server itself is covered by
/// StagingServerTests.</summary>
public class StagingCommandTests
{
    private static StagingServeArgs Args(string? run = "20261123T120000Z-ab12", string? runDir = null,
        string host = "localhost", int? port = null) => new(run, runDir, host, port, "/project");

    [Fact]
    public void A_run_id_with_defaults_is_valid() => Assert.Null(StagingCommand.ValidateOptions(Args()));

    [Theory]
    [InlineData(null, null, "exactly one of --run")]
    [InlineData("r1", "/runs/r1", "exactly one of --run")]
    [InlineData("../other", null, "plain directory name")]
    [InlineData("..", null, "plain directory name")]
    public void Run_selection_rules(string? run, string? runDir, string expected)
    {
        var error = StagingCommand.ValidateOptions(Args(run, runDir));
        Assert.Equal(PzErrorCode.StagingServeOptionsInvalid, error!.Code);
        Assert.Contains(expected, error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(70000)]
    public void Port_must_be_in_range(int port) =>
        Assert.Contains("--port", StagingCommand.ValidateOptions(Args(port: port))!.Message, StringComparison.Ordinal);

    [Fact]
    public void Host_must_be_a_host_name_or_ip_address() =>
        Assert.Contains("--host", StagingCommand.ValidateOptions(Args(host: "x'; drop"))!.Message, StringComparison.Ordinal);

    [Fact]
    public void Banner_gives_a_paste_ready_attach_and_lists_the_tables()
    {
        var banner = StagingCommand.Banner("r1", "quack:localhost:41000", "abc123",
            ["staging.orders", "staging.stg_orders"]);

        Assert.Contains("CREATE SECRET pz_staging (TYPE quack, TOKEN 'abc123', SCOPE 'quack:localhost:41000');", banner, StringComparison.Ordinal);
        Assert.Contains("ATTACH 'quack:localhost:41000' AS run;", banner, StringComparison.Ordinal);
        Assert.Contains("SELECT * FROM run.staging.orders LIMIT 10;", banner, StringComparison.Ordinal);
        Assert.Contains("Tables (2):\n  staging.orders\n  staging.stg_orders\n", banner, StringComparison.Ordinal);
        Assert.Contains("redact: true", banner, StringComparison.Ordinal);
        Assert.EndsWith("\n", banner, StringComparison.Ordinal);
    }

    [Fact]
    public void Banner_says_so_when_the_run_staged_nothing()
    {
        var banner = StagingCommand.Banner("r1", "quack:localhost:41000", "abc123", []);
        Assert.Contains("The run staged no tables.", banner, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT", banner, StringComparison.Ordinal);
    }
}
