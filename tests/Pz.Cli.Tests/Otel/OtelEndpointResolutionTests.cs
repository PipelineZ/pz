using Pz.Cli.Commands;

namespace Pz.Cli.Tests.Otel;

/// <summary>Precedence/validation of <c>--otel-endpoint</c>/<c>PZ_OTEL_ENDPOINT</c> (the grpc endpoint) through
/// <see cref="RunCommand.TryResolveOtel"/>, with the environment passed in as a lookup so no test touches the
/// process-global variables.</summary>
public sealed class OtelEndpointResolutionTests
{
    private static Func<string, string?> Env(string? endpoint) =>
        key => key == "PZ_OTEL_ENDPOINT" ? endpoint : null;

    private static RunCommand.OtelRaw Flag(string? endpoint) => new(null, endpoint, null, null, null);

    [Fact]
    public void No_option_no_env_resolves_to_null_otel_off()
    {
        var ok = RunCommand.TryResolveOtel(Flag(null), Env(null), out var options, out var error);

        Assert.True(ok);
        Assert.Null(options.Endpoint);
        Assert.False(options.IsOn);
        Assert.Null(error);
    }

    [Fact]
    public void Option_wins_over_env()
    {
        var ok = RunCommand.TryResolveOtel(Flag("http://from-option:4317"), Env("http://from-env:4317"), out var options, out var error);

        Assert.True(ok);
        Assert.Equal("from-option", options.Endpoint!.Host);
        Assert.Null(error);
    }

    [Fact]
    public void Env_used_when_option_absent()
    {
        var ok = RunCommand.TryResolveOtel(Flag(null), Env("http://from-env:4317"), out var options, out var error);

        Assert.True(ok);
        Assert.Equal("from-env", options.Endpoint!.Host);
        Assert.Null(error);
    }

    [Theory]
    [InlineData("not-a-url")]
    [InlineData("ftp://wrong-scheme:21")]
    [InlineData("relative/path")]
    public void Invalid_endpoint_fails_cleanly(string raw)
    {
        var ok = RunCommand.TryResolveOtel(Flag(raw), Env(null), out var options, out var error);

        Assert.False(ok);
        Assert.False(options.IsOn);
        Assert.NotNull(error);
        Assert.Contains("--otel-endpoint", error);
    }
}
