using Pz.Cli.Commands;

namespace Pz.Cli.Tests.Otel;

/// <summary>Pure precedence/validation logic for
/// <c>--otel-endpoint</c>/<c>PZ_OTEL_ENDPOINT</c> — mutates the real process-global env var, so every
/// test resets it in a finally (mirrors <see cref="Pz.Cli.Tests.RunCommandTests"/>'s DATA_DIR/OUT_DIR
/// discipline) and this class is NOT safe to run in parallel with anything else that reads/writes
/// PZ_OTEL_ENDPOINT (nothing else in this suite does today).</summary>
public sealed class OtelEndpointResolutionTests
{
    [Fact]
    public void No_option_no_env_resolves_to_null_otel_off()
    {
        Environment.SetEnvironmentVariable("PZ_OTEL_ENDPOINT", null);
        try
        {
            var ok = RunCommand.TryResolveOtelEndpoint(null, out var endpoint, out var error);

            Assert.True(ok);
            Assert.Null(endpoint);
            Assert.Null(error);
        }
        finally
        {
            Environment.SetEnvironmentVariable("PZ_OTEL_ENDPOINT", null);
        }
    }

    [Fact]
    public void Option_wins_over_env()
    {
        Environment.SetEnvironmentVariable("PZ_OTEL_ENDPOINT", "http://from-env:4317");
        try
        {
            var ok = RunCommand.TryResolveOtelEndpoint("http://from-option:4317", out var endpoint, out var error);

            Assert.True(ok);
            Assert.NotNull(endpoint);
            Assert.Equal("from-option", endpoint!.Host);
            Assert.Null(error);
        }
        finally
        {
            Environment.SetEnvironmentVariable("PZ_OTEL_ENDPOINT", null);
        }
    }

    [Fact]
    public void Env_used_when_option_absent()
    {
        Environment.SetEnvironmentVariable("PZ_OTEL_ENDPOINT", "http://from-env:4317");
        try
        {
            var ok = RunCommand.TryResolveOtelEndpoint(null, out var endpoint, out var error);

            Assert.True(ok);
            Assert.NotNull(endpoint);
            Assert.Equal("from-env", endpoint!.Host);
            Assert.Null(error);
        }
        finally
        {
            Environment.SetEnvironmentVariable("PZ_OTEL_ENDPOINT", null);
        }
    }

    [Theory]
    [InlineData("not-a-url")]
    [InlineData("ftp://wrong-scheme:21")]
    [InlineData("relative/path")]
    public void Invalid_endpoint_fails_cleanly(string raw)
    {
        Environment.SetEnvironmentVariable("PZ_OTEL_ENDPOINT", null);
        try
        {
            var ok = RunCommand.TryResolveOtelEndpoint(raw, out var endpoint, out var error);

            Assert.False(ok);
            Assert.Null(endpoint);
            Assert.NotNull(error);
            Assert.Contains("--otel-endpoint", error);
        }
        finally
        {
            Environment.SetEnvironmentVariable("PZ_OTEL_ENDPOINT", null);
        }
    }
}
