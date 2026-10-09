using Pz.Cli.Commands;
using Pz.Cli.Otel;

namespace Pz.Cli.Tests.Otel;

public sealed class OtelOptionsResolutionTests
{
    private static Func<string, string?> Env(params (string Key, string Value)[] vars) =>
        key => vars.FirstOrDefault(v => v.Key == key).Value;

    private static readonly RunCommand.OtelRaw NoFlags = new(null, null, null, null, null);

    [Fact]
    public void Nothing_set_is_off()
    {
        Assert.True(RunCommand.TryResolveOtel(NoFlags, Env(), out var o, out var e));
        Assert.Null(e);
        Assert.False(o.IsOn);
    }

    [Fact]
    public void Grpc_endpoint_alone_is_today()
    {
        Assert.True(RunCommand.TryResolveOtel(NoFlags with { Endpoint = "http://c:4317" }, Env(), out var o, out _));
        Assert.Equal(OtelProtocol.Grpc, o.Protocol);
        Assert.Equal("http://c:4317/", o.Endpoint!.AbsoluteUri);
        Assert.True(o.IsOn);
    }

    [Fact]
    public void Http_with_per_signal_urls_and_headers_file()
    {
        var flags = new RunCommand.OtelRaw("http/protobuf", null, "https://t/v1/traces", "https://m/v1/metrics", "/tmp/h");
        Assert.True(RunCommand.TryResolveOtel(flags, Env(), out var o, out _));
        Assert.Equal(OtelProtocol.HttpProtobuf, o.Protocol);
        Assert.Equal("https://t/v1/traces", o.TracesEndpoint!.AbsoluteUri);
        Assert.Equal("https://m/v1/metrics", o.MetricsEndpoint!.AbsoluteUri);
        Assert.Equal("/tmp/h", o.HeadersFile);
    }

    [Fact]
    public void Traces_only_is_on()
    {
        var flags = NoFlags with { Protocol = "http/protobuf", TracesEndpoint = "https://t/v1/traces" };
        Assert.True(RunCommand.TryResolveOtel(flags, Env(), out var o, out _));
        Assert.True(o.IsOn);
        Assert.Null(o.MetricsEndpoint);
    }

    [Fact]
    public void A_flag_outranks_its_env_but_keeps_the_other_env_fallbacks()
    {
        var env = Env(("PZ_OTEL_PROTOCOL", "http/protobuf"), ("PZ_OTEL_TRACES_ENDPOINT", "https://env-t/x"),
            ("PZ_OTEL_METRICS_ENDPOINT", "https://env-m/x"), ("PZ_OTEL_HEADERS_FILE", "/env/h"));
        Assert.True(RunCommand.TryResolveOtel(NoFlags with { TracesEndpoint = "https://flag-t/x" }, env, out var o, out _));
        Assert.Equal("https://flag-t/x", o.TracesEndpoint!.AbsoluteUri);
        Assert.Equal("https://env-m/x", o.MetricsEndpoint!.AbsoluteUri);
        Assert.Equal("/env/h", o.HeadersFile);
    }

    [Theory]
    [InlineData("http/protobuf", "http://c:4317", null, null, "--otel-endpoint is for grpc")]
    [InlineData("grpc", "http://c:4317", "https://t/x", null, "--otel-traces-endpoint needs --otel-protocol http/protobuf")]
    [InlineData("grpc", "http://c:4317", null, "https://m/x", "--otel-metrics-endpoint needs --otel-protocol http/protobuf")]
    [InlineData("ftp", null, null, null, "invalid --otel-protocol value 'ftp'")]
    [InlineData("http/protobuf", null, "not a url", null, "invalid --otel-traces-endpoint value")]
    public void Mixed_or_bad_settings_are_usage_errors(string? protocol, string? endpoint, string? traces, string? metrics, string expected)
    {
        Assert.False(RunCommand.TryResolveOtel(new(protocol, endpoint, traces, metrics, null), Env(), out _, out var e));
        Assert.Contains(expected, e, StringComparison.Ordinal);
    }

    [Fact]
    public void Headers_file_with_grpc_is_refused()
    {
        Assert.False(RunCommand.TryResolveOtel(NoFlags with { Endpoint = "http://c:4317", HeadersFile = "/h" }, Env(), out _, out var e));
        Assert.Contains("--otel-headers-file needs --otel-protocol http/protobuf", e, StringComparison.Ordinal);
    }

    [Fact]
    public void Ambient_OTEL_protocol_is_ignored()
    {
        Assert.True(RunCommand.TryResolveOtel(NoFlags with { Endpoint = "http://c:4317" },
            Env(("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")), out var o, out _));
        Assert.Equal(OtelProtocol.Grpc, o.Protocol);
    }
}
