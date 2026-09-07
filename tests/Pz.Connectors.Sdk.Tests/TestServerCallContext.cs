using Grpc.Core;

namespace Pz.Connectors.Sdk.Tests;

/// <summary>The minimum ServerCallContext a unit test needs to call a generated service base
/// directly: a cancellation token and empty metadata.</summary>
internal sealed class TestServerCallContext(CancellationToken ct, Metadata? requestHeaders = null, string method = "test") : ServerCallContext
{
    private readonly Metadata _responseTrailers = [];
    private readonly Metadata _requestHeaders = requestHeaders ?? [];

    protected override string MethodCore => method;
    protected override string HostCore => "localhost";
    protected override string PeerCore => "unix";
    protected override DateTime DeadlineCore => DateTime.MaxValue;
    protected override Metadata RequestHeadersCore => _requestHeaders;
    protected override CancellationToken CancellationTokenCore => ct;
    protected override Metadata ResponseTrailersCore => _responseTrailers;
    protected override Status StatusCore { get; set; }
    protected override WriteOptions? WriteOptionsCore { get; set; }
    protected override AuthContext AuthContextCore => new(null, new Dictionary<string, List<AuthProperty>>());
    protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
        throw new NotSupportedException();
    protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
}

internal sealed class ListStreamWriter<T> : IServerStreamWriter<T>
{
    public List<T> Written { get; } = [];
    public WriteOptions? WriteOptions { get; set; }
    public Task WriteAsync(T message)
    {
        Written.Add(message);
        return Task.CompletedTask;
    }
}
