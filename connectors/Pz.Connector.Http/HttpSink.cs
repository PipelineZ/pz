using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http;

/// <summary>Generic HTTP/REST API sink. Universal tier only; append = chunked
/// row-array requests, merge = keyed per-row PUT/PATCH (exactly one key in v1); replace never
/// reaches here (PZ0324). AbortSemantics.None is the honest declaration: you cannot un-POST —
/// AbortAsync cleans up nothing, and the engine reports "delivery stopped" accordingly.</summary>
internal sealed class HttpSink : ISink, IOperationGateAware
{
    private readonly HttpConnectionConfig _connection;
    private readonly TimeProvider _time;
    private readonly HttpClient _client;
    private IOperationGate? _gate;

    public HttpSink(HttpConnectionConfig connection) : this(connection, TimeProvider.System)
    {
    }

    internal HttpSink(HttpConnectionConfig connection, TimeProvider time)
    {
        _connection = connection;
        _time = time;
        _client = HttpSource.CreateClient(connection);
    }

    public AbortSemantics AbortSemantics => AbortSemantics.None;

    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        // Defense-in-depth: PZ0228/PZ0324 refuse earlier through pz run.
        if (spec.Mode is not ("append" or "merge"))
        {
            throw new PzConnectorException(
                $"output '{spec.Output}': http sink supports only 'append'/'merge' write modes (got '{spec.Mode}')",
                isTransient: false);
        }

        var config = HttpSinkOutputConfig.Parse(spec);

        if (spec.Mode == "merge")
        {
            if (spec.Keys.Count != 1)
            {
                throw new PzConnectorException(
                    $"output '{spec.Output}': http sink merge supports exactly one key column " +
                    $"(got {spec.Keys.Count})",
                    isTransient: false);
            }

            if (!schema.FieldsList.Any(f => f.Name == spec.Keys[0]))
            {
                throw new PzConnectorException(
                    $"output '{spec.Output}': merge key column '{spec.Keys[0]}' is not in the output schema",
                    isTransient: false);
            }
        }

        return ValueTask.FromResult<ISinkWriteSession>(
            new HttpWriteSession(_client, _connection, config, spec, _time, _gate));
    }

    /// <summary>Called exactly once per opened sink, before any write call.</summary>
    public void UseOperationGate(IOperationGate gate) => _gate = gate;

    public ValueTask DisposeAsync()
    {
        _client.Dispose();
        return ValueTask.CompletedTask;
    }
}
