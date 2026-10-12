using System.Diagnostics;
using Pz.Connectors.Abstractions;

using Pz.Connectors.Sdk.Telemetry;

namespace Pz.Connectors.Sdk;

/// <summary>Opens <c>remote.*</c> spans for calls a connector makes to the system it talks to, and
/// feeds the same calls into the node's remote statistics. Reach it as
/// <see cref="PzConnectorContext.Remote"/>. Spans carry only the operation name and numbers, never
/// URLs, headers, SQL or exception messages. At most 200 spans open per operation; further calls
/// still count as requests.</summary>
public sealed class RemoteCalls
{
    private readonly ActivitySource _source;

    internal RemoteCalls(ActivitySource source) => _source = source;

    /// <summary>Starts a <c>remote.&lt;operation&gt;</c> span parented on the current activity. The
    /// operation matches <c>[a-z][a-z0-9_]{0,31}</c>; anything else is recorded as <c>call</c>, never
    /// as an error. Outside an operation the call works and records nothing.</summary>
    /// <param name="operation">A short fixed verb such as <c>query</c> or <c>get_object</c>.</param>
    /// <returns>The call; dispose it when the remote request finishes.</returns>
    public RemoteCall Start(string operation)
    {
        var name = IsValidOperation(operation) ? operation : "call";
        var stats = ConnectorStats.Current;
        Activity? activity = null;
        if (_source.HasListeners())
        {
            var budget = RemoteSpanBudget.Current;
            if (budget is null || budget.TryTake())
            {
                activity = _source.StartActivity("remote." + name, ActivityKind.Client);
                activity?.SetTag("pz.remote.operation", name);
            }
            else
            {
                (stats as ConnectorStatsAccumulator)?.AddSpansDropped(1);
            }
        }

        return new RemoteCall(activity, stats);
    }

    /// <summary>A handler that wraps each request in a <c>remote.http</c> span and counts requests,
    /// wire bytes (known <c>Content-Length</c>s) and throttling (429 and 503). It records method, host,
    /// port and status code only.</summary>
    /// <param name="inner">The handler to delegate to; a default <see cref="HttpClientHandler"/> when null.</param>
    /// <returns>A handler to pass to <see cref="HttpClient"/>.</returns>
    public DelegatingHandler HttpHandler(HttpMessageHandler? inner = null) =>
        new RemoteHttpHandler(this, inner ?? new HttpClientHandler());

    private static bool IsValidOperation(string? name)
    {
        if (name is null || name.Length is 0 or > 32 || name[0] is not (>= 'a' and <= 'z'))
        {
            return false;
        }

        foreach (var c in name)
        {
            if (c is not ((>= 'a' and <= 'z') or (>= '0' and <= '9') or '_'))
            {
                return false;
            }
        }

        return true;
    }
}

/// <summary>One remote call. Disposing counts one request and ends the span. Dispose cannot see an
/// exception in flight, so call <see cref="Fail"/> in the catch block first. Methods chain and are
/// safe from several threads.</summary>
public sealed class RemoteCall : IDisposable
{
    private readonly Activity? _activity;
    private readonly IConnectorStats _stats;
    private int _disposed;

    internal RemoteCall(Activity? activity, IConnectorStats stats)
    {
        _activity = activity;
        _stats = stats;
    }

    /// <summary>Adds bytes sent or received on the wire for this call.</summary>
    /// <param name="n">Byte count; must not be negative.</param>
    /// <returns>This call.</returns>
    public RemoteCall WireBytes(long n)
    {
        _stats.AddWireBytes(n);
        return this;
    }

    /// <summary>Adds time spent waiting on the remote system.</summary>
    /// <param name="ms">Milliseconds.</param>
    /// <returns>This call.</returns>
    public RemoteCall RemoteMs(long ms)
    {
        _stats.AddRemoteMs(ms);
        return this;
    }

    /// <summary>Counts requests the remote system throttled (rate limited).</summary>
    /// <param name="n">How many.</param>
    /// <returns>This call.</returns>
    public RemoteCall Throttled(long n = 1)
    {
        _stats.AddThrottled(n);
        return this;
    }

    /// <summary>Adds billed cost in a unit such as <c>ru</c> or <c>bytes_billed</c>.</summary>
    /// <param name="units">Amount of the unit.</param>
    /// <param name="unit">The unit name; the first unit reported for a node wins.</param>
    /// <returns>This call.</returns>
    public RemoteCall Cost(double units, string unit)
    {
        _stats.AddCost(units, unit);
        return this;
    }

    /// <summary>Adds a connector-specific number, named <c>vendor.metric</c>.</summary>
    /// <param name="name">Lowercase dotted name.</param>
    /// <param name="value">The amount to add.</param>
    /// <returns>This call.</returns>
    public RemoteCall Extra(string name, double value)
    {
        _stats.AddExtra(name, value);
        return this;
    }

    /// <summary>Marks the span failed. Only the exception type is recorded, never its message.</summary>
    /// <param name="ex">The failure.</param>
    public void Fail(Exception ex)
    {
        ArgumentNullException.ThrowIfNull(ex);
        if (_activity is null)
        {
            return;
        }

        _activity.SetStatus(ActivityStatusCode.Error);
        _activity.SetTag("error.type", ex.GetType().FullName);
    }

    /// <summary>Counts one request and ends the span. Idempotent.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _stats.AddRequests(1);
        _activity?.Dispose();
    }

    internal void Tag(string key, object? value) => _activity?.SetTag(key, value);
}
