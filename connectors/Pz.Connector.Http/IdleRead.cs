using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http;

/// <summary>Bounds each read of a streamed response body by <paramref name="timeout"/>, measured on
/// <paramref name="time"/>. A streamed send returns once the headers arrive, so the client's own
/// timeout no longer applies; without this a server that sends headers and then stalls would hang the
/// read forever. The bound is per read, not in total: a slow body that keeps arriving is never cut.
/// A stall becomes a transient error; the caller's cancellation still surfaces as
/// <see cref="OperationCanceledException"/>.</summary>
/// <param name="stalled">The message for a stall (label, stage and redacted target included).</param>
internal sealed class IdleRead(TimeSpan timeout, TimeProvider time, Func<string> stalled)
{
    public async ValueTask<int> ReadAsync(Stream stream, Memory<byte> buffer, CancellationToken ct)
    {
        using var idle = new CancellationTokenSource(timeout, time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, idle.Token);
        try
        {
            return await stream.ReadAsync(buffer, linked.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is OperationCanceledException or IOException
                                   && idle.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            throw new PzConnectorException(stalled(), isTransient: true, innerException: ex);
        }
    }

    /// <summary>The message tail every stall shares.</summary>
    public static string Tail(TimeSpan timeout)
        => $"stalled: no data for {timeout.TotalSeconds:0.###} s ('timeout_seconds')";
}
