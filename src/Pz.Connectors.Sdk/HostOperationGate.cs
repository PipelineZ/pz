using System.Runtime.ExceptionServices;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;

namespace Pz.Connectors.Sdk;

/// <summary>The connector side of the host's operation gate: one <c>GateAcquire</c>, wait for the
/// <c>GateGrant</c>, run the operation once, report the outcome in <c>GateComplete</c>.
///
/// <para>Single attempt, and <c>idempotent</c> is sent as false whatever the caller asked. The host
/// never tells the connector that a gate gave up retrying, so a connector that waited for a second
/// grant after reporting a transient failure could wait forever; sending idempotent=false keeps the
/// host from re-granting a request nobody is awaiting. Pacing (the wait for the grant) and budget
/// reporting work in full; retrying a transient failure stays with the engine's node-level retry.
/// The transient exception is still reported in <c>GateComplete.transient_error</c> so the host's
/// gate observes it, then rethrown unchanged to the caller.</para></summary>
internal sealed class HostOperationGate(HostChannelPeer peer) : IOperationGate
{
    public async Task<T> ExecuteAsync<T>(
        string opLabel, bool idempotent, Func<CancellationToken, Task<T>> op, CancellationToken ct)
    {
        var requestId = Guid.NewGuid().ToString("n");
        var granted = peer.RegisterGrant(requestId);
        try
        {
            await peer.SendAsync(new HostChannelUp
            {
                GateAcquire = new GateAcquire { RequestId = requestId, OpLabel = opLabel, Idempotent = false },
            }, ct).ConfigureAwait(false);

            await granted.WaitAsync(ct).ConfigureAwait(false);

            T result = default!;
            Exception? failure = null;
            try
            {
                result = await op(ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failure = ex;
            }

            var complete = new HostChannelUp { GateComplete = new GateComplete { RequestId = requestId } };
            if (failure is PzConnectorException { IsTransient: true } transient)
            {
                complete.GateComplete.TransientError = new PzErrorDetail
                {
                    Code = string.Empty,
                    Message = transient.Message,
                    IsTransient = true,
                    RetryAfterMs = (long)(transient.RetryAfter?.TotalMilliseconds ?? 0),
                    Hint = string.Empty,
                };
            }

            await peer.SendAsync(complete, ct).ConfigureAwait(false);

            if (failure is not null)
            {
                ExceptionDispatchInfo.Capture(failure).Throw();
            }

            return result;
        }
        finally
        {
            peer.ForgetGrant(requestId);
        }
    }

    public void ReportBudget(int remaining, DateTimeOffset resetAt) =>
        _ = peer.SendGateBudgetAsync(remaining, resetAt);
}
