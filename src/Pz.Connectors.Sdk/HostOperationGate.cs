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
/// gate observes it, then rethrown unchanged to the caller. <c>GateComplete</c> is sent best-effort
/// on every outcome, cancellation included -- it is the only thing that lets the host's
/// <c>IOperationGate.ExecuteAsync</c> return and release its permit, so a cancelled gated read must
/// never leave it unsent.</para></summary>
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
            catch (Exception ex)
            {
                // Catch-all, cancellation included: whatever op raises, the host still granted a
                // permit for this request and needs a GateComplete to release it. A cancelled read is
                // reported below as a non-success completion rather than silently dropped.
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
            else if (failure is OperationCanceledException)
            {
                complete.GateComplete.TransientError = new PzErrorDetail
                {
                    Code = string.Empty,
                    Message = "operation cancelled",
                    IsTransient = false,
                    RetryAfterMs = 0,
                    Hint = string.Empty,
                };
            }

            // Best-effort, on CancellationToken.None: `ct` may be the very token that just cancelled
            // `op`, so sending the completion on it would drop it exactly when the host's gate permit
            // most needs to hear about it -- leaving IOperationGate.ExecuteAsync in flight on the host
            // until pump teardown. Mirrors HostChannelPeer.SendBestEffortAsync's own swallow: a channel
            // that is gone by now has nowhere for this to land anyway.
            await peer.SendBestEffortAsync(complete).ConfigureAwait(false);

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
