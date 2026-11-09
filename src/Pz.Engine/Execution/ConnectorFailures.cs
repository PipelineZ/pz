using Pz.Connectors.Abstractions;
using Pz.Core.Validation;

namespace Pz.Engine.Execution;

/// <summary>A connector host's own failure (a process that would not spawn, a handshake that failed), described
/// with the host's code and hint. The engine cannot reference the host's exception type, so the registry's
/// builder supplies <see cref="ConnectorRegistry.DescribeHostFailure"/> to recognise it. <see cref="IsFatal"/> is
/// the builder's verdict: the host failed while in use (pz could not do its own work), rather than an install
/// problem the author fixes.</summary>
public sealed record ConnectorHostFailure(string Code, string Message, string? Hint)
{
    public bool IsFatal { get; init; }
}

/// <summary>A classified connector failure: the coded error to report, and whether it means pz could not do its own
/// work (fatal, exit 3) rather than that the project is wrong (exit 2).</summary>
public sealed record ConnectorFailure(PzError Error, bool IsFatal);

/// <summary>Classifies what a connector throws while pz reads its config offline (planning, or the same probes at
/// `pz validate`) into one coded error naming the connection and, when there is one, the dataset or output:
/// <list type="bullet">
/// <item>a non-transient refusal coded <see cref="PzConnectorErrorCodes.Config"/> is the author's config to fix:
/// PZ0301;</item>
/// <item>any other <see cref="PzConnectorException"/>, transient or not (a network or auth failure, a connector
/// defect the SDK wrapped as <see cref="PzConnectorErrorCodes.Unhandled"/>, a crashed connector process), is a
/// connector failure: PZ0330, which says so and never tells the author to edit the config;</item>
/// <item>a host failure keeps the host's own code and hint.</item>
/// </list>
/// Anything else is not a connector's failure and is left to propagate (null).
///
/// <para>Fatal or not is decided here, by kind, never from an error code: a config refusal is not fatal; every
/// other connector failure is (pz could not do its own work); a host failure is what the host's description says
/// (<see cref="ConnectorHostFailure.IsFatal"/>, decided where the registry is built).</para></summary>
public static class ConnectorFailures
{
    public const string ConfigHint = "fix the options the connector names";

    public static ConnectorFailure? Classify(Exception ex, string connector, string subject, string file, string phase,
        Func<Exception, ConnectorHostFailure?>? describeHostFailure)
    {
        if (ex is PzConnectorException failure)
        {
            if (!failure.IsTransient && failure.Code == PzConnectorErrorCodes.Config)
            {
                return new ConnectorFailure(new PzError(PzErrorCode.ConnectorConfigInvalid, $"{subject}: {failure.Message}",
                    file, null, ConfigHint), IsFatal: false);
            }

            var transient = failure.IsTransient ? " (transient; re-run)" : "";
            var hint = failure.IsTransient
                ? "re-run; if it keeps failing, check that the connector can reach what the connection points at"
                : failure.Code == PzConnectorErrorCodes.Unhandled
                    ? $"this is a defect in connector '{connector}', not in the project -- report it to the connector's " +
                      "author with its log"
                    : $"connector '{connector}' failed rather than refusing the config -- check its log and what the " +
                      "connection points at";
            return new ConnectorFailure(new PzError(PzErrorCode.ConnectionCheckFailed,
                $"{subject}: connector '{connector}' failed while {phase}{transient}: {failure.Message}", file, null, hint),
                IsFatal: true);
        }

        return describeHostFailure?.Invoke(ex) is { } host
            ? new ConnectorFailure(new PzError(host.Code, $"{subject}: {host.Message}", file, null, host.Hint), host.IsFatal)
            : null;
    }
}
