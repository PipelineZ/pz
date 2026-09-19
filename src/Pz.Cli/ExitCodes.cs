namespace Pz.Cli;

/// <summary>The whole-process exit-code contract every verb returns through: a CI caller distinguishes
/// "nodes failed" from "the invocation itself was wrong" from "pz could not run at all" without parsing
/// output. A usage error (an unrecognized command/option, a missing required argument) is <see
/// cref="ConfigError"/>, not <see cref="NodeFailures"/> -- <see cref="Pz.Cli.CliApp.Run"/> overrides
/// System.CommandLine's own parse-error handling, which otherwise hardcodes exit 1 for that case;
/// `--help`/`--version` still exit <see cref="Ok"/>.</summary>
public static class ExitCodes
{
    /// <summary>Every applicable unit of work succeeded: every node ran and none failed (`pz run`/`pz
    /// retry`/`pz test`), or the verb's own check passed (`pz validate`, `pz connector test`).</summary>
    public const int Ok = 0;

    /// <summary>One or more nodes failed, but the run itself completed (every node either ran to a
    /// result or was deliberately skipped as a failed node's descendant) -- the project and invocation
    /// were both usable. Also returned when a run is cancelled (Ctrl+C, SIGTERM) after at least one node
    /// had already failed: <see cref="Pz.Engine.Dispatch.RunOrchestrator.ExecuteAsync"/> keeps
    /// <c>CompletedWithFailures</c> in that case rather than reporting the cancellation.</summary>
    public const int NodeFailures = 1;

    /// <summary>The invocation or the project configuration was wrong before (or instead of) any node
    /// running: a usage error, a validation failure, a missing/malformed project file.</summary>
    public const int ConfigError = 2;

    /// <summary>The run could not complete on its own terms: an orchestrator-level exception, or a
    /// cancellation (Ctrl+C, SIGTERM) that reached <see
    /// cref="Pz.Engine.Dispatch.RunOrchestrator.ExecuteAsync"/> before any node had failed -- including
    /// one cancelled during setup, before the dispatcher existed to turn it into skipped nodes (<see
    /// cref="Pz.Cli.Commands.RunCommand.ExecuteRun"/>'s own catch). Also reserved for an unanticipated
    /// exception that escaped every verb's own handling (generic PZ0500, or one of <see
    /// cref="Pz.Cli.Commands.EngineFailureMapper"/>'s more specific codes).</summary>
    public const int Fatal = 3;
}
