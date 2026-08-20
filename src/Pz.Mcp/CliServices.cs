using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.State;
using Pz.PackageManagement.Hosting;

namespace Pz.Mcp;

/// <summary>The Pz.Cli-resident composition pz's MCP handlers need, injected by McpCommand
/// (dependency inversion: Pz.Cli references Pz.Mcp, never the reverse). Tests inject fakes or
/// the real Pz.Cli implementations — tests/Pz.Mcp.Tests references Pz.Cli for exactly this.</summary>
public sealed class CliServices
{
    public required Func<PzProject, string, CancellationToken, Task<(ConnectorRegistry Registry, ConnectorHost? Host)>>
        CreateRegistryAsync { get; init; }

    /// <summary>Resolves this project's state backend (local/HTTP/SQL Server per project.yml's
    /// <c>state:</c>) and hands back the three keyed-state façades plus the run-artifact store —
    /// wired by McpCommand via <c>Pz.Cli.StateBackendFactory.Create</c>, which the caller must run
    /// <c>EnsureSchema()</c> on first. May throw <c>PzConfigException</c> (bad state config) —
    /// pz_state is the one handler that catches it.</summary>
    public required Func<PzProject, string, McpStateStores> CreateStateStores { get; init; }

    /// <summary>Scaffolds a brand-new project: <c>(dir, name, templateId) -&gt; errors</c> (empty =
    /// success), wired by McpCommand to <c>Pz.Cli.Commands.InitCommand</c>'s real scaffolding logic.
    /// The <c>dir</c> argument is the absolute target directory (pz_init_project always scaffolds into
    /// the server's own project directory); <c>name</c> is the raw leaf argument pz init's own CLI
    /// contract expects (its project-name derivation stays InitCommand's, never reimplemented here).
    /// <c>templateId</c> is a built-in template's catalog id -- a <see langword="string"/> rather than
    /// InitCommand's own <c>TemplateCatalog</c> type because Pz.Mcp cannot reference Pz.Cli (the
    /// dependency runs the other way). The implementation pre-checks "directory is empty" itself and
    /// returns PZ0603 (<see cref="PzErrorCode.McpInitDirNotEmpty"/>) before touching anything.</summary>
    public required Func<string, string, string, IReadOnlyList<PzError>> InitProject { get; init; }

    /// <summary>Wired by McpCommand to a Pz.Cli adapter that mirrors <c>RunCommand.Execute</c>'s
    /// composition (load+compile, <c>RunSelection.Resolve</c>, <c>RunCommand.ExecuteRun</c>) -- see
    /// <see cref="ExecutionTools"/>'s doc comment for the full contract, including the PZ0604
    /// (<see cref="PzErrorCode.McpRunLockHeld"/>) pre-check the adapter runs before touching anything.
    /// <see cref="McpRunOutcome.Errors"/> non-empty means the run never started (config/selection/lock
    /// error); an empty list with a non-Ok <see cref="McpRunOutcome.ExitCode"/> means the run DID start
    /// but ended in node failures or an orchestrator-level fatal -- both are "ok" MCP calls whose result
    /// payload reports the real outcome.</summary>
    public required Func<McpRunRequest, CancellationToken, Task<McpRunOutcome>> RunAsync { get; init; }

    /// <summary>Wired by McpCommand to a Pz.Cli adapter built on <c>RetryCommand.BuildRetryPlan</c> (the
    /// same extracted core `pz retry` itself calls) -- see <see cref="ExecutionTools"/>. A "nothing
    /// to retry" outcome (the prior run already succeeded, or every retryable node has since changed) is
    /// NOT an error: <see cref="McpRunOutcome.ExitCode"/> is <c>ExitCodes.Ok</c>, <see cref="McpRunOutcome.Errors"/>
    /// is empty, and <see cref="McpRunOutcome.Note"/> carries the same message `pz retry` would have
    /// printed to stdout.</summary>
    public required Func<string /*projectDir*/, bool /*fullRefresh*/, CancellationToken, Task<McpRunOutcome>> RetryAsync { get; init; }
}

/// <summary>The pz_run/pz_retry request shape shared with <see cref="CliServices.RunAsync"/> --
/// <see cref="FlowNames"/> mirrors `pz run <name>...`'s positional argument (empty means none given),
/// <see cref="All"/> mirrors `--all`. Passing both non-empty is a <see cref="PzErrorCode.SelectionConflict"/>
/// the underlying <c>RunSelection.Resolve</c> call already reports -- this record does not itself
/// validate that; it is a plain data carrier.</summary>
public sealed record McpRunRequest(string ProjectDir, string[] FlowNames, bool All, bool FullRefresh);

/// <summary>The outcome <see cref="CliServices.RunAsync"/>/<see cref="CliServices.RetryAsync"/> hand back
/// to <see cref="ExecutionTools"/>. <see cref="Note"/> is additive -- <c>pz_retry</c>'s "nothing to
/// retry" outcome is genuinely successful (ExitCode Ok, no errors) but is not a fresh run either, and
/// the MCP result
/// envelope needs to say so explicitly rather than silently re-describing whatever run
/// <see cref="McpStateStores.Artifacts"/>'s <c>ReadLatest()</c> happens to still be holding. Defaults to
/// null so every other construction site (a real run/retry) is unaffected.
///
/// <see cref="Notices"/>/<see cref="Warnings"/> are a second additive pair: the DagCompiler-produced
/// compile notices and <see cref="Pz.Core.Dag.CompiledDag.Warnings"/> that
/// <c>RunCommand.Execute</c>/<c>VerifyTools.CompileAsync</c> both surface (console lines / the
/// pz_compile result respectively) must ride the envelope rather than being dropped by the Pz.Cli
/// adapters. Null on every
/// construction site that never reaches a successful compile (a lock/selection/dry-compile/config
/// refusal); <see cref="ExecutionTools"/> treats null as empty when rendering.</summary>
public sealed record McpRunOutcome(
    int ExitCode,
    IReadOnlyList<PzError> Errors,
    string? Note = null,
    IReadOnlyList<string>? Notices = null,
    IReadOnlyList<PzWarning>? Warnings = null);

/// <summary>The state seams pz_state (and pz_run/pz_retry) need — a same-shaped
/// projection of Pz.Cli's internal <c>StateBackends</c> record, stripped of the fields only the CLI's
/// own console rendering uses (<c>Description</c>, <c>EventSink</c>, <c>EnsureSchema</c> — already
/// invoked once by the time this record exists).</summary>
public sealed record McpStateStores(
    WatermarkStore Watermarks, SyncStateStore SyncState, SchemaBaselineStore Schemas, IRunArtifactStore Artifacts);
