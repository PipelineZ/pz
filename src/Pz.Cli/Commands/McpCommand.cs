using System.CommandLine;
using System.Text.Json.Nodes;
using ModelContextProtocol.Server;
using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Validation;
using Pz.Cli.Rendering;
using Pz.Diagnostics.Events;
using Pz.Mcp;
using Pz.Mcp.ClientSetup;

namespace Pz.Cli.Commands;

/// <summary>`pz mcp`: serve this project over the Model Context Protocol on stdio.
/// stdout belongs to the protocol from here on — Console.Out is parked onto stderr BEFORE the
/// transport starts, so console-writing internals reused by handlers (ExecuteRun's notes and
/// summary lines, InitCommand) surface as client-side logs instead of corrupting the wire.</summary>
internal static class McpCommand
{
    public static Command Create()
    {
        var projectOption = new Option<string?>("--project") { Description = "Project directory (default: current directory)" };
        var allowRunOption = new Option<bool>("--allow-run")
        {
            Description = "Also expose pz_run/pz_retry/pz_run_results — lets a connected agent move real data",
        };
        var command = new Command("mcp", "Serve this project to AI agents over the Model Context Protocol (stdio)");
        command.Options.Add(projectOption);
        command.Options.Add(allowRunOption);
        command.SetAction((parseResult, ct) => Execute(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(allowRunOption), ct));
        command.Subcommands.Add(CreateInit());
        return command;
    }

    /// <summary>Known values of the <c>init</c> subcommand's positional <c>clients</c> argument -- also
    /// the list named in PZ0605's "no client and no --all" and "unknown client" error messages.</summary>
    private static readonly string[] KnownClients = ["vscode", "claude-code", "copilot-cli", "opencode"];

    private static Command CreateInit()
    {
        var clientsArgument = new Argument<string[]>("clients")
        {
            Description = "vscode | claude-code | copilot-cli | opencode (one or more)",
            Arity = ArgumentArity.ZeroOrMore,
        };
        var allOption = new Option<bool>("--all") { Description = "Wire up all four clients" };
        var allowRunOption = new Option<bool>("--allow-run")
        {
            Description = "Bake --allow-run into every generated pz server entry, so the connected " +
                "client also gets pz_run/pz_retry/pz_run_results",
        };
        var skillLocationsOption = new Option<string?>("--skill-locations")
        {
            Description = "Comma-separated skill install locations (standard|claudecode|github|opencode), " +
                "or all|none (default: standard plus the locations implied by the chosen clients)",
        };
        var projectOption = new Option<string?>("--project") { Description = "Project directory (default: current directory)" };
        var command = new Command("init",
            "Write MCP client config files (merge-preserving) and install the pz-pipelines skill, for " +
            "one or more AI clients (vscode, claude-code, copilot-cli, opencode)");
        command.Arguments.Add(clientsArgument);
        command.Options.Add(allOption);
        command.Options.Add(allowRunOption);
        command.Options.Add(skillLocationsOption);
        command.Options.Add(projectOption);
        command.SetAction(parseResult => Init(
            parseResult.GetValue(clientsArgument) ?? [],
            parseResult.GetValue(allOption),
            parseResult.GetValue(allowRunOption),
            parseResult.GetValue(skillLocationsOption),
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory()));
        return command;
    }

    /// <summary>`pz mcp init`: writes each selected client's MCP config file
    /// (<see cref="ClientConfigWriter"/>, merge-preserving) and installs the `pz-pipelines` skill
    /// (<see cref="SkillInstaller"/>) into the locations that client implies (plus `standard`, always).
    /// <paramref name="homeOverride"/> exists ONLY for tests: the real CLI wiring always passes null, so
    /// copilot-cli's target resolves against the real <c>$HOME</c>/user profile -- tests pass a temp dir
    /// here instead, so no test run can ever touch a developer's real <c>~/.copilot</c>.
    ///
    /// <see cref="ClientConfigWriter.Apply"/> throws <see cref="PzConfigException"/> (PZ0605) on an
    /// unparseable existing config file -- this handler must catch it and render `error {ex.Error}` +
    /// <see cref="ExitCodes.ConfigError"/>, the same convention every other verb follows (e.g.
    /// <c>CleanCommand</c>, <c>CdcCommand</c>), rather than let it escape as an unhandled exception. An
    /// unrecognized <c>--skill-locations</c> token is validated up front (before any config file or skill
    /// is written) rather than silently ignored -- the no-silent-failures rule applies to usage input,
    /// not just runtime errors.</summary>
    internal static int Init(
        IReadOnlyList<string> clients, bool all, bool allowRun, string? skillLocationsCsv,
        string projectDir, string? homeOverride = null)
    {
        if (!all && clients.Count == 0)
        {
            Console.Error.WriteLine(
                $"error {PzErrorCode.McpClientConfigInvalid}: no client named -- pass one or more of " +
                $"{string.Join(", ", KnownClients)}, or --all");
            return ExitCodes.ConfigError;
        }

        var selected = all ? KnownClients : [.. clients.Distinct()];
        var unknownClient = selected.FirstOrDefault(c => !KnownClients.Contains(c));
        if (unknownClient is not null)
        {
            Console.Error.WriteLine(
                $"error {PzErrorCode.McpClientConfigInvalid}: unknown client '{unknownClient}' -- expected " +
                $"one of {string.Join(", ", KnownClients)}");
            return ExitCodes.ConfigError;
        }

        var impliedLocations = new List<string> { "standard" };
        foreach (var client in selected)
        {
            impliedLocations.Add(client switch
            {
                "claude-code" => "claudecode",
                "vscode" or "copilot-cli" => "github",
                "opencode" => "opencode",
                _ => "standard",
            });
        }

        if (!TryResolveSkillLocations(skillLocationsCsv, impliedLocations, out var skillLocations, out var badLocation))
        {
            Console.Error.WriteLine(
                $"error {PzErrorCode.McpClientConfigInvalid}: unknown --skill-locations value '{badLocation}' " +
                $"-- expected a comma-separated list of {string.Join(", ", SkillInstaller.Locations.Keys)}, " +
                "or all|none");
            return ExitCodes.ConfigError;
        }

        var home = homeOverride ?? Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);

        try
        {
            foreach (var client in selected)
            {
                var outcome = client switch
                {
                    "vscode" => ClientConfigWriter.Apply(
                        Path.Combine(projectDir, ".vscode", "mcp.json"), "servers", "pz",
                        entry => WriteVsCodeEntry(entry, allowRun)),
                    "claude-code" => ClientConfigWriter.Apply(
                        Path.Combine(projectDir, ".mcp.json"), "mcpServers", "pz",
                        entry => WriteClaudeCodeEntry(entry, allowRun)),
                    "copilot-cli" => ClientConfigWriter.Apply(
                        Path.Combine(home, ".copilot", "mcp-config.json"), "mcpServers", "pz",
                        entry => WriteCopilotCliEntry(entry, allowRun)),
                    "opencode" => ClientConfigWriter.Apply(
                        Path.Combine(projectDir, "opencode.json"), "mcp", "pz",
                        entry => WriteOpenCodeEntry(entry, allowRun)),
                    _ => throw new InvalidOperationException($"unreachable client '{client}'"),
                };
                Console.WriteLine(outcome.Replaced
                    ? $"updated pz entry in {outcome.File}"
                    : $"wrote {outcome.File}");
            }

            if (skillLocations.Count > 0)
            {
                foreach (var dir in SkillInstaller.Install(projectDir, skillLocations))
                {
                    Console.WriteLine($"installed pz-pipelines skill into {dir}");
                }
            }
        }
        catch (PzConfigException ex)
        {
            Console.Error.WriteLine($"error {ex.Error}");
            return ExitCodes.ConfigError;
        }

        return ExitCodes.Ok;
    }

    private static bool TryResolveSkillLocations(
        string? csv, List<string> implied, out IReadOnlyList<string> locations, out string? badToken)
    {
        badToken = null;

        if (csv is null)
        {
            locations = [.. implied.Distinct()];
            return true;
        }

        if (string.Equals(csv, "none", StringComparison.OrdinalIgnoreCase))
        {
            locations = [];
            return true;
        }

        if (string.Equals(csv, "all", StringComparison.OrdinalIgnoreCase))
        {
            locations = [.. SkillInstaller.Locations.Keys];
            return true;
        }

        var tokens = csv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var unknownLocation = tokens.FirstOrDefault(t => !SkillInstaller.Locations.ContainsKey(t));
        if (unknownLocation is not null)
        {
            locations = [];
            badToken = unknownLocation;
            return false;
        }

        locations = tokens;
        return true;
    }

    private static void WriteVsCodeEntry(JsonObject entry, bool allowRun)
    {
        entry["type"] = "stdio";
        entry["command"] = "pz";
        entry["args"] = ArgsArray(allowRun);
    }

    private static void WriteClaudeCodeEntry(JsonObject entry, bool allowRun)
    {
        entry["command"] = "pz";
        entry["args"] = ArgsArray(allowRun);
    }

    private static void WriteCopilotCliEntry(JsonObject entry, bool allowRun)
    {
        entry["type"] = "local";
        entry["command"] = "pz";
        entry["args"] = ArgsArray(allowRun);
        entry["tools"] = new JsonArray { "*" };
    }

    private static void WriteOpenCodeEntry(JsonObject entry, bool allowRun)
    {
        var command = new JsonArray { "pz", "mcp" };
        if (allowRun)
        {
            command.Add("--allow-run");
        }

        entry["type"] = "local";
        entry["command"] = command;
        entry["enabled"] = true;
    }

    private static JsonArray ArgsArray(bool allowRun)
    {
        var args = new JsonArray { "mcp" };
        if (allowRun)
        {
            args.Add("--allow-run");
        }

        return args;
    }

    internal static async Task<int> Execute(string projectDir, bool allowRun, CancellationToken ct)
    {
        Console.SetOut(Console.Error); // park stdout: the MCP transport owns the real stream
        var services = BuildServices();
        await using var server = McpServer.Create(
            new StdioServerTransport("pz"),
            PzMcpServer.CreateOptions(projectDir, services, allowRun));
        await server.RunAsync(ct);
        return ExitCodes.Ok;
    }

    /// <summary>The real (non-fake) <see cref="CliServices"/> wiring -- extracted so `pz mcp` itself and
    /// tests/Pz.Mcp.Tests's execution-tool tests share exactly one construction.</summary>
    internal static CliServices BuildServices() => new()
    {
        CreateRegistryAsync = (project, dir, token) =>
            ConnectorRegistryFactory.CreateAsync(project, dir, noLockCheck: false, token),
        CreateStateStores = (project, dir) =>
        {
            var backends = StateBackendFactory.Create(project, dir, TimeProvider.System);
            backends.EnsureSchema();
            return new McpStateStores(backends.Watermarks, backends.SyncState, backends.Schemas, backends.Artifacts);
        },
        InitProject = InitProject,
        RunAsync = RunAsync,
        RetryAsync = RetryAsync,
    };

    /// <summary>The <see cref="CliServices.RunAsync"/> wiring: mirrors
    /// <see cref="RunCommand.Execute"/>'s composition -- load+compile (with the same localfiles
    /// <c>base_dir</c> injection and real compile-notices capture), the SAME implicit
    /// <see cref="SqlDryCompiler"/> tier-4 pre-flight `pz run` runs BEFORE any run dir/staging DB/connector
    /// opens (broken pipeline SQL must never reach <see cref="RunCommand.ExecuteRun"/> and risk committing
    /// a sibling-branch sink write before failing mid-run), then
    /// <see cref="RunSelection.Resolve"/> (PZ0215/PZ0216/PZ0210 surface here as the
    /// <see cref="PzValidationException"/> they already are), then <see cref="RunCommand.ExecuteRun"/>.
    /// Always <c>--log-format json</c> (an MCP caller has no console to render a live tree into) and
    /// always <c>--fail-fast=false</c>/<c>--no-lock-check=false</c> -- an agent-driven run behaves like a
    /// plain `pz run`, never like a CI-tuned one. Deliberately still skips <c>--otel-endpoint</c>/
    /// <c>--state-url</c> resolution -- neither is part of the MCP tool surface.
    ///
    /// Runs the PZ0604 (<see cref="PzErrorCode.McpRunLockHeld"/>) lock probe FIRST, before opening the
    /// project at all -- an agent-driven caller has no interactive operator to arbitrate a lock conflict,
    /// so refusing outright (rather than blocking on the OS lock inside <see cref="RunCommand.ExecuteRun"/>)
    /// is the only sane MCP behavior.</summary>
    private static async Task<McpRunOutcome> RunAsync(McpRunRequest request, CancellationToken ct)
    {
        var projectDir = request.ProjectDir;
        if (FindHeldRunDir(projectDir) is { } heldDir)
        {
            return new McpRunOutcome(ExitCodes.ConfigError, [LockHeldError(heldDir)]);
        }

        PzProject project;
        CompiledDag fullDag;
        var compileNotices = new List<string>();
        try
        {
            var env = SharedInputHelpers.SnapshotEnvironment();
            project = ProjectLoader.Load(projectDir, env);
            project = SharedInputHelpers.AnchorToProjectDir(project, projectDir);
            var renderCtx = new RenderContext(project, Guid.NewGuid().ToString("N"), DateTimeOffset.UtcNow) { Env = env };
            fullDag = DagCompiler.Compile(project, renderCtx, compileNotices, new DuckDbSqlAstReader());
        }
        catch (PzValidationException ex)
        {
            return new McpRunOutcome(ExitCodes.ConfigError, ex.Errors);
        }

        // Same tier-4 pre-flight `pz run`/`pz retry` run BEFORE any run dir,
        // staging DB, or connector opens -- a rejection here creates no `.pz/runs/` directory, exactly
        // like the CLI verb.
        var dry = await SqlDryCompiler.RunAsync(fullDag, ct).ConfigureAwait(false);
        if (dry.Errors.Count > 0)
        {
            return new McpRunOutcome(ExitCodes.ConfigError, dry.Errors);
        }

        IReadOnlySet<NodeId>? selection;
        try
        {
            selection = RunSelection.Resolve(fullDag, request.FlowNames, select: null, request.All, gateBareMultiFlow: true);
        }
        catch (PzValidationException ex)
        {
            return new McpRunOutcome(ExitCodes.ConfigError, ex.Errors);
        }

        try
        {
            // Two independent gaps in what an MCP caller can see, filled from the same call because they
            // populate different fields of the same envelope.
            //
            // Run-TIME warnings (schema drift under `warn`, duplicate merge keys) are events the CLI
            // renders to a console an MCP caller doesn't have -- capture them off the same bus the NDJSON
            // renderer drains (ExecuteRun awaits that drain before returning) and append them to the
            // compile-time warnings the envelope already carries.
            //
            // Run-time NOTICES are the same story one field over. `pz mcp` parks Console.Out onto stderr
            // before the transport starts, so a note ExecuteRun printed would be invisible -- a corrupt
            // watermark file (silently re-extracting the full source) or a failed watermark write would
            // otherwise envelope as ok:true / status:success / exit_code:0 / notices:[], byte-identical
            // to a clean run. Notice order matches the CLI's own output: compile notices first, then
            // whatever the run raised.
            var runWarnings = new RunWarningCapture();
            var runtimeNotices = new List<string>();
            var exitCode = await RunCommand.ExecuteRun(
                project, fullDag, projectDir, selection, failFast: false, noLockCheck: false, logFormat: "json", ct,
                rendererFactory: () => new CompositeEventRenderer(new JsonRenderer(), runWarnings),
                fullRefresh: request.FullRefresh, runtimeNotices: runtimeNotices);
            return new McpRunOutcome(exitCode, [], Notices: [.. compileNotices, .. runtimeNotices],
                Warnings: [.. fullDag.Warnings, .. runWarnings.Snapshot()]);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Mirrors RunCommand.Execute's own outer catch: an unexpected exception must never surface
            // as a raw stack trace/unhandled fault to the MCP client.
            return new McpRunOutcome(ExitCodes.Fatal, [new PzError(PzErrorCode.UnexpectedEngineFailure,
                $"unexpected engine failure — {ex.Message}", null, null, null)]);
        }
    }

    /// <summary>The <see cref="CliServices.RetryAsync"/> wiring: load+compile
    /// exactly like <see cref="RunAsync"/> above (including real compile-notices capture), the same
    /// <see cref="SqlDryCompiler"/> tier-4 pre-flight <see cref="RetryCommand.Execute"/> runs before
    /// touching prior-run state, then <see cref="RetryCommand.BuildRetryPlan"/> -- the same extracted
    /// core `pz retry` itself now calls -- then <see cref="RunCommand.ExecuteRun"/> with the resulting
    /// selection/reuse/carried-forward. Same PZ0604 lock pre-check as <see cref="RunAsync"/>, for the same
    /// reason: <c>ExecuteRun</c> would otherwise block on the OS lock with no operator to arbitrate.</summary>
    private static async Task<McpRunOutcome> RetryAsync(string projectDir, bool fullRefresh, CancellationToken ct)
    {
        if (FindHeldRunDir(projectDir) is { } heldDir)
        {
            return new McpRunOutcome(ExitCodes.ConfigError, [LockHeldError(heldDir)]);
        }

        PzProject project;
        CompiledDag fullDag;
        var compileNotices = new List<string>();
        try
        {
            var env = SharedInputHelpers.SnapshotEnvironment();
            project = ProjectLoader.Load(projectDir, env);
            project = SharedInputHelpers.AnchorToProjectDir(project, projectDir);
            var renderCtx = new RenderContext(project, Guid.NewGuid().ToString("N"), DateTimeOffset.UtcNow) { Env = env };
            fullDag = DagCompiler.Compile(project, renderCtx, compileNotices, new DuckDbSqlAstReader());
        }
        catch (PzValidationException ex)
        {
            return new McpRunOutcome(ExitCodes.ConfigError, ex.Errors);
        }

        // Same tier-4 pre-flight `pz retry` runs before reading prior-run state.
        var dry = await SqlDryCompiler.RunAsync(fullDag, ct).ConfigureAwait(false);
        if (dry.Errors.Count > 0)
        {
            return new McpRunOutcome(ExitCodes.ConfigError, dry.Errors);
        }

        try
        {
            var plan = RetryCommand.BuildRetryPlan(project, fullDag, projectDir, fullRefresh,
                out var refusal, out var nothingToRetry, out var changedNodeNotices);
            if (refusal is { } err)
            {
                return new McpRunOutcome(ExitCodes.ConfigError, [err]);
            }

            // The "<node> changed since the failed run" notices `pz retry` prints must ride the envelope
            // too, on BOTH outcomes -- they are the whole explanation for a "nothing to retry (project
            // changed)" result, and a notice the CLI surfaces is never silently dropped by the MCP
            // adapter. Order matches the CLI: compile notices first
            // (they were produced first), then the per-stale-node notes.
            var notices = new List<string>(compileNotices);
            notices.AddRange(changedNodeNotices);

            if (nothingToRetry is { } note)
            {
                return new McpRunOutcome(ExitCodes.Ok, [], note, notices, fullDag.Warnings);
            }

            // Same run-time warning capture and run-time notice collection as RunAsync above -- here the
            // notices list is the one already holding the compile and stale-node notes, so ExecuteRun
            // appends after them,
            // keeping the envelope's ordering identical to the CLI's console output.
            var runWarnings = new RunWarningCapture();
            var exitCode = await RunCommand.ExecuteRun(
                project, fullDag, projectDir, plan!.Selection, failFast: false, noLockCheck: false, logFormat: "json", ct,
                rendererFactory: () => new CompositeEventRenderer(new JsonRenderer(), runWarnings),
                fullRefresh: fullRefresh, reuse: plan.Reuse, carriedForward: plan.CarriedForward,
                runtimeNotices: notices);
            return new McpRunOutcome(exitCode, [], Notices: notices,
                Warnings: [.. fullDag.Warnings, .. runWarnings.Snapshot()]);
        }
        catch (PzConfigException ex)
        {
            // Mirrors RetryCommand.Execute's own catch: the state backend's own failures (PZ0125/PZ0518/
            // PZ0519) are config errors with a code and a next step, not "unexpected engine failure".
            return new McpRunOutcome(ExitCodes.ConfigError, [ex.Error]);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new McpRunOutcome(ExitCodes.Fatal, [new PzError(PzErrorCode.UnexpectedEngineFailure,
                $"unexpected engine failure — {ex.Message}", null, null, null)]);
        }
    }

    /// <summary>Projects the run-time warning events onto
    /// <see cref="PzWarning"/>s for the MCP result envelope -- the same facts the CLI's
    /// <see cref="ConsoleRenderer"/> prints as `warning:` lines, in the same wording. Drift is captured
    /// under `warn` policy only: under `fail` the node itself fails with <c>PZ0331</c> and the envelope's
    /// per-node error already tells the story. Render runs on the renderer pump's task while Snapshot is
    /// called from the adapter after the drain, so the lock covers the timed-out-drain straggler case.</summary>
    private sealed class RunWarningCapture : IEventRenderer
    {
        private readonly List<PzWarning> _warnings = [];
        private readonly Lock _gate = new();

        public void Render(RunEvent evt)
        {
            var warning = evt switch
            {
                SourceDriftDetectedEvent e when e.Policy == "warn" => new PzWarning(PzErrorCode.SchemaDrift,
                    $"schema drift on {e.Connection}.{e.Entity} (warn): " +
                    $"{string.Join(", ", e.Changes.Select(ConsoleRenderer.FormatChange))}",
                    null, null,
                    "run 'pz schema accept' to accept the new schema, or fix the source"),
                MergeKeyDuplicatesDetectedEvent e => new PzWarning(PzErrorCode.MergeKeyDuplicates,
                    $"output '{e.Output}': {e.DuplicateGroups} merge key group(s) " +
                    $"[{string.Join(", ", e.Keys)}] have {e.ExtraRows} duplicate staged row(s) -- merge keeps " +
                    "one connector-determined survivor per key (staging order, not cursor order)",
                    null, null,
                    "dedup in the pipeline (e.g. max-cursor per key) if a specific row must win"),
                LossyIntegerInferenceDetectedEvent e => new PzWarning(PzErrorCode.LossyIntegerInference,
                    $"{e.Connection}.{e.Entity}: column(s) [{string.Join(", ", e.Columns)}] auto-detected " +
                    "as DOUBLE but hold only whole numbers beyond 2^53 -- digits may have been lost",
                    null, null,
                    "declare a columns: contract (e.g. bigint or hugeint) to load these losslessly"),
                AmbiguousDateInferenceDetectedEvent e => new PzWarning(PzErrorCode.AmbiguousDateInference,
                    $"{e.Connection}.{e.Entity}: date column(s) [{string.Join(", ", e.Columns)}] parsed " +
                    $"with assumed format {e.Format} -- every value is day/month-ambiguous, so a " +
                    "month-first source is misread on every row",
                    null, null,
                    "normalize the source to ISO 8601, or declare the column varchar and parse it explicitly in SQL"),
                _ => null,
            };

            if (warning is null)
            {
                return;
            }

            lock (_gate)
            {
                _warnings.Add(warning);
            }
        }

        public IReadOnlyList<PzWarning> Snapshot()
        {
            lock (_gate)
            {
                return [.. _warnings];
            }
        }
    }

    /// <summary>The first run directory a live process owns, or null when none -- the same
    /// <see cref="RunDirLock"/> probe `pz clean`/`pz state` use (<c>StateCommand.LiveRunId</c>): the OS
    /// releases the lock on exit including SIGKILL, so a crashed run never blocks an MCP run/retry
    /// forever.</summary>
    private static string? FindHeldRunDir(string projectDir)
    {
        var runsDir = Path.Combine(projectDir, ".pz", "runs");
        if (!Directory.Exists(runsDir))
        {
            return null;
        }

        foreach (var dir in Directory.EnumerateDirectories(runsDir).OrderBy(d => d, StringComparer.Ordinal))
        {
            if (!RunDirLock.IsFree(dir))
            {
                return dir;
            }
        }

        return null;
    }

    private static PzError LockHeldError(string heldDir) => new(PzErrorCode.McpRunLockHeld,
        $"another run is already in progress ('{Path.GetFileName(heldDir)}' holds a live .pz/runs lock)",
        heldDir, null,
        "wait for the other run to finish before retrying -- if no pz process is actually running, the " +
        "lock is stale (the OS releases it on process exit, including a crash) and pz clean can reclaim it");


    /// <summary>The <see cref="CliServices.InitProject"/> wiring: validates <paramref name="templateId"/>
    /// against <see cref="TemplateCatalog"/> (PZ0131, <see cref="PzErrorCode.InitTemplateUnknown"/>) and
    /// pre-checks "directory is empty" itself (PZ0603, <see cref="PzErrorCode.McpInitDirNotEmpty"/>) --
    /// both BEFORE calling <see cref="InitCommand"/>'s internals or touching the filesystem, and both
    /// against the caller's own arguments, so an invalid call fails on what it got wrong rather than on
    /// whatever <see cref="InitCommand.Execute"/> happens to check first. InitCommand's own not-empty
    /// check exists for the CLI's own PZ0130/console-error path, which doesn't produce a
    /// <see cref="PzError"/> this seam can return, and its own unknown-template check exists for the
    /// CLI's own PZ0131/console-error path for the same reason. Once past both checks, this calls
    /// <see cref="InitCommand.Execute"/> with exactly the arguments `pz init &lt;name&gt;` (or
    /// `pz init &lt;name&gt; --template &lt;id&gt;`) would receive if run from <paramref name="dir"/>'s
    /// parent directory -- <paramref name="name"/> is <paramref name="dir"/>'s own leaf, so
    /// <c>Path.GetFullPath(name, workingDir)</c> resolves back to <paramref name="dir"/> exactly, and
    /// project-name derivation (sanitization) stays InitCommand's, never reimplemented here.
    /// <paramref name="templateId"/> is looked up against the same <see cref="TemplateCatalog"/> the
    /// CLI's own `--template` flag selects, so the two front doors scaffold identically and default
    /// identically.</summary>
    private static IReadOnlyList<PzError> InitProject(string dir, string name, string templateId)
    {
        if (TemplateCatalog.Find(templateId) is null)
        {
            var known = string.Join(", ", TemplateCatalog.All.Select(t => t.Id));
            return [new PzError(PzErrorCode.InitTemplateUnknown,
                $"no built-in template named '{templateId}'.", null, null,
                $"pick one of: {known}")];
        }

        if (File.Exists(dir) || (Directory.Exists(dir) && Directory.EnumerateFileSystemEntries(dir).Any()))
        {
            return [new PzError(PzErrorCode.McpInitDirNotEmpty,
                $"target directory '{dir}' already exists and is not empty.", dir, null,
                "call pz_init_project against an empty project directory, or clear it first")];
        }

        var workingDir = Path.GetDirectoryName(dir) ?? dir;
        var exitCode = InitCommand.Execute(name, workingDir, templateId);
        return exitCode == ExitCodes.Ok
            ? []
            : [new PzError(PzErrorCode.McpInitDirNotEmpty,
                $"pz_init_project failed for '{dir}' (exit code {exitCode}) -- see server logs for details",
                dir, null, "check the directory path and try again")];
    }
}
