using System.CommandLine;
using Pz.Cli;
using Pz.Cli.Otel;
using Pz.Cli.Rendering;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;
using Pz.Diagnostics.Events;
using Pz.DuckDb;
using Pz.Engine.Artifacts;
using Pz.Engine.Events;
using Pz.Engine.Execution;
using Pz.Engine.Planning;
using Pz.Engine.Resilience;
using Pz.Engine.Dispatch;
using Pz.Engine.State;
using Pz.Engine.Validation;
using Pz.PackageManagement.Hosting;

namespace Pz.Cli.Commands;

internal static class RunCommand
{
    /// <summary>The renderer pump gets this long to drain whatever events are still buffered on the bus
    /// after <see cref="RunEventBus.Complete"/> before the process moves on —
    /// a hung terminal/renderer must never hang `pz run` itself.</summary>
    private static readonly TimeSpan RendererDrainTimeout = TimeSpan.FromSeconds(5);

    public static Command Create()
    {
        var projectOption = new Option<string?>("--project") { Description = "Project directory (default: current directory)" };
        var varsOption = new Option<string?>("--vars") { Description = "JSON object of var overrides" };
        var failFastOption = new Option<bool>("--fail-fast") { Description = "Cancel remaining nodes as soon as one fails" };
        var fullRefreshOption = new Option<bool>("--full-refresh")
        {
            Description = "Ignore stored watermarks and sync state when reading incremental/sync datasets " +
                "this run -- capture and advancement still run, re-establishing them from the full extract.",
        };
        var namesArgument = new Argument<string[]>("names")
        {
            Description = "Flow name(s): each runs that node plus every ancestor and descendant (the whole flow through it)",
            Arity = ArgumentArity.ZeroOrMore,
        };
        var command = new Command("run",
            "Execute a flow end-to-end: load sources, run pipelines and checks, write sinks. " +
            "`pz run <name>` runs the flow through that node; with 2+ independent flows, bare " +
            "`pz run` is an error (PZ0215) -- name a flow, use --select, or pass --all.");
        command.Arguments.Add(namesArgument);
        command.Options.Add(projectOption);
        command.Options.Add(varsOption);
        command.Options.Add(failFastOption);
        command.Options.Add(fullRefreshOption);
        command.Options.Add(SharedOptions.Select);
        command.Options.Add(SharedOptions.All);
        command.Options.Add(SharedOptions.NoLockCheck);
        command.Options.Add(SharedOptions.LogFormat);
        command.Options.Add(SharedOptions.OtelEndpoint);
        command.Options.Add(SharedOptions.StateUrl);
        command.SetAction((parseResult, ct) => Execute(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(varsOption),
            parseResult.GetValue(namesArgument) ?? [],
            parseResult.GetValue(SharedOptions.Select),
            parseResult.GetValue(SharedOptions.All),
            parseResult.GetValue(failFastOption),
            parseResult.GetValue(SharedOptions.NoLockCheck),
            parseResult.GetValue(SharedOptions.LogFormat),
            parseResult.GetValue(SharedOptions.OtelEndpoint),
            parseResult.GetValue(SharedOptions.StateUrl),
            parseResult.GetValue(fullRefreshOption),
            ct));
        return command;
    }

    internal static async Task<int> Execute(
        string projectDir, string? varsJson, string[] names, string? select, bool all, bool failFast,
        bool noLockCheck, string? logFormatRaw, string? otelEndpointRaw, string? stateUrlRaw,
        bool fullRefresh, CancellationToken ct)
    {
        if (!TryParseLogFormat(logFormatRaw, out var logFormat))
        {
            Console.Error.WriteLine(
                $"error: invalid --log-format value '{logFormatRaw}' (expected 'text' or 'json')");
            return ExitCodes.ConfigError;
        }

        if (!TryResolveOtelEndpoint(otelEndpointRaw, out var otelEndpoint, out var otelError))
        {
            Console.Error.WriteLine($"error: {otelError}");
            return ExitCodes.ConfigError;
        }

        PzProject project;
        CompiledDag fullDag;
        var compileNotices = new List<string>();
        try
        {
            var env = SharedInputHelpers.SnapshotEnvironment();
            var overrides = SharedInputHelpers.ParseVars(varsJson);
            project = ProjectLoader.Load(projectDir, env, overrides);
            project = InjectLocalFilesBaseDir(project, projectDir);
            if (!StateUrlOverride.TryApply(project, stateUrlRaw, env, out project, out var stateUrlError))
            {
                Console.Error.WriteLine($"error: {stateUrlError}");
                return ExitCodes.ConfigError;
            }
            var renderCtx = new RenderContext(project, Guid.NewGuid().ToString("N"), DateTimeOffset.UtcNow) { Env = env };
            fullDag = DagCompiler.Compile(project, renderCtx, compileNotices, new DuckDbSqlAstReader());
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }

            return ExitCodes.ConfigError;
        }

        SharedInputHelpers.WriteWarnings(fullDag.Warnings);

        // Render DagCompiler's non-fatal compile notices (currently only "cursor unverified until
        // --connect / first run") the same way every other CLI verb
        // surfaces advisory-not-error findings -- a plain "note: " stdout line, info-level, never an error.
        foreach (var notice in compileNotices)
        {
            Console.WriteLine($"note: {notice}");
        }

        try
        {
            // Tier 4 (implicit pre-run): SQL dry-compile against contract-derived empty
            // tables in a throwaway DuckDB session, run before the real staging session ever opens (so
            // a rejection here creates no `.pz/runs/` directory). Quiet on success -- unlike `pz
            // validate`, `pz run` prints only errors, never the undeclared-dataset/skip notices.
            var dry = await SqlDryCompiler.RunAsync(fullDag, ct);
            if (dry.Errors.Count > 0)
            {
                foreach (var error in dry.Errors)
                {
                    Console.Error.WriteLine($"error {error}");
                }

                return ExitCodes.ConfigError;
            }

            IReadOnlySet<NodeId>? selection;
            try
            {
                selection = RunSelection.Resolve(fullDag, names, select, all, gateBareMultiFlow: true);
            }
            catch (PzValidationException ex)
            {
                foreach (var error in ex.Errors)
                {
                    Console.Error.WriteLine($"error {error}");
                }

                return ExitCodes.ConfigError;
            }

            return await ExecuteRun(
                project, fullDag, projectDir, selection, failFast, noLockCheck, logFormat, ct,
                otelEndpoint: otelEndpoint, fullRefresh: fullRefresh);
        }
        catch (Exception ex)
        {
            // An unexpected exception escaping the execute phase (disk full, permission
            // errors, an orchestrator-level bug that still manages to throw, etc.) must never surface
            // as a raw stack trace to the user — mint a fatal PZ0500 instead. PzValidationException
            // from --select parsing below is handled locally within ExecuteRun and never reaches here.
            Console.Error.WriteLine(
                $"error {PzErrorCode.UnexpectedEngineFailure}: unexpected engine failure — {ex.Message}");
            return ExitCodes.Fatal;
        }
    }

    /// <summary>Shared by `pz run` (selection resolved from `--select`, or null for everything) and
    /// `pz test` (<see cref="TestCommand"/> passes an explicit set of Check node ids + whatever
    /// ancestors <see cref="RunOrchestrator"/>'s selection expansion pulls in). Checks execute here
    /// like any other node kind — <paramref name="fullDag"/> is run
    /// as-is, with no Check-node filtering. <paramref name="logFormat"/> is always an already-validated
    /// "text" or "json" (see <see cref="TryParseLogFormat"/>). <paramref name="drainTimeout"/> and
    /// <paramref name="rendererFactory"/> are internal test seams (both default to production behavior:
    /// <see cref="RendererDrainTimeout"/> and the real logFormat-selected renderer) — a stuck-renderer
    /// regression test injects a near-zero timeout and a renderer whose <c>Render</c> never returns, so
    /// the test proves the drain race actually bounds wall-clock time instead of waiting out the real
    /// 5-second default.</summary>
    internal static async Task<int> ExecuteRun(
        PzProject project, CompiledDag fullDag, string projectDir, IReadOnlySet<NodeId>? selection, bool failFast,
        bool noLockCheck, string logFormat, CancellationToken ct,
        TimeSpan? drainTimeout = null, Func<IEventRenderer>? rendererFactory = null, Uri? otelEndpoint = null,
        bool fullRefresh = false, ReuseManifest? reuse = null, IReadOnlyList<NodeResult>? carriedForward = null,
        ICollection<string>? runtimeNotices = null)
    {
        // Sortable, unique-enough-for-a-local-tool run identity. Runtime identity, not
        // compile output — golden/determinism rules do not apply here.
        var runId = $"{DateTimeOffset.UtcNow:yyyyMMddTHHmmssfff}Z-{Random.Shared.Next(0, 0x10000):x4}";
        var startedAt = DateTimeOffset.UtcNow;
        var paths = new RunPaths(projectDir, runId);
        Directory.CreateDirectory(paths.RunDir);

        // Hold an exclusive OS lock on this run dir for the run's lifetime, so a concurrent `pz clean`
        // skips it instead of deleting a live staging DB. The OS
        // releases it even on SIGKILL, so a crashed run's directory becomes sweepable with no heuristic.
        // `pz retry` reaches this same seam through ExecuteRun, so both verbs are covered here.
        using var runDirLock = RunDirLock.Acquire(paths.RunDir);

        // Every run-time "note: " line routes through this one seam instead of a bare Console.WriteLine
        // per site, for two reasons. (1) json mode's "every stdout line parses as JSON" contract (spelled
        // out at the summary line below) must hold for every note -- the state-backend note, the
        // corrupt-state notice, and the watermark/sync persist-failure notes included -- so stdout stays
        // machine-parseable exactly when a fault makes one of them fire. (2) `pz mcp` has no console at
        // all (Console.Out is parked onto stderr before the transport starts), so a note written straight
        // to the console is invisible to an agent: ToolEnvelope's `notices` array would carry compile
        // notices only, and a silent full re-extract from corrupt watermark state would envelope as
        // ok:true / status:success / exit_code:0 / notices:[] -- indistinguishable from a clean run.
        // `runtimeNotices` is how McpCommand's RunAsync/RetryAsync adapters collect them onto
        // McpRunOutcome.Notices; it stays null for the CLI verbs, whose console output is unchanged.
        //
        // The lock matters: the RunContext callback below fires from executor threads, so concurrent
        // nodes can raise notices at once. Console itself is thread-safe; an ICollection is not.
        var noticeWriter = logFormat == "json" ? Console.Error : Console.Out;
        var noticeGate = new Lock();
        void Notice(string text)
        {
            lock (noticeGate)
            {
                runtimeNotices?.Add(text);
                noticeWriter.WriteLine($"note: {text}");
            }
        }

        // Resolve the configured backend (local files or SQL Server) and — for SQL Server — ensure the
        // state schema is at the version this build expects, BEFORE any node executes: PZ0518
        // (unreachable)/PZ0519 (schema
        // mismatch) must surface here, in the run's load phase, not as a surprise at the first
        // watermark write deep inside node execution.
        StateBackends backends;
        try
        {
            backends = StateBackendFactory.Create(project, projectDir, TimeProvider.System, runId);
            backends.EnsureSchema();
        }
        catch (PzConfigException ex)
        {
            Console.Error.WriteLine($"error {ex.Error}");
            return ExitCodes.ConfigError;
        }

        // The value is ambient by design when it comes from the environment, so its provenance is printed
        // rather than hidden. Silent for the common case (no `state:` block at all -> BackendSource
        // "default"): a project with no state: block must keep its output byte for byte.
        if (project.State.BackendSource != "default")
        {
            Notice($"state backend: {backends.Description}");
        }

        var duckOptions = project.Engine.DuckDb is { } cfg
            ? new DuckOptions(cfg.MemoryLimit, cfg.Threads, cfg.TempDirectory)
            : null;

        await using var duck = DuckSession.Open(paths.StagingDbPath, duckOptions);
        await duck.ExecuteAsync("create schema if not exists staging", ct);

        ConnectorRegistry registry;
        ConnectorHost? host;
        try
        {
            (registry, host) = await ConnectorRegistryFactory.CreateAsync(project, projectDir, noLockCheck, ct);
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }

            return ExitCodes.ConfigError;
        }

        await using var connectorHost = host;

        ExecutionPlan plan;
        try
        {
            plan = await new ExecutionPlanner(registry)
                .PlanAsync(fullDag, project.Engine.ForceUniversal, ct, project.Engine);
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }

            return ExitCodes.ConfigError;
        }

        PlanWriter.Write(plan, Path.Combine(projectDir, ".pz", "target"));

        // SnapshotRunEvents keeps its first-class callback position (registered directly, never only a
        // bus subscriber) so the persisted run-artifact snapshot never depends on the bus or a renderer
        // being alive. RunEventPublisher is the second CompositeRunEvents target, mapping the same
        // callbacks onto the bus for whichever renderer --log-format selected. Writes go through
        // backends.Artifacts (Local or SQL).
        var startedAtIso = startedAt.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
        var snapshotEvents = new SnapshotRunEvents(backends.Artifacts, runId, startedAtIso);
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, runId, TimeProvider.System);
        var events = new CompositeRunEvents(snapshotEvents, publisher);

        IEventRenderer baseRenderer = rendererFactory is not null
            ? rendererFactory()
            : logFormat == "json" ? new JsonRenderer() : new LiveTreeRenderer();
        // A configured event sink (state.events: true on a remote backend) fans in alongside
        // whatever --log-format selected; state.events: false (the default) leaves the renderer
        // untouched, so run_events persistence is opt-in even when the rest of state is remote.
        IEventRenderer renderer = backends.EventSink is { } eventSink
            ? new CompositeEventRenderer(baseRenderer, eventSink)
            : baseRenderer;
        var pump = new RendererPump(bus, renderer);

        // engine.batch_bytes overrides BatchOptions.Default's 32MB target for
        // every batch-producing site the RunContext-carrying executors touch; absent -> null -> Default.
        var batchOptions = project.Engine.BatchBytes is { } batchBytes ? new BatchOptions(batchBytes) : null;

        // Per-project watermark store -- shared by SourceLoadExecutor's read side (via
        // runCtx.Watermarks) and this method's own advancement pass at the very end. The corrupt-file
        // notice routes through the same "note: " convention as DagCompiler's compile notices above.
        // Backend-resolved (local files or SQL Server).
        var watermarkStore = backends.Watermarks;

        // Per-project sync-state store -- shared by SourceLoadExecutor's read side (via
        // runCtx.SyncState) and this method's own advancement pass alongside WatermarkAdvancement below.
        // Backend-resolved, same as watermarkStore.
        var syncStateStore = backends.SyncState;

        // engine.breaker: absent -> null -> KindDispatchingExecutor's gate is a no-op
        // (RunContext.Breakers' documented default). onStateChanged fans every
        // transition out through the same `events` composite (snapshot no-op + bus publisher) every other
        // IRunEvents callback in this method already uses -- `events` is constructed above, so it's ready
        // by the time this closure captures it.
        var breakers = project.Engine.Breaker is { } breakerConfig
            ? new BreakerRegistry(breakerConfig, TimeProvider.System,
                (instance, oldState, newState, trigger, coolDown) =>
                    events.SafeBreakerStateChanged(instance, oldState, newState, trigger, coolDown))
            : null;

        // ALWAYS constructed, unlike breakers -- budget-hint pacing must
        // work for gated connectors even when no rate_limit: appears anywhere in the project.
        var rateLimiters = new RateLimiterRegistry(TimeProvider.System);

        var runCtx = new RunContext(duck, registry, paths, events, plan, batchOptions, watermarkStore, fullRefresh,
            Notice, Breakers: breakers, Reuse: reuse, SyncState: syncStateStore,
            RateLimiters: rateLimiters, SchemaBaselines: backends.Schemas, OnSourceDrift: project.OnSourceDrift);

        // The ONLY place OTel providers get wired up — a no-op when
        // otelEndpoint is null (OTel not configured), so PzActivitySource/PzMeters emission in the
        // engine stays the documented zero-cost no-op. Disposed (flushed) at this method's natural
        // exit, which is AFTER the run summary is printed below.
        await using var otel = OtelProviders.Create(otelEndpoint);

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        ConsoleCancelEventHandler cancelHandler = (_, args) =>
        {
            args.Cancel = true;
            linkedCts.Cancel();
        };
        Console.CancelKeyPress += cancelHandler;

        RunResult result;
        try
        {
            var options = new RunOptions(project.Engine.Threads, failFast, selection, project.Name, carriedForward);
            result = await new RunOrchestrator(new KindDispatchingExecutor(), runCtx)
                .ExecuteAsync(fullDag, options, linkedCts.Token);
        }
        finally
        {
            Console.CancelKeyPress -= cancelHandler;
        }

        var terminalStatus = StatusName(result.Status);

        // This snapshot deliberately persists the run's node results but NOT its terminal status: the
        // terminal write is the last thing this method does, after watermark and sync-state advancement
        // (see the end of this method).
        //
        // Stamping the terminal status here would stamp it seconds before finalization has actually
        // finished -- the retention sweep, a renderer drain of up to RendererDrainTimeout, renderer
        // disposal, and both state advancements all still lie ahead. A process death anywhere in that
        // window (SIGKILL, OOM kill, host eviction) would leave a durable run_results.json reading
        // "success" for a run whose watermarks were never advanced, and `pz retry` would then answer
        // "nothing to retry (run … succeeded)" with exit 0 -- reported successful and unrecoverable at
        // once, with the next `pz run` silently re-extracting from the stale watermark.
        //
        // Writing "running" here makes a crash during finalization indistinguishable from a crash during
        // node execution, which this codebase already handles deliberately and correctly: BuildRetryPlan
        // refuses a "running" prior run with PZ0503 ("prior run was interrupted … re-run 'pz run'") --
        // an honest, actionable outcome instead of a false success. Node-level crash safety is
        // unaffected: every NodeResult is durable at this exact point.
        snapshotEvents.TryWriteSnapshot(result.Nodes, "running");

        // Reclaim .pz/runs disk before the bus closes. Placed here
        // and not beside the watermark write below for one reason: bus.Complete() shuts the renderer pump
        // down, so a RetentionSweptEvent published after it is silently dropped. Retention persists no
        // state the watermark/sync advancements care about, so the two orderings are independent.
        //
        // Purge: false and OlderThan: null are the surface, not tunable defaults -- automatic retention is
        // staging-only, forever. The run that just finished is protected twice over: keep_last >= 1 (the
        // loader rejects 0, PZ0123) puts it at index 0 of the newest-first ordering, and its own RunDirLock
        // still marks it live, which RunRetention.Decide honors ahead of every other rule.
        SweepOutcome? retentionOutcome = null;
        string? retentionNote = null;
        var sweptCount = 0;
        if (project.Retention is { } retention)
        {
            try
            {
                // The store-aware overload --
                // byte-identical to the plain projectDir overload when backends.Artifacts is the local
                // store (that overload just constructs one of these itself), and what makes automatic
                // retention actually bound pz.runs/run_nodes/run_events growth when it resolved to SQL
                // Server (Purge is forced internally there, since a remote candidate never has a staging
                // database to partially clean).
                retentionOutcome = RunSweeper.Sweep(
                    projectDir,
                    backends.Artifacts,
                    new RetentionOptions(retention.KeepLast, OlderThan: null, Purge: false),
                    DateTimeOffset.UtcNow,
                    dryRun: false);

                sweptCount = retentionOutcome.Decisions.Count(d => d.Action != SweepAction.Keep);
                // A sweep can fail every deletion it attempted (e.g. permission-denied on
                // every candidate) and still swept nothing and freed nothing -- gating on sweptCount/
                // TmpDirsSwept alone would silently drop the event and the console line in that case,
                // against this project's "no silent failures" philosophy. Failures.Count > 0 forces both
                // to fire even when nothing was actually reclaimed.
                if (sweptCount > 0 || retentionOutcome.TmpDirsSwept > 0 || retentionOutcome.Failures.Count > 0)
                {
                    // BytesFreed is staging-only per SweepOutcome's own shape -- tmp workdir
                    // bytes are the separate TmpBytesFreed field. The event (and the console line below)
                    // report the combined total per https://pipelinez.dev/events/'s bytesFreed contract ("including stale
                    // .pz/tmp workdirs"); `pz clean` gets away with BytesFreed alone only because it prints
                    // tmp bytes on their own separate report line.
                    bus.Publish(new RetentionSweptEvent(TimeProvider.System.GetUtcNow(), runId, sweptCount,
                        retentionOutcome.BytesFreed + retentionOutcome.TmpBytesFreed, retentionOutcome.Failures.Count));
                }
                else
                {
                    retentionOutcome = null;
                }
            }
            // This catch has no direct test: exercising it would need a test-only seam on ExecuteRun
            // (e.g. an injectable sweeper) solely to force RunSweeper.Sweep to throw. The discipline
            // mirrors the adjacent WatermarkAdvancement/SyncStateAdvancement catches below, which ARE
            // covered.
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Same non-fatal discipline as the watermark/sync advancements below: the data is
                // delivered; the housekeeping is not, and a full disk must never flip a successful run.
                // Deferred to summaryWriter (not printed here) for the same reason the success line is:
                // RendererPump is still draining node lines until the bus completes below, and json mode's
                // "every stdout line parses as JSON" contract must hold for this line too.
                retentionOutcome = null;
                // Bare text (no "note: " prefix): the prefix and the write both belong to Notice, which
                // is called at the deferred print site below so the printed bytes and their position
                // relative to the summary line are unchanged.
                retentionNote =
                    $"could not clean older runs ({MessageRedaction.Redact(ex)}); run pz clean to reclaim disk";
            }
        }

        // The bus's writer completes only after RunCompleted has been published, so every
        // event the renderer needs is already enqueued by the time we ask it to drain. A hung renderer
        // must never hang the process -- past the timeout we drop whatever is left with one stderr note.
        bus.Complete();
        var effectiveDrainTimeout = drainTimeout ?? RendererDrainTimeout;
        var drainWon = await Task.WhenAny(pump.Completion, Task.Delay(effectiveDrainTimeout)) == pump.Completion;
        if (!drainWon)
        {
            Console.Error.WriteLine(
                $"warning: renderer did not finish draining run events within {effectiveDrainTimeout.TotalSeconds:0.##}s; remaining output dropped");
        }

        switch (renderer)
        {
            case IAsyncDisposable asyncDisposableRenderer:
                await asyncDisposableRenderer.DisposeAsync();
                break;
            case IDisposable disposableRenderer:
                disposableRenderer.Dispose();
                break;
        }

        // SqlEventSink's own dispose-time events_dropped UPDATE silently no-ops if no pz.runs row exists
        // yet for this run -- an unenforced ordering invariant between two independently-constructed
        // components. Reading Dropped AFTER the dispose above (which just ran, as part of the
        // renderer/composite disposal) and folding it into one more snapshot makes the count durable
        // regardless of that ordering: this call is guaranteed to create the runs row if the sink's own
        // UPDATE could not.
        //
        // This intermediate write stays "running" like the one above (the terminal write is at the end
        // of the method), and the count is also carried forward into that terminal write so the last
        // snapshot never regresses Dropped back to null.
        long? eventsDropped = null;
        if (backends.EventSink is SqlEventRenderer sqlEventRenderer)
        {
            eventsDropped = sqlEventRenderer.Dropped;
            snapshotEvents.TryWriteSnapshot(result.Nodes, "running", eventsDropped);
        }

        var succeeded = result.Nodes.Count(n => n.Status == NodeStatus.Success);
        var failed = result.Nodes.Count(n => n.Status == NodeStatus.Failed);
        var skipped = result.Nodes.Count(n => n.Status == NodeStatus.Skipped);
        // json mode's contract is "every stdout line parses as JSON" (the run_completed NDJSON event
        // already carries this same summary) -- the human-readable line still gets printed, just to
        // stderr instead of stdout, so it stays available without breaking machine parsing of stdout.
        // text mode prints the summary line to stdout.
        // Same writer the run-time notices use (hoisted to the top of this method) -- one rule, one place.
        var summaryWriter = noticeWriter;
        // paths.RunResultsPath names a file that is never written when backends.Artifacts is the SQL
        // store (state.artifacts: true on a remote backend) -- printing it unconditionally would point at
        // a file that does not exist. The local path is exactly right whenever LocalRunArtifactStore is
        // actually in use, including a remote backend with state.artifacts: false.
        var resultsLocation = backends.Artifacts is LocalRunArtifactStore
            ? paths.RunResultsPath
            : $"{project.State.Schema}.runs";
        summaryWriter.WriteLine(
            $"run {runId}: {succeeded} succeeded, {failed} failed, {skipped} skipped ({resultsLocation})");

        // Only when something was actually freed (or something failed to be, per the gate
        // above). A user whose disk usage drops should be able to point at the line that says why, so
        // this is not gated on verbosity.
        if (retentionOutcome is { } sweep)
        {
            // `sweptCount` is runs SELECTED for deletion, not runs actually deleted --
            // RunSweeper.Sweep subtracts bytes back out on a failed delete but the decision itself still
            // counts as "selected". Without the failure suffix, a permission-denied sweep would print
            // "cleaned 3 staging database(s) ... freed 0 B" and say nothing about the 3 failures. The
            // full failure list stays out of pz run's output on purpose -- pz clean prints those on
            // demand; here only the count is surfaced.
            var failureSuffix = sweep.Failures.Count > 0
                ? $" ({sweep.Failures.Count} could not be deleted; run pz clean for details)"
                : string.Empty;
            // A remote store has no staging.duckdb to sweep -- what actually got deleted is
            // whole runs (its pz.runs/run_nodes/run_events rows), so the unit said here must match.
            var unit = backends.Artifacts is LocalRunArtifactStore ? "staging database(s)" : "run(s)";
            summaryWriter.WriteLine(
                $"cleaned {sweptCount} {unit} and {sweep.TmpDirsSwept} stale workdir(s) — " +
                $"freed {CleanCommand.FormatBytes(sweep.BytesFreed + sweep.TmpBytesFreed)}{failureSuffix}");
        }
        else if (retentionNote is not null)
        {
            Notice(retentionNote);
        }

        // The watermark state write is the LAST state action this run takes -- after the node-results
        // snapshots above and everything else (bus drain, renderer disposal, summary line). fullDag (not
        // the effective/selected subset) is passed so WatermarkAdvancement can walk the real DAG's
        // descendant edges; it filters to result.Nodes (this run's effective set) itself.
        //
        // WatermarkAdvancement's own doc comment states watermark state "must never block or be blocked
        // by" the run_results artifact -- so a persistence failure here (disk full, permission error,
        // unwritable state path) must never flip an otherwise-successful run's outcome to Fatal. Surface
        // it as the same advisory "note: " line the corrupt-file notice above uses, and fall through to
        // the normal status-derived exit code. Cancellation is not swallowed: it must still propagate.
        try
        {
            WatermarkAdvancement.Advance(fullDag, result.Nodes, watermarkStore);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var sanitized = MessageRedaction.Redact(ex);
            Notice(
                $"could not persist watermarks ({sanitized}); the next run will re-extract from the previous watermark");
        }

        // Sync-state persistence is a sibling of the watermark advancement immediately above --
        // same "last action, never blocks/is blocked by run_results.json" placement, same non-fatal
        // try/catch discipline (a persistence failure here must never flip an otherwise-successful run's
        // exit code).
        try
        {
            SyncStateAdvancement.Advance(fullDag, result.Nodes, syncStateStore);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var sanitized = MessageRedaction.Redact(ex);
            Notice(
                $"could not persist sync state ({sanitized}); the next run will re-extract from the previous token");
        }

        // THE terminal write, and the last durable action the run takes. Everything a finished run must
        // persist -- node results, plan, retention, watermarks, sync state -- has happened by the time
        // control reaches here, so a run_results.json that says "success" genuinely means the run
        // finished.
        //
        // Unconditional on purpose. The watermark write must be the run's last state action and must
        // "never block or be blocked by" the run_results artifact; that non-blocking property is supplied
        // by the try/catch discipline immediately above (a persist failure prints a note and falls
        // through to the status-derived exit code), NOT by writing run_results first.
        snapshotEvents.TryWriteSnapshot(result.Nodes, terminalStatus, eventsDropped);

        return result.Status switch
        {
            RunStatus.Success => ExitCodes.Ok,
            RunStatus.CompletedWithFailures => ExitCodes.NodeFailures,
            RunStatus.Fatal => ExitCodes.Fatal,
            _ => throw new ArgumentOutOfRangeException(nameof(result), result.Status, "unknown run status"),
        };
    }

    private static string StatusName(RunStatus status) => status switch
    {
        RunStatus.Success => "success",
        RunStatus.CompletedWithFailures => "completed_with_failures",
        RunStatus.Fatal => "fatal",
        _ => throw new ArgumentOutOfRangeException(nameof(status), status, "unknown run status"),
    };

    /// <summary>Shared by `pz run` and `pz test` (<see cref="TestCommand"/>): <c>--log-format</c>
    /// defaults to "text" when unset and is otherwise restricted to "text"/"json" (case-insensitive) --
    /// an unrecognized value is a CLI usage error, not a crash.</summary>
    internal static bool TryParseLogFormat(string? raw, out string logFormat)
    {
        logFormat = raw ?? "text";
        if (string.Equals(logFormat, "text", StringComparison.OrdinalIgnoreCase))
        {
            logFormat = "text";
            return true;
        }

        if (string.Equals(logFormat, "json", StringComparison.OrdinalIgnoreCase))
        {
            logFormat = "json";
            return true;
        }

        return false;
    }

    /// <summary>Shared by `pz run`/`pz test`/`pz retry`: resolves <c>--otel-endpoint</c> (wins) or the
    /// <c>PZ_OTEL_ENDPOINT</c> environment variable into an absolute http(s) <see cref="Uri"/>, or
    /// <c>null</c> when neither is set (OTel stays fully off). An unparseable value (bad scheme, not an
    /// absolute URL, etc.) is a clean CLI usage error via <paramref name="error"/> — never a crash.</summary>
    internal static bool TryResolveOtelEndpoint(string? optionValue, out Uri? endpoint, out string? error)
    {
        var raw = optionValue ?? Environment.GetEnvironmentVariable("PZ_OTEL_ENDPOINT");
        if (string.IsNullOrWhiteSpace(raw))
        {
            endpoint = null;
            error = null;
            return true;
        }

        if (Uri.TryCreate(raw, UriKind.Absolute, out var uri) &&
            (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps))
        {
            endpoint = uri;
            error = null;
            return true;
        }

        endpoint = null;
        error = $"invalid --otel-endpoint value '{raw}' (expected an absolute http(s) URL)";
        return false;
    }

    /// <summary>Injects <c>base_dir = projectDir</c> into the <c>connection:</c> config of every
    /// <c>localfiles</c> source/sink (see <see cref="BuiltinConnectors"/>'s doc comment for why this
    /// lives here rather than in the connector or the registry). Safe to do before compiling: neither
    /// <c>SourceLoad</c> nor <c>SinkWrite</c> node IDs are derived from <c>Connection</c>
    /// (<c>DagCompiler</c> canonicalizes only dataset/output options and columns).</summary>
    private static PzProject InjectLocalFilesBaseDir(PzProject project, string projectDir)
    {
        var connections = project.Connections
            .Select(s => s.Connector is "localfiles" or "sqlite" ? s with { Connection = WithBaseDir(s.Connection, projectDir) } : s)
            .ToList();
        return project with { Connections = connections };
    }

    private static IReadOnlyDictionary<string, object?> WithBaseDir(
        IReadOnlyDictionary<string, object?> connection, string projectDir)
    {
        var merged = new Dictionary<string, object?>(connection) { ["base_dir"] = projectDir };
        return merged;
    }

}
