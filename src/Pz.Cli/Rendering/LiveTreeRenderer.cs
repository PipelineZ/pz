using Pz.Diagnostics.Events;
using Spectre.Console;

namespace Pz.Cli.Rendering;

/// <summary>`--log-format text` when interactive (see <see cref="CiDetector"/>): a Spectre
/// <see cref="LiveDisplay"/> tree grouped sources/pipelines/checks/sinks, with each node row updated in
/// place as its status/rows/elapsed change. Falls back to <see cref="ConsoleRenderer"/> (plain
/// sequential lines) whenever <see cref="CiDetector.IsInteractive()"/> is false — real TTYs can't easily
/// be driven from an automated test, so this class is kept deliberately thin and is verified manually;
/// the interactivity gate itself and the non-interactive fallback ARE covered by tests.</summary>
public sealed class LiveTreeRenderer : IEventRenderer, IDisposable
{
    private readonly IEventRenderer? _fallback;
    private readonly Dictionary<string, NodeRow> _nodes = [];
    private readonly Lock _gate = new();
    private readonly ManualResetEventSlim _ctxReady = new(initialState: false);
    private readonly Task? _liveTask;
    private LiveDisplayContext? _ctx;
    private volatile bool _stopping;

    private sealed class NodeRow
    {
        public required string Kind { get; init; }
        public required string Name { get; init; }
        public string Status { get; set; } = "running";
        public long Rows { get; set; }
        public long DurationMs { get; set; }
        public string? Hint { get; set; }
        public string? Provenance { get; set; }
        public string? DriftWarning { get; set; }
        public string? Error { get; set; }
    }

    public LiveTreeRenderer(IAnsiConsole? console = null, TextWriter? fallbackWriter = null,
        Func<bool>? isInteractive = null)
    {
        var interactive = (isInteractive ?? CiDetector.IsInteractive)();
        if (!interactive)
        {
            _fallback = new ConsoleRenderer(fallbackWriter);
            return;
        }

        var ansiConsole = console ?? AnsiConsole.Console;
        _liveTask = ansiConsole.Live(BuildTree()).StartAsync(async ctx =>
        {
            _ctx = ctx;
            _ctxReady.Set();
            while (!_stopping)
            {
                await Task.Delay(100).ConfigureAwait(false);
            }
        });
    }

    public void Render(RunEvent evt)
    {
        if (_fallback is not null)
        {
            _fallback.Render(evt);
            return;
        }

        lock (_gate)
        {
            switch (evt)
            {
                case NodeStartedEvent e:
                    _nodes[e.NodeId] = new NodeRow { Kind = e.Kind, Name = e.Name };
                    break;

                case NodeProgressEvent e:
                    if (_nodes.TryGetValue(e.NodeId, out var progressRow))
                    {
                        progressRow.Rows = e.Rows;
                    }

                    break;

                case NodeCompletedEvent e:
                    if (!_nodes.TryGetValue(e.NodeId, out var row))
                    {
                        row = new NodeRow { Kind = e.Kind, Name = e.Name };
                        _nodes[e.NodeId] = row;
                    }

                    row.Status = e.Status;
                    row.Rows = e.Rows;
                    row.DurationMs = e.DurationMs;
                    row.Hint = BottleneckHint.For(e);
                    row.Provenance = e.Provenance;
                    // Collapsed to the message's first line (the full block goes to run_results.json):
                    // a tree row is a fixed-height summary, and DuckDB's multi-line "Possible solutions:"
                    // list would push every other node's row off screen mid-run.
                    row.Error = e.ErrorCode is { } code ? $"{code}: {FirstLine(e.ErrorMessage)}" : null;
                    break;

                // Fires after NodeStarted (ordering guarantee:
                // node_started -> ... -> [source_drift_detected] -> node_completed), so the row
                // always already exists by the time this arrives -- no create-if-missing branch
                // needed (unlike NodeCompletedEvent, this event carries no node Name to synthesize
                // one with).
                case SourceDriftDetectedEvent e:
                    if (_nodes.TryGetValue(e.NodeId, out var driftRow))
                    {
                        driftRow.DriftWarning =
                            $"drift: {e.Connection}.{e.Entity} ({e.Policy}): " +
                            $"{string.Join(", ", e.Changes.Select(ConsoleRenderer.FormatChange))}";
                    }

                    break;

                // Same after-NodeStarted ordering guarantee as the
                // drift event above, so the row always already exists -- and the same yellow
                // DriftWarning slot renders it, since a node publishes at most one of the two (drift
                // is SourceLoad-only, this one SinkWrite-only).
                case MergeKeyDuplicatesDetectedEvent e:
                    if (_nodes.TryGetValue(e.NodeId, out var dupRow))
                    {
                        dupRow.DriftWarning =
                            $"dup keys: {e.DuplicateGroups} group(s) [{string.Join(", ", e.Keys)}] " +
                            $"collapse {e.ExtraRows} staged row(s)";
                    }

                    break;

                // Same after-NodeStarted ordering guarantee, same yellow DriftWarning slot. A
                // SourceLoad can publish this AND drift
                // in one run (both are contract-less-only); the drift gate runs after the lint, so
                // drift — the actionable one, with its own accept verb — is the line that sticks.
                case LossyIntegerInferenceDetectedEvent e:
                    if (_nodes.TryGetValue(e.NodeId, out var lossyRow))
                    {
                        lossyRow.DriftWarning =
                            $"lossy ints: [{string.Join(", ", e.Columns)}] auto-detected DOUBLE past 2^53";
                    }

                    break;

                case AmbiguousDateInferenceDetectedEvent e:
                    if (_nodes.TryGetValue(e.NodeId, out var dateRow))
                    {
                        dateRow.DriftWarning =
                            $"ambiguous dates: [{string.Join(", ", e.Columns)}] assumed {e.Format}";
                    }

                    break;

                case RunCompletedEvent:
                    _stopping = true;
                    break;

                case RetentionSweptEvent:
                    // The stream's first (and only) event after RunCompletedEvent, which already set
                    // _stopping above. The live loop only polls _stopping every 100ms, and a GB-scale sweep easily
                    // outlasts that window, so the Spectre LiveDisplay backing _ctx may already have
                    // ended by the time this arrives. Nothing to render here anyway: RunCommand prints
                    // its own "cleaned ..." summary line, so a tree update would risk a duplicate render
                    // (or a throw against a finished context) for no visible gain.
                    return;
            }
        }

        // The live loop's ctx is assigned asynchronously right after StartAsync begins; give it a
        // moment to show up rather than silently dropping the very first render.
        _ctxReady.Wait(TimeSpan.FromSeconds(1));
        if (_ctx is { } ctx)
        {
            ctx.UpdateTarget(BuildTree());
            ctx.Refresh();
        }
    }

    /// <summary>First line of a possibly multi-line error message, for the single-line tree row.</summary>
    private static string FirstLine(string? message)
    {
        var text = message ?? "";
        var breakAt = text.IndexOf('\n');
        return (breakAt < 0 ? text : text[..breakAt]).TrimEnd('\r');
    }

    private Tree BuildTree()
    {
        lock (_gate)
        {
            var tree = new Tree("pz run");
            AddGroup(tree, "sources", "SourceLoad");
            AddGroup(tree, "pipelines", "Pipeline");
            AddGroup(tree, "checks", "Check");
            AddGroup(tree, "sinks", "SinkWrite");
            return tree;
        }
    }

    private void AddGroup(Tree tree, string label, string kind)
    {
        var rows = _nodes.Values.Where(n => n.Kind == kind).OrderBy(n => n.Name, StringComparer.Ordinal).ToList();
        if (rows.Count == 0)
        {
            return;
        }

        var group = tree.AddNode(new Text(label));
        foreach (var row in rows)
        {
            var (glyph, color) = row.Status switch
            {
                "success" => ("ok", Color.Green),
                "failed" => ("FAIL", Color.Red),
                "skipped" => ("skip", Color.Yellow),
                _ => ("..", Color.Grey),
            };
            var line = new Text($"{glyph} {row.Name} {row.Rows} rows {row.DurationMs}ms", new Style(color));
            var lineNode = group.AddNode(line);
            if (row.Error is { } error)
            {
                // Red, above the dim Hint/Provenance children: a failure's cause is the one thing the
                // reader needs off this tree, so it must not read as another piece of trailing telemetry.
                lineNode.AddNode(new Text(error, new Style(Color.Red)));
            }

            if (row.Hint is { } hint)
            {
                lineNode.AddNode(new Text(hint, new Style(Color.Grey)));
            }

            if (row.Provenance is { } provenance)
            {
                // Same dim styling as the Hint child node above — "reused"/"carried_forward" are the
                // exact wire values (RunEventPublisher.ProvenanceName), rendered as a human label here.
                // An unknown future provenance value passes through raw rather than being mislabelled as
                // "carried forward".
                var provenanceLabel = provenance switch
                {
                    "reused" => "(reused)",
                    "carried_forward" => "(carried forward)",
                    _ => provenance,
                };
                lineNode.AddNode(new Text(provenanceLabel, new Style(Color.Grey)));
            }

            if (row.DriftWarning is { } driftWarning)
            {
                // Yellow (not the Hint/Provenance child nodes' dim Grey) -- a schema drift warning is
                // actionable in a way a bottleneck hint or a provenance label isn't, so it should stand
                // out in the tree the same way a "skip" status glyph does.
                lineNode.AddNode(new Text(driftWarning, new Style(Color.Yellow)));
            }
        }
    }

    public void Dispose()
    {
        _stopping = true;
        try
        {
            _liveTask?.Wait(TimeSpan.FromSeconds(2));
        }
        catch
        {
            // Best-effort teardown only — a wedged live-display task must never fault Dispose.
        }

        _ctxReady.Dispose();
    }
}
