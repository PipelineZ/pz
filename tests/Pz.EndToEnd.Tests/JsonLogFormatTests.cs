using System.Text.Json;
using Pz.Cli;

namespace Pz.EndToEnd.Tests;

/// <summary>`pz run --log-format json` on a real project — every stdout
/// line parses as JSON, the first event is `run_started`, the last is `run_completed`, and each node's
/// lifecycle events (`node_started` -&gt; ... -&gt; `node_completed`) appear in that per-node order.
///
/// Joins "console-redirection" (see <see cref="ConsoleRedirectionCollection"/>): <see cref="Console.Out"/>
/// is a process-global static, and xunit runs different test classes within an assembly on separate
/// threads by default -- a concurrently-running test that writes to the real console while this test has
/// swapped in a capturing <see cref="StringWriter"/> would leak its output into this test's buffer (and
/// vice versa). Every e2e test class that redirects <see cref="Console.Out"/>/<see cref="Console.Error"/>
/// must join this collection so they serialize against each other instead of racing.</summary>
[Collection("console-redirection")]
public sealed class JsonLogFormatTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-e2e-json-tests", Guid.NewGuid().ToString("N"));

    public JsonLogFormatTests() => CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "csv-to-parquet"), _work);

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void Run_with_log_format_json_emits_ndjson_on_stdout()
    {
        var stdout = new StringWriter();
        var originalOut = Console.Out;
        Console.SetOut(stdout);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(["run", "--project", _work, "--log-format", "json"]).Invoke();
        }
        finally
        {
            Console.SetOut(originalOut);
        }

        Assert.Equal(ExitCodes.Ok, exit);

        var lines = stdout.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => l.TrimEnd('\r'))
            .ToList();
        Assert.NotEmpty(lines);

        var parsed = new List<JsonElement>();
        foreach (var line in lines)
        {
            using var doc = JsonDocument.Parse(line);
            parsed.Add(doc.RootElement.Clone());
        }

        Assert.Equal("run_started", parsed[0].GetProperty("event").GetString());

        // A `retention_swept` event can follow `run_completed` as the stream's final line (see
        // https://pipelinez.dev/events/), but only when automatic retention actually swept something. This fixture's
        // `_work` temp dir is fresh per test (no prior runs to sweep), so no `retention_swept` line is
        // ever emitted here and `run_completed` still holds as the last event. Do not "fix" this
        // assertion without first checking whether the fixture now has sweepable prior runs.
        Assert.Equal("run_completed", parsed[^1].GetProperty("event").GetString());

        // Per-node lifecycle ordering: for each nodeId, node_started must appear before node_completed.
        var startedAt = new Dictionary<string, int>();
        var completedAt = new Dictionary<string, int>();
        for (var i = 0; i < parsed.Count; i++)
        {
            var evt = parsed[i].GetProperty("event").GetString()!;
            if (evt is "node_started" or "node_completed")
            {
                var nodeId = parsed[i].GetProperty("nodeId").GetString()!;
                if (evt == "node_started")
                {
                    startedAt[nodeId] = i;
                }
                else
                {
                    completedAt[nodeId] = i;
                }
            }
        }

        Assert.NotEmpty(completedAt);
        foreach (var (nodeId, completedIndex) in completedAt)
        {
            Assert.True(startedAt.TryGetValue(nodeId, out var startedIndex),
                $"node {nodeId} completed with no matching node_started event");
            Assert.True(startedIndex < completedIndex,
                $"node {nodeId}: node_started (index {startedIndex}) must precede node_completed (index {completedIndex})");
        }

        // Every node in the effective set gets a NodeCompletedEvent (3 nodes: source, pipeline, sink).
        Assert.Equal(3, completedAt.Count);
    }

    private static void CopyTree(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (var file in Directory.EnumerateFiles(from, "*", SearchOption.AllDirectories))
        {
            var dest = Path.Combine(to, Path.GetRelativePath(from, file));
            Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
            File.Copy(file, dest, overwrite: true);
        }
    }
}

[CollectionDefinition("console-redirection")]
public class ConsoleRedirectionCollection;
