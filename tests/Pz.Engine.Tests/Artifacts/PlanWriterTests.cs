using System.Text.Json;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Engine.Artifacts;
using Pz.Engine.Planning;

namespace Pz.Engine.Tests.Artifacts;

public sealed class PlanWriterTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));

    // Fixed, arbitrary budget used only to pin PlanWriter's JSON shape -- not exercising the formula
    // itself (see MemoryBudgetTests for that): duckdb 1GiB, channels ~384MiB, overhead 256MiB.
    private static readonly MemoryBudget SampleBudget = new(
        DuckDbBytes: 1_073_741_824L, DuckDbDisclaimer: null,
        ChannelBytes: 402_653_184L, FixedOverheadBytes: 268_435_456L, TotalBytes: 1_744_830_464L);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static ExecutionPlan OneNodePlan() => new(
        [
            new PlannedNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_crm__orders",
                EdgeStrategy.NativeScan, 1, "native scan: connector 'localfiles' provides read_csv over data/orders.csv"),
        ],
        SampleBudget);

    [Fact]
    public void Plan_reports_pushdown_counts_but_never_predicate_text()
    {
        var plan = new ExecutionPlan(
            [
                new PlannedNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_crm__orders",
                    EdgeStrategy.ArrowStream, 1, "arrow stream: connector 'postgres' has no native path",
                    new PushdownInfo(2, PredicatePushed: true)),
            ],
            SampleBudget);

        PlanWriter.Write(plan, _dir);
        var json = File.ReadAllText(Path.Combine(_dir, "plan.json"));

        Assert.Contains("\"columns_pushed\": 2", json, StringComparison.Ordinal);
        Assert.Contains("\"predicate_pushed\": true", json, StringComparison.Ordinal);
    }

    [Fact]
    public void A_whole_row_read_reports_null_columns_rather_than_zero()
    {
        // Null distinguishes "no pruning possible" from "zero columns", which cannot happen.
        var plan = new ExecutionPlan(
            [
                new PlannedNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_crm__orders",
                    EdgeStrategy.ArrowStream, 1, "arrow stream", new PushdownInfo(null, PredicatePushed: true)),
            ],
            SampleBudget);

        PlanWriter.Write(plan, _dir);
        var json = File.ReadAllText(Path.Combine(_dir, "plan.json"));

        Assert.Contains("\"columns_pushed\": null", json, StringComparison.Ordinal);
    }

    [Fact]
    public void A_node_that_pushes_nothing_writes_no_pushdown_keys_at_all()
    {
        // Additive: every plan.json that pushed nothing stays byte-identical to before the feature.
        PlanWriter.Write(OneNodePlan(), _dir);
        var json = File.ReadAllText(Path.Combine(_dir, "plan.json"));

        Assert.DoesNotContain("columns_pushed", json, StringComparison.Ordinal);
        Assert.DoesNotContain("predicate_pushed", json, StringComparison.Ordinal);
    }

    [Fact]
    public void Plan_json_is_byte_stable_across_writes()
    {
        var plan = OneNodePlan();
        var dirA = Path.Combine(_dir, "a");
        var dirB = Path.Combine(_dir, "b");

        PlanWriter.Write(plan, dirA);
        PlanWriter.Write(plan, dirB);

        var bytesA = File.ReadAllBytes(Path.Combine(dirA, "plan.json"));
        var bytesB = File.ReadAllBytes(Path.Combine(dirB, "plan.json"));
        Assert.Equal(bytesA, bytesB);
    }

    [Fact]
    public void Plan_json_field_order_and_final_newline()
    {
        var plan = OneNodePlan();
        PlanWriter.Write(plan, _dir);

        var text = File.ReadAllText(Path.Combine(_dir, "plan.json"));

        // System.Text.Json's default encoder escapes apostrophes as ' (its conservative,
        // HTML-safe default) — the literal expected string below reflects that, not a raw `'`.
        var expected =
            "{\n" +
            "  \"version\": 1,\n" +
            "  \"nodes\": [\n" +
            "    {\n" +
            "      \"id\": \"aaaaaaaaaaaaaaaa\",\n" +
            "      \"kind\": \"SourceLoad\",\n" +
            "      \"name\": \"src_crm__orders\",\n" +
            "      \"strategy\": \"native_scan\",\n" +
            "      \"partitions\": 1,\n" +
            "      \"reason\": \"native scan: connector \\u0027localfiles\\u0027 provides read_csv over data/orders.csv\"\n" +
            "    }\n" +
            "  ],\n" +
            "  \"memoryBudget\": {\n" +
            "    \"duckdbBytes\": 1073741824,\n" +
            "    \"duckdbDisclaimer\": null,\n" +
            "    \"channelBytes\": 402653184,\n" +
            "    \"fixedOverheadBytes\": 268435456,\n" +
            "    \"totalBytes\": 1744830464,\n" +
            // ONE key appended after totalBytes, the last field of the last object -- every byte before
            // it is part of the pinned golden shape. Null here because
            // SampleBudget is constructed literally rather than through MemoryBudget.Compute, so it takes
            // the field's default; the unset-threads path that populates it is covered by
            // MemoryBudgetTests, and its serialized form by Plan_json_budget_carries_the_threads_disclaimer.
            "    \"duckdbThreadsDisclaimer\": null\n" +
            "  }\n" +
            "}\n";

        Assert.Equal(expected, text);
    }

    /// <summary>memoryBudget is additive and appended LAST -- every byte before it (version, nodes)
    /// matches the shape pinned by the test above -- and the object itself is byte-stable across
    /// repeated writes of the same plan.</summary>
    [Fact]
    public void Plan_json_budget_is_additive_and_byte_stable()
    {
        var plan = OneNodePlan();
        var dirA = Path.Combine(_dir, "a");
        var dirB = Path.Combine(_dir, "b");

        PlanWriter.Write(plan, dirA);
        PlanWriter.Write(plan, dirB);

        var bytesA = File.ReadAllBytes(Path.Combine(dirA, "plan.json"));
        var bytesB = File.ReadAllBytes(Path.Combine(dirB, "plan.json"));
        Assert.Equal(bytesA, bytesB);

        var text = File.ReadAllText(Path.Combine(dirA, "plan.json"));
        using var document = JsonDocument.Parse(text);
        var topLevelKeys = document.RootElement.EnumerateObject().Select(p => p.Name).ToArray();
        Assert.Equal(["version", "nodes", "memoryBudget"], topLevelKeys);

        var preExisting = text[..text.IndexOf("\"memoryBudget\"", StringComparison.Ordinal)];
        Assert.Contains("\"version\": 1", preExisting);
        Assert.Contains("\"id\": \"aaaaaaaaaaaaaaaa\"", preExisting);
        Assert.DoesNotContain("memoryBudget", preExisting);

        var budget = document.RootElement.GetProperty("memoryBudget");
        Assert.Equal(1_073_741_824L, budget.GetProperty("duckdbBytes").GetInt64());
        Assert.Equal(JsonValueKind.Null, budget.GetProperty("duckdbDisclaimer").ValueKind);
        Assert.Equal(402_653_184L, budget.GetProperty("channelBytes").GetInt64());
        Assert.Equal(268_435_456L, budget.GetProperty("fixedOverheadBytes").GetInt64());
        Assert.Equal(1_744_830_464L, budget.GetProperty("totalBytes").GetInt64());
    }

    [Fact]
    public void Plan_json_budget_renders_disclaimer_string_when_duckdb_bytes_unknown()
    {
        var budget = new MemoryBudget(null, "duckdb.memory_limit is not set", 100L, 200L, 300L);
        var plan = new ExecutionPlan(
            [new PlannedNode(new NodeId("dddddddddddddddd"), NodeKind.SourceLoad, "n",
                EdgeStrategy.ArrowStream, 1, "arrow stream: connector 'x' has no native path")],
            budget);

        PlanWriter.Write(plan, _dir);
        var text = File.ReadAllText(Path.Combine(_dir, "plan.json"));

        using var document = JsonDocument.Parse(text);
        var written = document.RootElement.GetProperty("memoryBudget");
        Assert.Equal(JsonValueKind.Null, written.GetProperty("duckdbBytes").ValueKind);
        Assert.Equal("duckdb.memory_limit is not set", written.GetProperty("duckdbDisclaimer").GetString());
    }

    /// <summary>The unset-engine.duckdb.threads caveat reaches plan.json,
    /// not just the console — a plan.json consumer reading totalBytes as a capacity promise is exactly
    /// the reader this caveat exists for.</summary>
    [Fact]
    public void Plan_json_budget_carries_the_threads_disclaimer()
    {
        var budget = MemoryBudget.Compute(
            new EngineConfig(Threads: 2, DuckDb: new DuckOptionsConfig(MemoryLimit: "1GiB")));
        var plan = new ExecutionPlan(
            [new PlannedNode(new NodeId("eeeeeeeeeeeeeeee"), NodeKind.SourceLoad, "n",
                EdgeStrategy.ArrowStream, 1, "arrow stream: connector 'x' has no native path")],
            budget);

        PlanWriter.Write(plan, _dir);
        var text = File.ReadAllText(Path.Combine(_dir, "plan.json"));

        using var document = JsonDocument.Parse(text);
        var written = document.RootElement.GetProperty("memoryBudget");
        Assert.Equal(MemoryBudget.ThreadsDisclaimer, written.GetProperty("duckdbThreadsDisclaimer").GetString());
    }

    [Fact]
    public void Plan_json_has_no_absolute_paths()
    {
        var plan = new ExecutionPlan(
            [
                new PlannedNode(new NodeId("bbbbbbbbbbbbbbbb"), NodeKind.SourceLoad, "src_crm__orders",
                    EdgeStrategy.NativeScan, 1, "native scan: connector 'localfiles' provides read_csv over data/orders.csv"),
                new PlannedNode(new NodeId("cccccccccccccccc"), NodeKind.SinkWrite, "lake.orders_curated",
                    EdgeStrategy.ArrowStream, 1, "arrow stream: connector 'localfiles' has no native path"),
            ],
            SampleBudget);

        PlanWriter.Write(plan, _dir);
        var text = File.ReadAllText(Path.Combine(_dir, "plan.json"));

        Assert.DoesNotContain(_dir, text);
        Assert.DoesNotContain("/home", text);
    }
}
