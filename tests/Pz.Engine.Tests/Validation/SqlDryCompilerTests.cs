using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;
using Pz.Engine.Validation;

namespace Pz.Engine.Tests.Validation;

/// <summary>Tier 4 (`SqlDryCompiler`): EXPLAIN-by-materialization against contract-derived empty tables
/// in a throwaway file-backed DuckDB session. Builds tiny hand-written <see cref="CompiledDag"/>s the
/// way <c>NodeExecutorTests</c> does (real DuckDB, no fakes) for the unit-level behaviors, plus two
/// tests loading real project trees (the Fixtures/hello-pz golden fixture vs. a mutated copy of the
/// shipped templates/sample) to make each one's own undeclared dataset (and which pipeline(s) it
/// skips) visible.</summary>
public sealed class SqlDryCompilerTests
{
    private static ConnectionDef Source(string name, string dataset, IReadOnlyDictionary<string, string>? columns) =>
        new(name, "test", new Dictionary<string, object?>(),
            [new DatasetDef(dataset, new Dictionary<string, object?>(), columns)], $"sources/{name}.yml");

    private static DagNode SourceLoadNode(NodeId id, string source, string dataset, IReadOnlyDictionary<string, string>? columns)
    {
        var def = Source(source, dataset, columns);
        return new DagNode(id, NodeKind.SourceLoad, $"src_{source}__{dataset}", [], null,
            new SourceDatasetDef(def, def.Datasets[0]));
    }

    private static DagNode PipelineNode(
        NodeId id, string name, string renderedSql, IReadOnlyList<NodeId> dependsOn, string materialization = "table")
    {
        var def = new PipelineDef(name, renderedSql, materialization, [], [], $"pipelines/{name}.sql");
        return new DagNode(id, NodeKind.Pipeline, name, dependsOn, renderedSql, def);
    }

    private static NodeId Id(string suffix) => new(suffix.PadLeft(16, '0'));

    [Fact]
    public async Task Typo_in_column_is_PZ0401_naming_pipeline_file()
    {
        var srcId = Id("1");
        var src = SourceLoadNode(srcId, "s", "d", new Dictionary<string, string> { ["id"] = "bigint" });
        var pipeline = PipelineNode(Id("2"), "bad", "select idd from staging.src_s__d", [srcId]);
        var dag = new CompiledDag([src, pipeline]);

        var result = await SqlDryCompiler.RunAsync(dag, default);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.SqlDryCompile, error.Code);
        Assert.Contains("idd", error.Message, StringComparison.Ordinal);
        Assert.Equal("pipelines/bad.sql", error.File);
        Assert.Empty(result.SkippedPipelines);
    }

    [Fact]
    public async Task Valid_project_produces_no_errors_and_no_leftover_files()
    {
        var srcId = Id("1");
        var src = SourceLoadNode(srcId, "s", "d", new Dictionary<string, string> { ["id"] = "bigint" });
        var pipeline = PipelineNode(Id("2"), "good", "select id from staging.src_s__d", [srcId]);
        var dag = new CompiledDag([src, pipeline]);

        // An isolated root (never the machine-global %TMP%/pz-dry-compile): the leftover-files
        // assertion below is only meaningful over a directory nothing else writes to — counting the
        // shared dir races every concurrent dry-compile in the suite.
        var tempRoot = Path.Combine(Path.GetTempPath(), "pz-tests", "dry-compile-" + Guid.NewGuid().ToString("N"));
        try
        {
            var result = await SqlDryCompiler.RunAsync(dag, default, tempRoot);

            Assert.Empty(result.Errors);
            Assert.Empty(result.SkippedPipelines);
            Assert.Empty(result.UndeclaredDatasets);

            Assert.Empty(Directory.GetFiles(tempRoot));
        }
        finally
        {
            try { Directory.Delete(tempRoot, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public async Task Undeclared_dataset_skips_dependents_without_error()
    {
        var srcId = Id("1");
        var src = SourceLoadNode(srcId, "s", "d", null); // no columns: contract
        var pipeline = PipelineNode(Id("2"), "over_undeclared", "select * from staging.src_s__d", [srcId]);
        var dag = new CompiledDag([src, pipeline]);

        var result = await SqlDryCompiler.RunAsync(dag, default);

        Assert.Empty(result.Errors);
        Assert.Contains("s.d", result.UndeclaredDatasets);
        Assert.Contains("over_undeclared", result.SkippedPipelines);
    }

    [Fact]
    public async Task Downstream_of_failing_pipeline_is_skipped_not_errored()
    {
        var srcId = Id("1");
        var src = SourceLoadNode(srcId, "s", "d", new Dictionary<string, string> { ["id"] = "bigint" });
        var aId = Id("2");
        var a = PipelineNode(aId, "a", "select nope from staging.src_s__d", [srcId]);
        var b = PipelineNode(Id("3"), "b", "select * from staging.a", [aId]);
        var dag = new CompiledDag([src, a, b]);

        var result = await SqlDryCompiler.RunAsync(dag, default);

        var error = Assert.Single(result.Errors);
        Assert.Equal("pipelines/a.sql", error.File);
        Assert.Contains("b", result.SkippedPipelines);
    }

    [Fact]
    public async Task View_materialization_is_validated()
    {
        var srcId = Id("1");
        var src = SourceLoadNode(srcId, "s", "d", new Dictionary<string, string> { ["id"] = "bigint" });
        var pipeline = PipelineNode(Id("2"), "a_view", "select nope from staging.src_s__d", [srcId], "view");
        var dag = new CompiledDag([src, pipeline]);

        var result = await SqlDryCompiler.RunAsync(dag, default);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.SqlDryCompile, error.Code);
        Assert.Contains("nope", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("int", "integer")]
    [InlineData("bigint", "bigint")]
    [InlineData("double", "double")]
    [InlineData("decimal", "decimal(38,9)")]
    [InlineData("varchar", "varchar")]
    [InlineData("boolean", "boolean")]
    [InlineData("date", "date")]
    [InlineData("timestamp", "timestamp")]
    public void ContractTypes_maps_all_eight_and_rejects_unknown(string contractType, string expectedDdl)
    {
        Assert.Equal(expectedDdl, ContractTypes.ToDuckDdl(contractType));
    }

    [Fact]
    public void ContractTypes_maps_all_eight_and_rejects_unknown_unknown_type_throws()
    {
        Assert.Throws<ArgumentException>(() => ContractTypes.ToDuckDdl("notatype"));
    }

    // --- Fixtures/hello-pz golden fixture vs. a mutated templates/sample copy: each has its own
    // undeclared dataset ---

    [Fact]
    public async Task HelloPz_golden_fixture_orders_dataset_has_no_contract_and_skips_its_dependents()
    {
        var dag = CompileHelloPz(Path.Combine(FindRepoRoot(), "tests", "Pz.Core.Tests", "Fixtures", "hello-pz"),
            new Dictionary<string, string> { ["DATA_DIR"] = "/tmp/pz-data", ["OUT_DIR"] = "/tmp/pz-out" });

        var result = await SqlDryCompiler.RunAsync(dag, default);

        Assert.Empty(result.Errors);
        Assert.Contains("crm.orders", result.UndeclaredDatasets);
        Assert.Contains("stg_orders", result.SkippedPipelines);
        Assert.Contains("orders_enriched", result.SkippedPipelines);
    }

    /// <summary>Every dataset in the shipped `templates/sample` now carries a `columns:` contract, so
    /// this test works from a COPY with `customers`' contract stripped back out of `connections.yml` --
    /// reconstructing the contract-less-csv case a contract-less `localfiles` csv source runs fine
    /// through via `pz run`'s native-scan tier. Tier 4 dry-compile is a different code path and cannot
    /// dry-compile without a contract, so `raw.customers` shows up as undeclared and its one reader,
    /// `orders_enriched`, is skipped -- while `raw.orders` (contract untouched) and its dependents
    /// (`stg_orders`, `order_totals`) dry-compile normally.</summary>
    [Fact]
    public async Task Sample_copy_customers_dataset_has_no_contract_and_skips_orders_enriched_only()
    {
        var work = Path.Combine(Path.GetTempPath(), "pz-sql-dry-compiler-tests", Guid.NewGuid().ToString("N"));
        CopyTree(Path.Combine(FindRepoRoot(), "templates", "sample"), work);
        try
        {
            var connectionsPath = Path.Combine(work, "connections.yml");
            var connectionsYml = File.ReadAllText(connectionsPath).Replace(
                "    customers:\n      read:\n        path: data/customers.csv\n        format: csv\n" +
                "        columns:\n          id: bigint\n          email: varchar\n",
                "    customers:\n      read:\n        path: data/customers.csv\n        format: csv\n");
            File.WriteAllText(connectionsPath, connectionsYml);

            var dag = CompileHelloPz(work, new Dictionary<string, string>());

            var result = await SqlDryCompiler.RunAsync(dag, default);

            Assert.Empty(result.Errors);
            Assert.Contains("raw.customers", result.UndeclaredDatasets);
            Assert.Contains("orders_enriched", result.SkippedPipelines);
            Assert.DoesNotContain("raw.orders", result.UndeclaredDatasets);
            Assert.DoesNotContain("stg_orders", result.SkippedPipelines);
            Assert.DoesNotContain("order_totals", result.SkippedPipelines);
        }
        finally
        {
            try { Directory.Delete(work, recursive: true); } catch { /* best-effort cleanup */ }
        }
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

    private static CompiledDag CompileHelloPz(string projectDir, IReadOnlyDictionary<string, string> env)
    {
        var project = ProjectLoader.Load(projectDir, env);
        var ctx = new RenderContext(project, "test-run", DateTimeOffset.UnixEpoch) { Env = env };
        return DagCompiler.Compile(project, ctx);
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Pz.slnx")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName ?? throw new InvalidOperationException("Pz.slnx not found above test base dir");
    }
}
