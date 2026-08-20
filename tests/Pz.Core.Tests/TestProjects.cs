using Pz.Core.Model;
using Pz.Core.Templating;

namespace Pz.Core.Tests;

public static class TestProjects
{
    public static PipelineDef Pipe(string name, string sql,
        string materialization = "table", string[]? tags = null, CheckDef[]? checks = null) =>
        new(name, sql, materialization, tags ?? [], checks ?? [], $"pipelines/{name}.sql");

    public static ConnectionDef Crm(params string[] datasets) =>
        new("crm", "localfiles", new Dictionary<string, object?> { ["root"] = "/data" },
            datasets.Select(d => new DatasetDef(d,
                new Dictionary<string, object?> { ["path"] = $"{d}.csv", ["format"] = "csv" }, null)).ToList(),
            "connections.yml");

    /// <summary>A single-dataset source, optionally declaring an <c>incremental:</c> cursor and/or a
    /// <c>columns:</c> contract -- for the PZ0212 cursor-validation tests.</summary>
    public static ConnectionDef CrmIncremental(string dataset, string cursor,
        IReadOnlyDictionary<string, string>? columns = null) =>
        new("crm", "localfiles", new Dictionary<string, object?> { ["root"] = "/data" },
            [new DatasetDef(dataset, new Dictionary<string, object?> { ["path"] = $"{dataset}.csv", ["format"] = "csv" },
                columns, new SyncModeDef(SyncMode.Incremental, new IncrementalDef(cursor)))],
            "connections.yml");

    /// <summary>Several incremental datasets on one source, sharing a cursor and columns contract.
    /// For facts that need one pipeline per sink output to prove sink-side errors aggregate: since
    /// a source dataset is read by exactly one pipeline (PZ0349), so each such pipeline
    /// needs its own dataset rather than all reading one.</summary>
    public static ConnectionDef CrmIncrementalMany(string cursor,
        IReadOnlyDictionary<string, string>? columns, params string[] datasets) =>
        new("crm", "localfiles", new Dictionary<string, object?> { ["root"] = "/data" },
            datasets.Select(d => new DatasetDef(d,
                new Dictionary<string, object?> { ["path"] = $"{d}.csv", ["format"] = "csv" },
                columns, new SyncModeDef(SyncMode.Incremental, new IncrementalDef(cursor)))).ToList(),
            "connections.yml");

    /// <summary>A connection-only sink. A ConnectionDef
    /// carries no outputs from YAML -- DagCompiler synthesizes them from the sink() call sites, so a
    /// test declares the sink here and its write options in the pipeline SQL via <see cref="Into"/>.</summary>
    public static ConnectionDef Sink(string name = "lake") =>
        new(name, "localfiles", new Dictionary<string, object?> { ["root"] = "/out" }, [],
            "connections.yml");

    /// <summary>The leading <c>INSERT INTO {{ sink(...) }}</c> line, carrying these tests' write
    /// options. <paramref name="format"/> defaults to parquet: it rides OutputDef.Options, which feeds
    /// the SinkWrite NodeId, so dropping it would shift hashes for no reason.</summary>
    public static string Into(string output, string? strategy = null, string[]? keys = null,
        string sink = "lake", string? duplicates = null, string? onDelete = null,
        string? format = "parquet")
    {
        var kwargs = new List<string>();
        if (strategy is not null) { kwargs.Add($"strategy: '{strategy}'"); }
        if (keys is { Length: > 0 }) { kwargs.Add($"keys: ['{string.Join("', '", keys)}']"); }
        if (duplicates is not null) { kwargs.Add($"duplicates: '{duplicates}'"); }
        if (onDelete is not null) { kwargs.Add($"on_delete: '{onDelete}'"); }
        if (format is not null) { kwargs.Add($"format: '{format}'"); }
        var args = kwargs.Count > 0 ? ", " + string.Join(", ", kwargs) : "";
        return $"INSERT INTO {{{{ sink('{sink}', '{output}'{args}) }}}}\n";
    }

    public static PzProject Project(
        IEnumerable<PipelineDef> pipelines,
        IEnumerable<ConnectionDef>? sources = null,
        IEnumerable<ConnectionDef>? sinks = null,
        EngineConfig? engine = null) =>
        // The two parameters stay -- a call site reads better naming the direction it means -- but they
        // land in ONE list: a connection is a place.
        new("t", "0.0.0", engine ?? new EngineConfig(),
            new Dictionary<string, object?> { ["min_amount"] = 10L },
            [], [.. sources ?? [], .. sinks ?? []], pipelines.ToList());

    public static RenderContext Ctx(PzProject project) =>
        new(project, "run-1", new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
}
