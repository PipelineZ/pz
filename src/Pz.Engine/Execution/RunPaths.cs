namespace Pz.Engine.Execution;

public sealed record RunPaths(string ProjectDir, string RunId)
{
    public string RunDir => Path.Combine(ProjectDir, ".pz", "runs", RunId);
    public string StagingDbPath => Path.Combine(RunDir, "staging.duckdb");
    public string RunResultsPath => Path.Combine(RunDir, "run_results.json");
}
