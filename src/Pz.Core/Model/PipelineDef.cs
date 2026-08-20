namespace Pz.Core.Model;

public sealed record PipelineDef(string Name, string RawSql, string Materialization,
    IReadOnlyList<string> Tags, IReadOnlyList<CheckDef> Checks, string FilePath);

/// <summary><see cref="SampleValues"/> is the per-check PII opt-out override for sample-row reporting
/// on a failing check -- <c>null</c> when the check's YAML declared no <c>sample_values:</c> key,
/// meaning "inherit the project-wide <c>engine.check_samples</c> default" (resolved by
/// <c>DagCompiler</c> into <c>CheckNodeDef.SampleValues</c>, which defaults to <c>true</c>).</summary>
public sealed record CheckDef(string Type, IReadOnlyList<string> Columns,
    IReadOnlyDictionary<string, object?> Options, bool? SampleValues = null);
