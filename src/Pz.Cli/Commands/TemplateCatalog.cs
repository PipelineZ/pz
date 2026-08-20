namespace Pz.Cli.Commands;

/// <summary>What a scaffolded template needs before `pz run` does anything useful. Drives both the
/// `--list-templates` marker and how far the test suite can exercise each template: only
/// <see cref="Offline"/> ones can be run in CI, the rest get compile-level coverage.</summary>
internal enum TemplateRunnability
{
    /// <summary>No pipelines: it loads and compiles, but there is nothing to execute.</summary>
    Nothing,
    Offline,
    NeedsNetwork,
    NeedsDatabase,
}

/// <summary><paramref name="NextSteps"/> is printed verbatim after scaffolding, with
/// <c>{0}</c> replaced by the sanitized project name. It is per-template because the single
/// hardcoded hint this replaced named a pipeline that only one template contains.</summary>
internal sealed record TemplateInfo(
    string Id,
    string Summary,
    TemplateRunnability Runnability,
    string NextSteps);

/// <summary>Every built-in starting point `pz init` can scaffold. Adding a template means adding a
/// directory under <c>templates/</c> and an entry here; the two are asserted equal as sets, in both
/// directions, so neither half can be forgotten.
///
/// Held in code rather than a manifest file inside each template directory: a manifest would be a
/// fifth required file that must then be excluded from the copy, and it would stop each directory
/// from being a plain pz project -- which is the property that lets the test suite compile the real
/// scaffold source rather than a copy of it.</summary>
internal static class TemplateCatalog
{
    /// <summary>What `pz init <name>` scaffolds with no `--template`. Minimal because that is what
    /// someone starting their own project wants: the sample's demo files COMPILE, so until they are
    /// deleted `pz run --all` moves data nobody asked for.</summary>
    public const string DefaultId = "minimal";

    public static readonly IReadOnlyList<TemplateInfo> All =
    [
        new("minimal",
            "project.yml + connections.yml, commented and ready to author against",
            TemplateRunnability.Nothing,
            "  cd {0}, declare a connection in connections.yml, then add a pipeline\n" +
            "  under pipelines/ that source()s from it -- `pz validate` checks both"),
        new("sample",
            "runnable four-pipeline demo over local CSVs: staging, a checked join, an aggregate",
            TemplateRunnability.Offline,
            "  cd {0} && pz run orders_enriched\n" +
            "  (this template ships two independent flows; `pz run --all` runs both)"),
        new("http",
            "GitHub REST API to a parquet delta log: pagination, a crawl guard, a typed contract",
            TemplateRunnability.NeedsNetwork,
            "  cd {0} && pz run --all\n" +
            "  (reads the public GitHub API -- needs internet, no credentials)"),
        new("incremental",
            "watermark-bounded reads over local CSVs: run it twice, see the second run land nothing",
            TemplateRunnability.Offline,
            "  cd {0} && pz run --all\n" +
            "  then run it again -- the second run extracts nothing, which is the point"),
        new("sqlserver",
            "SQL Server to SQL Server: incremental merge, five kinds of check, optional remote state",
            TemplateRunnability.NeedsDatabase,
            "  cd {0}, export the ERP_DB_* and MART_DB_* variables connections.yml names,\n" +
            "  then `pz validate --connect` before `pz run --all`"),
    ];

    public static TemplateInfo? Find(string id) =>
        All.FirstOrDefault(t => string.Equals(t.Id, id, StringComparison.Ordinal));
}
