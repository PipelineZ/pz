using System.CommandLine;

namespace Pz.Cli.Commands;

/// <summary>Options shared by verbs that operate on a resolved <c>CompiledDag</c> (compile, run, ...).</summary>
public static class SharedOptions
{
    public static Option<string?> Select { get; } =
        new("--select") { Description = "dbt-style node selector expression restricting which nodes are processed" };

    /// <summary>The explicit whole-project spelling for `run`/`plan`. Mutually exclusive with positional
    /// flow names and --select (PZ0216).</summary>
    public static Option<bool> All { get; } =
        new("--all")
        {
            Description = "Select the whole project (required for bare `pz run` when the project has 2+ independent flows)",
        };

    /// <summary>Loud bypass of <see cref="Pz.Cli.ConnectorRegistryFactory"/>'s drift check
    /// (declared connector requirements vs. pz.lock.json). A missing pz.lock.json is never bypassed by
    /// this flag — only mismatches between an EXISTING lock and the current project.yml are skipped.</summary>
    public static Option<bool> NoLockCheck { get; } =
        new("--no-lock-check") { Description = "Skip pz.lock.json drift verification against project.yml (loud bypass; does not skip a missing lock)" };

    /// <summary>`text` (default) renders a live Spectre tree on an interactive TTY, or plain sequential
    /// lines otherwise (CI/non-TTY); `json` renders one
    /// NDJSON object per run event on stdout instead. Validated in <c>RunCommand.Execute</c>/
    /// <c>TestCommand.Execute</c> (an unrecognized value is a PZ0### config error, not a crash).</summary>
    public static Option<string?> LogFormat { get; } =
        new("--log-format") { Description = "Output format: text (default) or json (NDJSON, one object per run event)" };

    /// <summary>An absolute http(s) OTLP/grpc collector endpoint. Falls back to the
    /// <c>PZ_OTEL_ENDPOINT</c> environment variable when unset (this option
    /// wins over the env var when both are present); validated in
    /// <see cref="RunCommand.TryResolveOtelEndpoint"/> (an unparseable value is a clean CLI usage error,
    /// not a crash). Unset (and no env var) means OTel stays fully off — no listener is ever registered,
    /// so span/meter emission in Pz.Engine remains the documented zero-cost no-op.</summary>
    public static Option<string?> OtelEndpoint { get; } =
        new("--otel-endpoint")
        {
            Description = "OTLP/grpc collector endpoint (absolute http(s) URL); falls back to PZ_OTEL_ENDPOINT",
        };

    /// <summary>See <see cref="StateUrlOverride"/>: the explicit, argv-visible spelling of "this run's
    /// state lives on the server". Outranks project.yml's
    /// state: block and every PZ_STATE_* variable; the bearer token still rides PZ_STATE_TOKEN.</summary>
    public static Option<string?> StateUrl { get; } =
        new("--state-url")
        {
            Description = "Run-scoped http state endpoint (absolute http(s) URL); outranks project.yml " +
                "state: and PZ_STATE_*; bearer token still from PZ_STATE_TOKEN",
        };
}
