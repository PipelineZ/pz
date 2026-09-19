using System.Buffers;
using System.CommandLine;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Pz.Core.Loading;
using Pz.Core.Validation;
using Pz.Engine.Artifacts;

namespace Pz.Cli.Commands;

/// <summary>`pz runs`: lists prior runs, newest first, over <see cref="IRunArtifactStore.ReadAllNewestFirst"/>
/// -- lazy on every backend (local files, SQL Server), so this works identically under
/// <c>state: {backend: sqlserver}</c> without a "the remote store can only report the latest run"
/// fallback. A management verb like `pz clean`/`pz state`: it reads only <c>state:</c> out of
/// project.yml (<see cref="ProjectLoader.LoadStateOnly"/>), never the full project, so a broken pipeline
/// or connections.yml is never the reason this verb refuses to report what already ran.</summary>
internal static class RunsCommand
{
    public static Command Create()
    {
        var projectOption = new Option<string?>("--project")
        {
            Description = "Project directory (default: current directory)",
        };
        var jsonOption = new Option<bool>("--json")
        {
            Description = "Print one JSON object per run (NDJSON) instead of a table",
        };
        var limitOption = new Option<int?>("--limit")
        {
            Description = "Show only the N most recent runs (default: all)",
        };

        var command = new Command("runs", "List prior runs, newest first, with their status, timing, and provenance");
        command.Options.Add(projectOption);
        command.Options.Add(jsonOption);
        command.Options.Add(limitOption);
        command.SetAction(parseResult => Execute(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(jsonOption),
            parseResult.GetValue(limitOption)));

        return command;
    }

    internal static int Execute(string projectDir, bool json, int? limit)
    {
        if (limit is < 1)
        {
            Console.Error.WriteLine(
                $"error {PzErrorCode.RunsLimitInvalid}: --limit must be 1 or greater " +
                $"(got '{limit.Value.ToString(CultureInfo.InvariantCulture)}') — omit it to show every run");
            return ExitCodes.ConfigError;
        }

        if (!File.Exists(Path.Combine(projectDir, "project.yml")))
        {
            Console.Error.WriteLine(
                $"error {PzErrorCode.YamlShape}: project.yml is missing — run pz runs from a project directory " +
                "or pass --project <dir>");
            return ExitCodes.ConfigError;
        }

        StateBackends backends;
        try
        {
            var (name, state, connections) =
                ProjectLoader.LoadStateOnly(projectDir, SharedInputHelpers.SnapshotEnvironment());
            backends = StateBackendFactory.Create(state, name, connections, projectDir, TimeProvider.System);
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }

            return ExitCodes.ConfigError;
        }
        catch (PzConfigException ex)
        {
            Console.Error.WriteLine($"error {ex.Error}");
            return ExitCodes.ConfigError;
        }

        return Report(backends.Artifacts, json, limit);
    }

    internal static int Report(IRunArtifactStore store, bool json, int? limit)
    {
        var runs = store.ReadAllNewestFirst();
        if (limit is { } n)
        {
            runs = runs.Take(n);
        }

        // The store is read lazily, so a remote one fails here, part-way through, rather than above.
        // Nothing is printed before every row is in hand: half a listing reads as the whole history.
        List<RunRow> rows;
        try
        {
            rows = runs.Select(Summarize).ToList();
        }
        catch (PzConfigException ex)
        {
            Console.Error.WriteLine($"error {ex.Error}");
            return ExitCodes.ConfigError;
        }

        if (json)
        {
            foreach (var row in rows)
            {
                Console.WriteLine(ToJson(row));
            }

            return ExitCodes.Ok;
        }

        if (rows.Count == 0)
        {
            Console.WriteLine("no runs found");
            return ExitCodes.Ok;
        }

        Console.WriteLine(
            $"{"RUN ID",-30} {"STATUS",-25} {"STARTED AT",-25} {"DURATION",-9} {"OK",4} {"FAILED",7} {"SKIPPED",8}  PROVENANCE");
        foreach (var row in rows)
        {
            Console.WriteLine(
                $"{row.RunId,-30} {row.Status,-25} {row.StartedAtIso ?? "-",-25} {FormatDuration(row.DurationMs),-9} " +
                $"{row.Succeeded,4} {row.Failed,7} {row.Skipped,8}  {FormatProvenance(row)}");
        }

        return ExitCodes.Ok;
    }

    private sealed record RunRow(string RunId, string Status, string? StartedAtIso, string? FinishedAtIso,
        long? DurationMs, int Succeeded, int Failed, int Skipped, int Reused, int CarriedForward);

    private static RunRow Summarize(PriorRun run)
    {
        long? durationMs = null;
        if (run.StartedAtIso is not null && run.FinishedAtIso is not null &&
            DateTimeOffset.TryParse(run.StartedAtIso, CultureInfo.InvariantCulture, DateTimeStyles.None, out var started) &&
            DateTimeOffset.TryParse(run.FinishedAtIso, CultureInfo.InvariantCulture, DateTimeStyles.None, out var finished))
        {
            durationMs = Math.Max(0, (long)(finished - started).TotalMilliseconds);
        }

        var succeeded = run.Nodes.Count(node => node.Status == "success");
        var failed = run.Nodes.Count(node => node.Status == "failed");
        var skipped = run.Nodes.Count(node => node.Status == "skipped");
        var reused = run.Nodes.Count(node => node.Provenance == "reused");
        var carriedForward = run.Nodes.Count(node => node.Provenance == "carried_forward");

        return new RunRow(run.RunId, run.Status, run.StartedAtIso, run.FinishedAtIso, durationMs,
            succeeded, failed, skipped, reused, carriedForward);
    }

    /// <summary>Mirrors <see cref="PlanCommand.FormatDuration"/>'s unit selection so a run's duration
    /// reads the same way `pz plan`'s retry-policy delays do — "-" only when the run has not finished
    /// (`FinishedAtIso` still null), never for a genuinely instant run.</summary>
    private static string FormatDuration(long? ms) =>
        ms is { } value ? PlanCommand.FormatDuration(TimeSpan.FromMilliseconds(value)) : "-";

    private static string FormatProvenance(RunRow row) => (row.Reused, row.CarriedForward) switch
    {
        (0, 0) => "-",
        (var reused, 0) => $"{reused.ToString(CultureInfo.InvariantCulture)} reused",
        (0, var carried) => $"{carried.ToString(CultureInfo.InvariantCulture)} carried_forward",
        (var reused, var carried) =>
            $"{reused.ToString(CultureInfo.InvariantCulture)} reused, {carried.ToString(CultureInfo.InvariantCulture)} carried_forward",
    };

    /// <summary>Byte-stable: explicit field order, no indentation, invariant-culture numbers (Utf8JsonWriter
    /// is always invariant), UTC ISO-8601 timestamps exactly as persisted by <c>RunResultsWriter</c>/
    /// <c>SqlRunArtifactStore</c> — same discipline as <see cref="Pz.Cli.Rendering.JsonRenderer"/>. No
    /// reflection-based (de)serialization, so this stays Native-AOT clean.</summary>
    private static string ToJson(RunRow row)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var json = new Utf8JsonWriter(buffer, new JsonWriterOptions { Indented = false }))
        {
            json.WriteStartObject();
            json.WriteString("runId", row.RunId);
            json.WriteString("status", row.Status);
            if (row.StartedAtIso is null) json.WriteNull("startedAt"); else json.WriteString("startedAt", row.StartedAtIso);
            if (row.FinishedAtIso is null) json.WriteNull("finishedAt"); else json.WriteString("finishedAt", row.FinishedAtIso);
            if (row.DurationMs is null) json.WriteNull("durationMs"); else json.WriteNumber("durationMs", row.DurationMs.Value);
            json.WriteNumber("succeeded", row.Succeeded);
            json.WriteNumber("failed", row.Failed);
            json.WriteNumber("skipped", row.Skipped);
            json.WriteNumber("reused", row.Reused);
            json.WriteNumber("carriedForward", row.CarriedForward);
            json.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }
}
