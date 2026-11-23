using System.CommandLine;
using System.Globalization;
using System.Text.RegularExpressions;
using Pz.Core.Validation;
using Pz.Engine.Staging;

namespace Pz.Cli.Commands;

internal sealed record StagingQueryArgs(string? RunId, string? RunDir, string? Sql, int Limit, bool Json,
    string ProjectDir);

/// <summary>`pz staging query`: one read-only SELECT against a finished run's staging, printed as a table
/// or, with <c>--json</c>, as exactly one JSON object on stdout (the contract PipelineX parses, shaped like
/// <c>pz preview --json</c>); everything else goes to stderr.</summary>
internal static partial class StagingCommand
{
    internal const int DefaultLimit = 50;

    [GeneratedRegex("^[A-Za-z0-9._-]+$")]
    private static partial Regex PlainRunId();

    public static Command Create()
    {
        var runOption = new Option<string?>("--run") { Description = "Query a finished run's staging: .pz/runs/<id>" };
        var runDirOption = new Option<string?>("--run-dir") { Description = "Query a run directory directly; needs no project files" };
        var sqlOption = new Option<string?>("--sql")
        {
            Description = "One SELECT over the run's tables, named staging.<name>",
        };
        var limitOption = new Option<int>("--limit")
        {
            Description = $"Rows to return, 1-{StagingQueryRunner.MaxLimit}",
            DefaultValueFactory = _ => DefaultLimit,
        };
        var jsonOption = new Option<bool>("--json") { Description = "Print one JSON object instead of a table" };
        var projectOption = new Option<string?>("--project") { Description = "Project directory (default: current directory)" };

        var query = new Command("query", "Run one read-only SELECT against a finished run's staging");
        foreach (var option in new Option[] { runOption, runDirOption, sqlOption, limitOption, jsonOption, projectOption })
        {
            query.Options.Add(option);
        }

        query.SetAction((parseResult, ct) => Query(new StagingQueryArgs(
            parseResult.GetValue(runOption),
            parseResult.GetValue(runDirOption),
            parseResult.GetValue(sqlOption),
            parseResult.GetValue(limitOption),
            parseResult.GetValue(jsonOption),
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory()), ct));

        var command = new Command("staging", "Inspect a finished run's staging database");
        command.Subcommands.Add(query);
        return command;
    }

    internal static async Task<int> Query(StagingQueryArgs args, CancellationToken ct)
    {
        if (ValidateOptions(args) is { } invalid)
        {
            return Fail(args, invalid, ExitCodes.ConfigError);
        }

        try
        {
            var result = await StagingQueryRunner.RunAsync(
                args.RunDir ?? Path.Combine(args.ProjectDir, ".pz", "runs", args.RunId!),
                args.RunId, args.Sql!, args.Limit, ct);
            Console.Out.Write(args.Json ? StagingQueryOutput.ToJson(result) + "\n" : StagingQueryOutput.ToTable(result));
            return ExitCodes.Ok;
        }
        catch (PzValidationException ex)
        {
            return Fail(args, ex.Errors[0], ExitCodes.ConfigError);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var error = EngineFailureMapper.TryMap(ex) ?? new PzError(PzErrorCode.UnexpectedEngineFailure,
                $"pz staging query failed: {ex.Message}", null, null, null);
            return Fail(args, error, ExitCodes.Fatal);
        }
    }

    /// <summary>The option rules; every break is PZ0544 naming the rule.</summary>
    internal static PzError? ValidateOptions(StagingQueryArgs args)
    {
        if ((args.RunId is null) == (args.RunDir is null))
        {
            return Options("give exactly one of --run <id>, --run-dir <path>");
        }

        if (args.RunId is { } runId && (!PlainRunId().IsMatch(runId) || runId is "." or ".."))
        {
            return Options($"--run takes a run id, a plain directory name under .pz/runs (got '{runId}')");
        }

        if (string.IsNullOrWhiteSpace(args.Sql) || StagingQueryRunner.Normalize(args.Sql).Length == 0)
        {
            return Options("--sql takes one SELECT over the run's tables");
        }

        if (args.Limit is < 1 or > StagingQueryRunner.MaxLimit)
        {
            return Options($"--limit must be 1-{StagingQueryRunner.MaxLimit} (got {args.Limit.ToString(CultureInfo.InvariantCulture)})");
        }

        return null;
    }

    private static PzError Options(string rule) =>
        new(PzErrorCode.StagingQueryOptionsInvalid, $"pz staging query: {rule}.", null, null,
            "see `pz staging query --help`");

    private static int Fail(StagingQueryArgs args, PzError error, int exit)
    {
        if (args.Json)
        {
            Console.Out.Write(PreviewOutput.ErrorJson(error) + "\n");
        }

        // Always on stderr too: a person running --json still sees why it failed.
        Console.Error.WriteLine($"error {error}");
        return exit;
    }
}
