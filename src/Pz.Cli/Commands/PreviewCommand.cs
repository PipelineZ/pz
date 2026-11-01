using System.CommandLine;
using System.Globalization;
using System.Text.RegularExpressions;
using Pz.Core.Validation;
using Pz.Engine.Preview;

namespace Pz.Cli.Commands;

internal sealed record PreviewArgs(string Node, string? RunId, string? RunDir, bool Fresh, int Limit, int? Slice,
    bool Json, string ProjectDir, string? VarsJson);

/// <summary>`pz preview &lt;node&gt;`: a small, redacted sample of a source or pipeline, read from a finished
/// run's staging (<c>--run</c>/<c>--run-dir</c>) or computed fresh (<c>--fresh</c>). With <c>--json</c>
/// stdout is exactly one JSON object, the contract PipelineX parses; everything else goes to stderr.</summary>
internal static partial class PreviewCommand
{
    internal const int DefaultLimit = 50;
    internal const int MaxLimit = 500;
    internal const int DefaultSlice = 1000;
    internal const int MaxSlice = 100_000;

    [GeneratedRegex("^[A-Za-z0-9._-]+$")]
    private static partial Regex PlainRunId();

    public static Command Create()
    {
        var nodeArgument = new Argument<string>("node") { Description = "Source or pipeline node name (see `pz ls`)" };
        var runOption = new Option<string?>("--run") { Description = "Read a finished run's staging: .pz/runs/<id>" };
        var runDirOption = new Option<string?>("--run-dir")
        {
            Description = "Read a run directory directly; needs no project files",
        };
        var freshOption = new Option<bool>("--fresh")
        {
            Description = "Compute the node now from a slice of each source; never sinks, never state",
        };
        var limitOption = new Option<int>("--limit")
        {
            Description = $"Rows to show, 1-{MaxLimit}",
            DefaultValueFactory = _ => DefaultLimit,
        };
        var sliceOption = new Option<int?>("--slice")
        {
            Description = $"With --fresh: rows read per source, 1-{MaxSlice} (default {DefaultSlice})",
        };
        var jsonOption = new Option<bool>("--json") { Description = "Print one JSON object instead of a table" };
        var projectOption = new Option<string?>("--project")
        {
            Description = "Project directory (default: current directory)",
        };
        var varsOption = new Option<string?>("--vars") { Description = "With --fresh: JSON object of var overrides" };

        var command = new Command("preview", "Show a sample of a source or pipeline, from a run or fresh");
        command.Arguments.Add(nodeArgument);
        foreach (var option in new Option[]
                 {
                     runOption, runDirOption, freshOption, limitOption, sliceOption, jsonOption, projectOption, varsOption,
                 })
        {
            command.Options.Add(option);
        }

        command.SetAction((parseResult, ct) => Execute(new PreviewArgs(
            parseResult.GetValue(nodeArgument)!,
            parseResult.GetValue(runOption),
            parseResult.GetValue(runDirOption),
            parseResult.GetValue(freshOption),
            parseResult.GetValue(limitOption),
            parseResult.GetValue(sliceOption),
            parseResult.GetValue(jsonOption),
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(varsOption)), ct));

        return command;
    }

    internal static async Task<int> Execute(PreviewArgs args, CancellationToken ct)
    {
        if (ValidateOptions(args) is { } invalid)
        {
            return Fail(args, invalid, ExitCodes.ConfigError);
        }

        try
        {
            var result = args.Fresh
                ? await PreviewFresh.RunAsync(args.ProjectDir, args.VarsJson, args.Node, args.Limit,
                    args.Slice ?? DefaultSlice, message => Console.Error.WriteLine($"note: {message}"), ct)
                : await PreviewRunReader.ReadAsync(
                    args.RunDir ?? Path.Combine(args.ProjectDir, ".pz", "runs", args.RunId!),
                    args.RunId, args.Node, args.Limit, ct);
            Console.Out.Write(args.Json ? PreviewOutput.ToJson(result) + "\n" : PreviewOutput.ToTable(result));
            return ExitCodes.Ok;
        }
        catch (PzValidationException ex)
        {
            return Fail(args, ex.Errors[0], ExitCodes.ConfigError);
        }
        catch (PreviewNodeFailedException ex)
        {
            return Fail(args, ex.Error, ExitCodes.NodeFailures);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var error = EngineFailureMapper.TryMap(ex) ?? new PzError(PzErrorCode.UnexpectedEngineFailure,
                $"pz preview failed: {ex.Message}", null, null, null);
            return Fail(args, error, ExitCodes.Fatal);
        }
    }

    /// <summary>The option rules; every break is PZ0539 naming the rule.</summary>
    internal static PzError? ValidateOptions(PreviewArgs args)
    {
        var modes = (args.RunId is null ? 0 : 1) + (args.RunDir is null ? 0 : 1) + (args.Fresh ? 1 : 0);
        if (modes != 1)
        {
            return Options("give exactly one of --run <id>, --run-dir <path>, --fresh");
        }

        if (args.Limit is < 1 or > MaxLimit)
        {
            return Options($"--limit must be 1-{MaxLimit} (got {args.Limit.ToString(CultureInfo.InvariantCulture)})");
        }

        if (args.Slice is { } slice)
        {
            if (!args.Fresh)
            {
                return Options("--slice applies only with --fresh");
            }

            if (slice is < 1 or > MaxSlice)
            {
                return Options($"--slice must be 1-{MaxSlice} (got {slice.ToString(CultureInfo.InvariantCulture)})");
            }
        }

        if (args.VarsJson is not null && !args.Fresh)
        {
            return Options("--vars applies only with --fresh; a run's values are already in its staging");
        }

        if (args.RunId is { } runId && (!PlainRunId().IsMatch(runId) || runId is "." or ".."))
        {
            return Options($"--run takes a run id, a plain directory name under .pz/runs (got '{runId}')");
        }

        return null;
    }

    private static PzError Options(string rule) =>
        new(PzErrorCode.PreviewOptionsInvalid, $"pz preview: {rule}.", null, null, "see `pz preview --help`");

    private static int Fail(PreviewArgs args, PzError error, int exit)
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
