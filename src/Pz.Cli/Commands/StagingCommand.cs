using System.CommandLine;
using System.Globalization;
using System.Text.RegularExpressions;
using Pz.Core.Validation;
using Pz.Engine.Staging;

namespace Pz.Cli.Commands;

internal sealed record StagingServeArgs(string? RunId, string? RunDir, string Host, int? Port, string ProjectDir);

/// <summary>`pz staging serve`: serves a finished run's staging read-only over the Quack protocol, until
/// Ctrl+C, so any DuckDB client can attach it and query the run's intermediate tables.</summary>
internal static partial class StagingCommand
{
    [GeneratedRegex("^[A-Za-z0-9._-]+$")]
    private static partial Regex PlainRunId();

    public static Command Create()
    {
        var runOption = new Option<string?>("--run") { Description = "Serve a finished run's staging: .pz/runs/<id>" };
        var runDirOption = new Option<string?>("--run-dir") { Description = "Serve a run directory directly; needs no project files" };
        var hostOption = new Option<string>("--host")
        {
            Description = "Address to listen on (default localhost; anything else is reachable from other machines)",
            DefaultValueFactory = _ => "localhost",
        };
        var portOption = new Option<int?>("--port") { Description = "Port to listen on (default: a free one)" };
        var projectOption = new Option<string?>("--project") { Description = "Project directory (default: current directory)" };

        var serve = new Command("serve", "Serve a finished run's staging read-only over the Quack protocol, until Ctrl+C");
        foreach (var option in new Option[] { runOption, runDirOption, hostOption, portOption, projectOption })
        {
            serve.Options.Add(option);
        }

        serve.SetAction((parseResult, ct) => Serve(new StagingServeArgs(
            parseResult.GetValue(runOption),
            parseResult.GetValue(runDirOption),
            parseResult.GetValue(hostOption)!,
            parseResult.GetValue(portOption),
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory()), ct));

        var command = new Command("staging", "Work with a run's staging database");
        command.Subcommands.Add(serve);
        return command;
    }

    internal static async Task<int> Serve(StagingServeArgs args, CancellationToken ct)
    {
        if (ValidateOptions(args) is { } invalid)
        {
            Console.Error.WriteLine($"error {invalid}");
            return ExitCodes.ConfigError;
        }

        var runDir = args.RunDir ?? Path.Combine(args.ProjectDir, ".pz", "runs", args.RunId!);
        StagingServer server;
        try
        {
            server = await StagingServer.StartAsync(runDir, args.Host, args.Port, ct);
        }
        catch (PzValidationException ex)
        {
            Console.Error.WriteLine($"error {ex.Errors[0]}");
            return ExitCodes.ConfigError;
        }

        await using (server)
        {
            if (!StagingServer.IsLoopback(args.Host))
            {
                Console.Error.WriteLine(
                    $"warning: listening on {args.Host}, which other machines can reach. The token and every value " +
                    "travel unencrypted; put a TLS reverse proxy in front, or serve on localhost.");
            }

            Console.Out.Write(Banner(args.RunId ?? Path.GetFileName(Path.TrimEndingDirectorySeparator(runDir)),
                server.Uri, server.Token, server.Tables));
            // The CLI leaves stop signals to each verb (see CliApp): Ctrl+C, SIGTERM or SIGHUP ends the
            // wait, so the server, the run directory's lock and the scratch view database are released.
            using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
            using var signals = StopSignals.Register(stop);
            try
            {
                await Task.Delay(Timeout.Infinite, stop.Token);
            }
            catch (OperationCanceledException)
            {
                // stopping is the only way out
            }
        }

        Console.Error.WriteLine("stopped serving.");
        return ExitCodes.Ok;
    }

    internal static string Banner(string run, string uri, string token, IReadOnlyList<string> tables)
    {
        var lines = new List<string>
        {
            $"Serving run {run}'s staging read-only at {uri} (Ctrl+C to stop).",
            "",
            "Attach it from a DuckDB 2.0 client:",
            "",
            "  INSTALL quack; LOAD quack;",
            $"  CREATE SECRET pz_staging (TYPE quack, TOKEN '{token}', SCOPE '{uri}');",
            $"  ATTACH '{uri}' AS run;",
        };
        if (tables.Count > 0)
        {
            lines.Add($"  SELECT * FROM run.{tables[0]} LIMIT 10;");
        }

        lines.Add("");
        lines.Add(tables.Count == 0
            ? "The run staged no tables."
            : $"Tables ({tables.Count.ToString(CultureInfo.InvariantCulture)}):");
        lines.AddRange(tables.Select(t => "  " + t));
        lines.Add("");
        lines.Add("Values are served as stored: `redact: true` marks apply only to `pz preview`.");
        return string.Join('\n', lines) + "\n";
    }

    /// <summary>The option rules; every break is PZ0544 naming the rule.</summary>
    internal static PzError? ValidateOptions(StagingServeArgs args)
    {
        if ((args.RunId is null) == (args.RunDir is null))
        {
            return Options("give exactly one of --run <id>, --run-dir <path>");
        }

        if (args.RunId is { } runId && (!PlainRunId().IsMatch(runId) || runId is "." or ".."))
        {
            return Options($"--run takes a run id, a plain directory name under .pz/runs (got '{runId}')");
        }

        if (args.Port is { } port && port is < 1 or > 65535)
        {
            return Options($"--port must be 1-65535 (got {port.ToString(CultureInfo.InvariantCulture)})");
        }

        if (!StagingServer.IsValidHost(args.Host))
        {
            return Options($"--host takes a host name or IP address (got '{args.Host}')");
        }

        return null;
    }

    private static PzError Options(string rule) =>
        new(PzErrorCode.StagingServeOptionsInvalid, $"pz staging serve: {rule}.", null, null,
            "see `pz staging serve --help`");
}
