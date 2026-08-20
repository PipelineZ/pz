using System.CommandLine;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Pz.Core.Validation;

namespace Pz.Cli.Commands;

/// <summary>`pz init &lt;name&gt;`: scaffolds a runnable starter project from the embedded
/// <c>Templates/init/**</c> resources (the single source of truth; there is no copy-from-samples build
/// step, so an installed tool works with no source tree on disk). The
/// scaffolded project uses only <c>Pz.Connector.LocalFiles</c> (a builtin), so `cd &lt;name&gt; &amp;&amp;
/// pz run` succeeds fully offline — no `pz restore`, no network, no docker.</summary>
internal static class InitCommand
{
    private const string ResourcePrefix = "Templates/init/";
    private const string ProjectNameToken = "{{PROJECT_NAME}}";

    public static Command Create()
    {
        var nameArgument = new Argument<string>("name")
        {
            Description = "Directory to scaffold the new project into ('.' scaffolds into the current directory)",
        };
        var command = new Command("init", "Scaffold a new runnable pz project from the built-in starter template");
        command.Arguments.Add(nameArgument);
        command.SetAction(parseResult => Execute(parseResult.GetValue(nameArgument)!, Directory.GetCurrentDirectory()));
        return command;
    }

    internal static int Execute(string name, string workingDir)
    {
        var targetDir = Path.GetFullPath(name, workingDir);

        if (File.Exists(targetDir))
        {
            var error = new PzError(PzErrorCode.InitTargetNotEmpty,
                $"target '{targetDir}' already exists and is a file, not a directory.", null, null,
                "run `pz init` against a new or empty directory, or remove the existing file first");
            Console.Error.WriteLine($"error {error}");
            return ExitCodes.ConfigError;
        }

        if (Directory.Exists(targetDir) && Directory.EnumerateFileSystemEntries(targetDir).Any())
        {
            var error = new PzError(PzErrorCode.InitTargetNotEmpty,
                $"target directory '{targetDir}' already exists and is not empty.", null, null,
                "run `pz init` against a new or empty directory, or clear this one first");
            Console.Error.WriteLine($"error {error}");
            return ExitCodes.ConfigError;
        }

        var rawLeaf = Path.GetFileName(targetDir.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        var projectName = SanitizeProjectName(rawLeaf);
        if (!string.Equals(projectName, rawLeaf, StringComparison.Ordinal))
        {
            Console.Error.WriteLine(
                $"warning: '{rawLeaf}' is not a valid pz project name (lowercase [a-z0-9_], leading letter); " +
                $"using '{projectName}' in project.yml instead");
        }

        Directory.CreateDirectory(targetDir);

        var assembly = Assembly.GetExecutingAssembly();
        var utf8NoBom = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);
        foreach (var resourceName in assembly.GetManifestResourceNames())
        {
            var normalized = resourceName.Replace('\\', '/');
            if (!normalized.StartsWith(ResourcePrefix, StringComparison.Ordinal))
            {
                continue;
            }

            var relativeSegments = normalized[ResourcePrefix.Length..].Split('/', StringSplitOptions.RemoveEmptyEntries);
            var destination = Path.Combine([targetDir, .. relativeSegments]);
            Directory.CreateDirectory(Path.GetDirectoryName(destination)!);

            using var stream = assembly.GetManifestResourceStream(resourceName)
                ?? throw new InvalidOperationException($"embedded resource '{resourceName}' declared but unreadable");
            using var reader = new StreamReader(stream, Encoding.UTF8);
            var content = reader.ReadToEnd().Replace(ProjectNameToken, projectName, StringComparison.Ordinal);
            File.WriteAllText(destination, content, utf8NoBom);
        }

        Console.WriteLine($"scaffolded a new pz project '{projectName}' at {targetDir}");
        // Echo the SANITIZED project name (not the raw `name` argument) so the hint is always consistent
        // with what project.yml's `name:` actually says -- this matches the common case exactly (bare,
        // already-valid names round-trip unchanged) and stays copy-paste-safe even when the raw argument
        // needed sanitizing (e.g. shell-hazardous characters like `!`).
        Console.WriteLine(
            "next steps:\n" +
            $"  cd {projectName} && pz run orders_enriched\n" +
            "  (this template ships two independent flows; `pz run --all` runs both)");
        return ExitCodes.Ok;
    }

    /// <summary>Lowercase, `[a-z0-9_]`, leading letter. Any other character
    /// (including `-`, spaces, punctuation) becomes `_`; runs of `_` collapse to one and leading/trailing
    /// `_` are trimmed, so `"My-Proj!"` -&gt; `"my_proj"`. A result starting with a digit (or empty, e.g.
    /// an all-punctuation input) gets a `p_` prefix so `name:` in project.yml always parses as an
    /// identifier-shaped value.</summary>
    internal static string SanitizeProjectName(string raw)
    {
        var lowered = raw.ToLowerInvariant();
        var replaced = new StringBuilder(lowered.Length);
        foreach (var ch in lowered)
        {
            replaced.Append(ch is (>= 'a' and <= 'z') or (>= '0' and <= '9') ? ch : '_');
        }

        var collapsed = Regex.Replace(replaced.ToString(), "_+", "_").Trim('_');
        return collapsed.Length == 0 || collapsed[0] is < 'a' or > 'z' ? "p_" + collapsed : collapsed;
    }
}
