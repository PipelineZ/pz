using System.CommandLine;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Pz.Core.Validation;

namespace Pz.Cli.Commands;

/// <summary>`pz init &lt;name&gt;`: scaffolds a new project from the embedded <c>Templates/**</c>
/// resources (the single source of truth; there is no copy-from-samples build step, so an installed
/// tool works with no source tree on disk).
///
/// The default is <see cref="Template.Minimal"/> — project.yml + connections.yml, nothing to delete
/// before authoring. <c>--sample</c> scaffolds the runnable four-pipeline demo instead; it uses only
/// <c>Pz.Connector.LocalFiles</c> (a builtin), so `cd &lt;name&gt; &amp;&amp; pz run --all` succeeds
/// fully offline — no `pz restore`, no network, no docker.</summary>
internal static class InitCommand
{
    /// <summary>Which embedded template set to scaffold. <see cref="Minimal"/> — project.yml +
    /// connections.yml and nothing else — is the default everywhere: it is what someone starting
    /// their own project wants, and the sample is a poor substitute for it because the demo files
    /// COMPILE, so until they are deleted `pz run --all` moves demo data nobody asked for.
    /// <see cref="Sample"/> is the runnable four-pipeline demo, for learning the shape from working
    /// code; it is opt-in via <c>--sample</c> (or <c>pz_init_project</c>'s <c>minimal: false</c>).
    ///
    /// <see cref="Execute"/> deliberately takes this with no default value: which project a caller
    /// scaffolds is never an incidental choice, and a default here is how the CLI and the MCP tool
    /// would silently drift apart.</summary>
    internal enum Template
    {
        Sample,
        Minimal,
    }

    private const string ProjectNameToken = "{{PROJECT_NAME}}";

    private static string ResourcePrefixFor(Template template) =>
        template == Template.Minimal ? "Templates/minimal/" : "Templates/init/";

    public static Command Create()
    {
        var nameArgument = new Argument<string>("name")
        {
            Description = "Directory to scaffold the new project into ('.' scaffolds into the current directory)",
        };
        var sampleOption = new Option<bool>("--sample")
        {
            Description = "Scaffold the runnable four-pipeline sample project instead of a minimal one",
        };
        var command = new Command("init", "Scaffold a new pz project (project.yml + connections.yml; --sample for a runnable demo)");
        command.Arguments.Add(nameArgument);
        command.Options.Add(sampleOption);
        command.SetAction(parseResult => Execute(
            parseResult.GetValue(nameArgument)!,
            Directory.GetCurrentDirectory(),
            parseResult.GetValue(sampleOption) ? Template.Sample : Template.Minimal));
        return command;
    }

    internal static int Execute(string name, string workingDir, Template template)
    {
        var resourcePrefix = ResourcePrefixFor(template);
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
            if (!normalized.StartsWith(resourcePrefix, StringComparison.Ordinal))
            {
                continue;
            }

            var relativeSegments = normalized[resourcePrefix.Length..].Split('/', StringSplitOptions.RemoveEmptyEntries);
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
        Console.WriteLine(template == Template.Minimal
            ? "next steps:\n" +
              $"  cd {projectName}, declare a connection in connections.yml, then add a pipeline\n" +
              "  under pipelines/ that source()s from it -- `pz validate` checks both\n" +
              "  (want a worked example instead? `pz init <name> --sample`)"
            : "next steps:\n" +
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
