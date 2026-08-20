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
/// Which template to scaffold is a catalog lookup (<see cref="TemplateCatalog"/>), never an
/// incidental choice: the default is <see cref="TemplateCatalog.DefaultId"/>, and any other built-in
/// is opt-in via <c>--template &lt;id&gt;</c>. `pz init --list-templates` browses the catalog without
/// scaffolding anything.</summary>
internal static class InitCommand
{
    /// <summary>The placeholder every template's `project.yml` carries as its `name:`, replaced with
    /// the caller's sanitized project name at scaffold time. Identifier-shaped rather than a
    /// moustache so a template directory is itself a loadable project -- which is what lets the test
    /// suite compile the real scaffold source instead of a copy of it.</summary>
    private const string ProjectNameToken = "pz_new_project";

    public static Command Create()
    {
        var nameArgument = new Argument<string?>("name")
        {
            Description = "Directory to scaffold the new project into ('.' scaffolds into the current directory)",
            Arity = ArgumentArity.ZeroOrOne,
        };
        var templateOption = new Option<string>("--template", "-t")
        {
            Description = $"Which built-in template to scaffold (default: {TemplateCatalog.DefaultId}). "
                + "`pz init --list-templates` shows them all",
            DefaultValueFactory = _ => TemplateCatalog.DefaultId,
        };
        var listOption = new Option<bool>("--list-templates")
        {
            Description = "List every built-in template and exit",
        };
        var command = new Command("init", "Scaffold a new pz project from a built-in template");
        command.Arguments.Add(nameArgument);
        command.Options.Add(templateOption);
        command.Options.Add(listOption);
        command.SetAction(parseResult => Execute(
            parseResult.GetValue(nameArgument),
            Directory.GetCurrentDirectory(),
            parseResult.GetValue(templateOption)!,
            parseResult.GetValue(listOption)));
        return command;
    }

    internal static int Execute(string? name, string workingDir, string templateId, bool listTemplates = false)
    {
        if (listTemplates && name is not null)
        {
            return Fail(PzErrorCode.InitInvocationInvalid,
                "`--list-templates` lists the catalog and scaffolds nothing, but a project name was also given.",
                "run `pz init --list-templates` to browse, or `pz init <name> --template <id>` to scaffold");
        }

        if (listTemplates)
        {
            ListTemplates();
            return ExitCodes.Ok;
        }

        if (name is null)
        {
            return Fail(PzErrorCode.InitInvocationInvalid,
                "no project name given.",
                "run `pz init <name>` to scaffold, or `pz init --list-templates` to see what is available");
        }

        var template = TemplateCatalog.Find(templateId);
        if (template is null)
        {
            var known = string.Join(", ", TemplateCatalog.All.Select(t => t.Id));
            return Fail(PzErrorCode.InitTemplateUnknown,
                $"no built-in template named '{templateId}'.",
                $"pick one of: {known} (see `pz init --list-templates`)");
        }

        var resourcePrefix = $"Templates/{template.Id}/";
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
        var copiedCount = 0;
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
            copiedCount++;
        }

        // The id passed TemplateCatalog.Find above, so the catalog and the embedded resources
        // disagree -- a build/packaging defect, not a user error. Reports rather than claiming
        // success over an empty directory (no silent failures).
        if (copiedCount == 0)
        {
            return Fail(PzErrorCode.InitTemplateUnknown,
                $"template '{template.Id}' is in the catalog, but no resources under '{resourcePrefix}' " +
                "are embedded in this build.",
                "report this as a pz packaging bug");
        }

        Console.WriteLine($"scaffolded a new pz project '{projectName}' at {targetDir}");
        // Echo the SANITIZED project name (not the raw `name` argument) so the hint is always consistent
        // with what project.yml's `name:` actually says -- this matches the common case exactly (bare,
        // already-valid names round-trip unchanged) and stays copy-paste-safe even when the raw argument
        // needed sanitizing (e.g. shell-hazardous characters like `!`).
        Console.WriteLine("next steps:\n" + string.Format(
            System.Globalization.CultureInfo.InvariantCulture, template.NextSteps, projectName));
        return ExitCodes.Ok;
    }

    private static int Fail(string code, string message, string nextStep)
    {
        Console.Error.WriteLine($"error {new PzError(code, message, null, null, nextStep)}");
        return ExitCodes.ConfigError;
    }

    private static void ListTemplates()
    {
        Console.WriteLine("built-in templates (`pz init <name> --template <id>`):");
        foreach (var template in TemplateCatalog.All)
        {
            var marker = template.Runnability switch
            {
                TemplateRunnability.Offline => "runs offline",
                TemplateRunnability.NeedsNetwork => "needs network",
                TemplateRunnability.NeedsDatabase => "needs a database",
                _ => "nothing to run yet",
            };
            var star = string.Equals(template.Id, TemplateCatalog.DefaultId, StringComparison.Ordinal)
                ? " (default)"
                : string.Empty;
            Console.WriteLine($"  {template.Id}{star}");
            Console.WriteLine($"      {template.Summary} [{marker}]");
        }
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
