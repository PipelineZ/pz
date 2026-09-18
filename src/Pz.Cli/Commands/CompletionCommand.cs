using System.CommandLine;
using System.Text;
using Pz.Core.Validation;

namespace Pz.Cli.Commands;

/// <summary>`pz completion bash|zsh|fish|pwsh`: prints a shell completion script for pz's verbs
/// (and one level of sub-verbs — `connector test`, `cdc status`/`drop`, etc.) to stdout. Generated from
/// <see cref="CommandTree"/>'s walk of the real <see cref="CliApp.Build"/> tree, never a hand-maintained
/// list, so a verb cannot drift out of sync with `--help`. Static only: no dynamic completion of node,
/// connection, or entity names (that needs a live project and is out of scope here) — no network, no
/// file writes, byte-stable output (LF line endings, final newline).</summary>
internal static class CompletionCommand
{
    private static readonly string[] SupportedShells = ["bash", "zsh", "fish", "pwsh"];

    public static Command Create()
    {
        var shellArgument = new Argument<string>("shell")
        {
            Description = "Shell to generate a completion script for: bash, zsh, fish, or pwsh",
        };
        var command = new Command("completion", "Print a shell completion script for pz's verbs to stdout");
        command.Arguments.Add(shellArgument);
        command.SetAction(parseResult => Execute(parseResult.GetValue(shellArgument)!));
        return command;
    }

    internal static int Execute(string shell)
    {
        if (Array.IndexOf(SupportedShells, shell) < 0)
        {
            Console.Error.WriteLine(
                $"error {PzErrorCode.CompletionShellInvalid}: unknown shell '{shell}' — expected one of: " +
                $"{string.Join(", ", SupportedShells)}");
            return ExitCodes.ConfigError;
        }

        var groups = CommandTree.Collect(CliApp.Build());
        var script = shell switch
        {
            "bash" => BashScript(groups),
            "zsh" => ZshScript(groups),
            "fish" => FishScript(groups),
            "pwsh" => PwshScript(groups),
            _ => throw new ArgumentOutOfRangeException(nameof(shell), shell, "unknown shell"),
        };

        Console.Out.Write(script);
        return ExitCodes.Ok;
    }

    private static string BashScript(IReadOnlyList<CommandTree.VerbGroup> groups)
    {
        var sb = new StringBuilder();
        sb.Append("_pz_complete() {\n");
        sb.Append("    local cur=${COMP_WORDS[COMP_CWORD]}\n");
        sb.Append($"    local commands=\"{string.Join(' ', groups.Select(g => g.Name))}\"\n");
        sb.Append("    if [[ $COMP_CWORD -eq 1 ]]; then\n");
        sb.Append("        COMPREPLY=( $(compgen -W \"$commands\" -- \"$cur\") )\n");
        sb.Append("        return 0\n");
        sb.Append("    fi\n");
        sb.Append("    case \"${COMP_WORDS[1]}\" in\n");
        foreach (var g in groups.Where(g => g.Children.Count > 0))
        {
            sb.Append($"        {g.Name}) COMPREPLY=( $(compgen -W \"{string.Join(' ', g.Children)}\" -- \"$cur\") ) ;;\n");
        }

        sb.Append("    esac\n");
        sb.Append("}\n");
        sb.Append("complete -F _pz_complete pz\n");
        return sb.ToString();
    }

    private static string ZshScript(IReadOnlyList<CommandTree.VerbGroup> groups)
    {
        var sb = new StringBuilder();
        sb.Append("#compdef pz\n\n");
        sb.Append("_pz() {\n");
        sb.Append("    local -a commands\n");
        sb.Append($"    commands=({string.Join(' ', groups.Select(g => g.Name))})\n\n");
        sb.Append("    if (( CURRENT == 2 )); then\n");
        sb.Append("        compadd -a commands\n");
        sb.Append("        return\n");
        sb.Append("    fi\n\n");
        sb.Append("    case \"${words[2]}\" in\n");
        foreach (var g in groups.Where(g => g.Children.Count > 0))
        {
            sb.Append($"        {g.Name}) compadd {string.Join(' ', g.Children)} ;;\n");
        }

        sb.Append("    esac\n");
        sb.Append("}\n\n");
        sb.Append("_pz \"$@\"\n");
        return sb.ToString();
    }

    private static string FishScript(IReadOnlyList<CommandTree.VerbGroup> groups)
    {
        var sb = new StringBuilder();
        sb.Append($"set -l pz_commands {string.Join(' ', groups.Select(g => g.Name))}\n");
        sb.Append("complete -c pz -f -n \"not __fish_seen_subcommand_from $pz_commands\" -a \"$pz_commands\"\n");
        foreach (var g in groups.Where(g => g.Children.Count > 0))
        {
            sb.Append(
                $"complete -c pz -f -n \"__fish_seen_subcommand_from {g.Name}\" -a \"{string.Join(' ', g.Children)}\"\n");
        }

        return sb.ToString();
    }

    private static string PwshScript(IReadOnlyList<CommandTree.VerbGroup> groups)
    {
        var sb = new StringBuilder();
        sb.Append("Register-ArgumentCompleter -Native -CommandName pz -ScriptBlock {\n");
        sb.Append("    param($wordToComplete, $commandAst, $cursorPosition)\n");
        sb.Append($"    $commands = {string.Join(',', groups.Select(g => $"'{g.Name}'"))}\n");
        sb.Append("    $tokens = $commandAst.CommandElements | ForEach-Object { $_.Extent.Text }\n");
        sb.Append("    if ($tokens.Count -le 2) {\n");
        sb.Append("        $commands | Where-Object { $_ -like \"$wordToComplete*\" } | " +
            "ForEach-Object { [System.Management.Automation.CompletionResult]::new($_, $_, 'ParameterValue', $_) }\n");
        sb.Append("        return\n");
        sb.Append("    }\n");
        sb.Append("    $sub = $tokens[1]\n");
        sb.Append("    $subcommands = @{\n");
        foreach (var g in groups.Where(g => g.Children.Count > 0))
        {
            sb.Append($"        '{g.Name}' = @({string.Join(',', g.Children.Select(c => $"'{c}'"))})\n");
        }

        sb.Append("    }\n");
        sb.Append("    if ($subcommands.ContainsKey($sub)) {\n");
        sb.Append("        $subcommands[$sub] | Where-Object { $_ -like \"$wordToComplete*\" } | " +
            "ForEach-Object { [System.Management.Automation.CompletionResult]::new($_, $_, 'ParameterValue', $_) }\n");
        sb.Append("    }\n");
        sb.Append("}\n");
        return sb.ToString();
    }
}
