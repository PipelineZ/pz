using System.CommandLine;
using Pz.Core.Validation;

namespace Pz.Cli.Commands;

/// <summary>`pz completion bash|zsh|fish|pwsh`: prints a shell completion script to stdout. A script
/// carries no list of its own — it hands the command line and the cursor position back to pz
/// (<c>pz "[suggest:&lt;position&gt;]" "&lt;line&gt;"</c>, answered by the command-line parser itself), so
/// verbs, sub-verbs and options complete from whatever the installed pz accepts and an upgrade never
/// leaves a stale script behind. Where pz has nothing to offer — an option's value is usually a path —
/// each script falls back to the shell's own file completion. No network, no file writes, byte-stable
/// output (LF line endings, final newline).</summary>
internal static class CompletionCommand
{
    private static readonly string[] SupportedShells = ["bash", "zsh", "fish", "pwsh"];

    public static Command Create()
    {
        var shellArgument = new Argument<string>("shell")
        {
            Description = "Shell to generate a completion script for: bash, zsh, fish, or pwsh",
        };
        shellArgument.CompletionSources.Add(SupportedShells);
        var command = new Command("completion", "Print a shell completion script for pz to stdout");
        command.Arguments.Add(shellArgument);
        command.SetAction(parseResult => Execute(parseResult.GetValue(shellArgument)!));
        return command;
    }

    internal static int Execute(string shell)
    {
        var script = shell switch
        {
            "bash" => Bash,
            "zsh" => Zsh,
            "fish" => Fish,
            "pwsh" => Pwsh,
            _ => null,
        };
        if (script is null)
        {
            Console.Error.WriteLine(
                $"error {PzErrorCode.CompletionShellInvalid}: unknown shell '{shell}' — expected one of: " +
                $"{string.Join(", ", SupportedShells)}");
            return ExitCodes.ConfigError;
        }

        Console.Out.Write(script.ReplaceLineEndings("\n"));
        return ExitCodes.Ok;
    }

    // `-o default` is what hands an empty reply on to bash's file name completion.
    private const string Bash = """
        _pz_complete() {
            local IFS=$'\n'
            local suggestions
            suggestions=$(pz "[suggest:${COMP_POINT}]" "${COMP_LINE}" 2>/dev/null)
            COMPREPLY=( $(compgen -W "${suggestions}" -- "${COMP_WORDS[COMP_CWORD]}") )
        }
        complete -o default -F _pz_complete pz

        """;

    // Works both ways a zsh user installs it: autoloaded from fpath (the function is then called for a
    // completion straight away) and sourced from .zshrc (it only registers itself).
    private const string Zsh = """
        #compdef pz

        _pz() {
            local -a suggestions
            suggestions=(${(f)"$(pz "[suggest:${CURSOR}]" "${BUFFER}" 2>/dev/null)"})
            compadd -- ${suggestions} || _files
        }

        if [ "${funcstack[1]}" = "_pz" ]; then
            _pz "$@"
        else
            compdef _pz pz
        fi

        """;

    // File names stay out of the way only where a verb is expected; everywhere else fish offers them
    // beside pz's own suggestions.
    private const string Fish = """
        complete -c pz -n "__fish_use_subcommand" -f
        complete -c pz -a '(pz "[suggest:"(string length -- (commandline -cp))"]" (commandline -cp) 2>/dev/null)'

        """;

    // The AST's text drops the trailing space of `pz state `, which is the difference between
    // completing `state` and completing what follows it, so the line is padded back out to the cursor.
    private const string Pwsh = """
        Register-ArgumentCompleter -Native -CommandName pz -ScriptBlock {
            param($wordToComplete, $commandAst, $cursorPosition)
            $line = "$commandAst".PadRight($cursorPosition)
            pz "[suggest:$cursorPosition]" $line 2>$null |
                Where-Object { $_ -like "$wordToComplete*" } |
                ForEach-Object { [System.Management.Automation.CompletionResult]::new($_, $_, 'ParameterValue', $_) }
        }

        """;
}
