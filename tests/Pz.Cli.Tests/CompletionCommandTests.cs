namespace Pz.Cli.Tests;

/// <summary>`pz completion` end to end. Entirely offline and side-effect-free -- no project, no
/// filesystem writes, no network -- so these need no fixture at all.
///
/// Joins "console-and-env-serialized" (defined in RestoreCommandTests.cs) purely because it redirects
/// the process-global Console.Out/Error to assert on CLI output, and would otherwise race the other
/// classes that do.</summary>
[Collection("console-and-env-serialized")]
public sealed class CompletionCommandTests
{
    [Theory]
    [InlineData("bash"), InlineData("zsh"), InlineData("fish"), InlineData("pwsh")]
    public void Supported_shell_exits_ok(string shell)
    {
        var stdout = CaptureOut(() => CliApp.Build().Parse(["completion", shell]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.NotEmpty(stdout);
    }

    // A script that carries its own list of verbs is out of date the day a verb or an option is
    // added. Each one asks pz itself instead, so it completes whatever the installed pz accepts.
    [Theory]
    [InlineData("bash"), InlineData("zsh"), InlineData("fish"), InlineData("pwsh")]
    public void Every_script_asks_pz_for_its_suggestions(string shell)
    {
        var stdout = CaptureOut(() => CliApp.Build().Parse(["completion", shell]).Invoke(), out _);

        Assert.Contains("[suggest:", stdout);
        Assert.DoesNotContain("rollback", stdout);
    }

    // What the scripts call. It goes through CliApp.Run, the entrypoint that rewrites a usage error's
    // exit code, because that override must leave the directive alone.
    [Theory]
    [InlineData("pz ", "run")]
    [InlineData("pz ", "completion")]
    [InlineData("pz state ", "rollback")]
    [InlineData("pz connector ", "test")]
    [InlineData("pz run --", "--project")]
    [InlineData("pz runs --", "--limit")]
    [InlineData("pz completion ", "zsh")]
    public void The_suggest_directive_answers_with_verbs_sub_verbs_and_options(string line, string expected)
    {
        var position = line.Length.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var stdout = CaptureOut(
            () => CliApp.Run(CliApp.Build(), [$"[suggest:{position}]", line], new StringWriter(), _ => null),
            out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains(expected, stdout.Split('\n', StringSplitOptions.TrimEntries));
    }

    [Fact]
    public void Every_registered_verb_is_suggested()
    {
        var stdout = CaptureOut(
            () => CliApp.Run(CliApp.Build(), ["[suggest:3]", "pz "], new StringWriter(), _ => null), out _);
        var suggested = stdout.Split('\n', StringSplitOptions.TrimEntries);

        Assert.All(CliApp.Build().Subcommands.Where(c => !c.Hidden), verb => Assert.Contains(verb.Name, suggested));
    }

    // The script as a shell runs it: `pz` is a function over the CLI this test project was built with.
    [SkippableTheory]
    [InlineData("pz sta", "state")]
    [InlineData("pz state ro", "rollback")]
    [InlineData("pz run --proj", "--project")]
    public void The_bash_script_completes_the_word_under_the_cursor(string line, string expected)
    {
        Assert.Equal(expected, Assert.Single(BashComplete(line)));
    }

    // An option's value is usually a path. With nothing of pz's own to offer, the script has to leave
    // the reply empty and be registered so that bash then falls back to file names.
    [SkippableFact]
    public void The_bash_script_leaves_a_path_argument_to_the_shells_own_file_completion()
    {
        Assert.Empty(BashComplete("pz run --project ./"));

        var script = CaptureOut(() => CliApp.Build().Parse(["completion", "bash"]).Invoke(), out _);
        Assert.Contains("complete -o default -F", script);
    }

    private static string[] BashComplete(string line)
    {
        Skip.If(OperatingSystem.IsWindows(), "needs bash");
        var script = CaptureOut(() => CliApp.Build().Parse(["completion", "bash"]).Invoke(), out _);
        var cli = Path.Combine(AppContext.BaseDirectory, "Pz.Cli.dll");
        var words = line.Split(' ');
        var driver =
            $"pz() {{ dotnet '{cli}' \"$@\"; }}\n" + script +
            $"COMP_LINE='{line}'\nCOMP_POINT={line.Length}\n" +
            $"COMP_WORDS=({string.Join(' ', words.Select(w => $"'{w}'"))})\nCOMP_CWORD={words.Length - 1}\n" +
            "_pz_complete\nprintf '%s\\n' \"${COMPREPLY[@]}\"\n";

        var start = new System.Diagnostics.ProcessStartInfo("bash")
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        start.ArgumentList.Add("-s");
        System.Diagnostics.Process? process = null;
        try
        {
            process = System.Diagnostics.Process.Start(start);
        }
        catch (System.ComponentModel.Win32Exception)
        {
            // no bash on this machine
        }

        Skip.If(process is null, "needs bash");
        using (process)
        {
            process!.StandardInput.Write(driver);
            process.StandardInput.Close();
            var stdout = process.StandardOutput.ReadToEnd();
            var stderr = process.StandardError.ReadToEnd();
            process.WaitForExit();
            Assert.True(process.ExitCode == 0, stderr);
            return stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }
    }

    [Theory]
    [InlineData("bash"), InlineData("zsh"), InlineData("fish"), InlineData("pwsh")]
    public void Script_is_lf_only_with_a_single_trailing_newline(string shell)
    {
        var stdout = CaptureOut(() => CliApp.Build().Parse(["completion", shell]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.DoesNotContain('\r', stdout);
        Assert.EndsWith("\n", stdout);
        Assert.False(stdout.EndsWith("\n\n", StringComparison.Ordinal), "must have exactly one trailing newline");
    }

    [Fact]
    public void Unknown_shell_is_a_config_error()
    {
        var stderr = Capture(() => CliApp.Build().Parse(["completion", "cmd"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0535", stderr);
        Assert.Contains("cmd", stderr);
    }

    /// <summary>A missing required argument is a parse error -- only <see cref="CliApp.Run"/> (not a
    /// bare <c>ParseResult.Invoke()</c>) overrides System.CommandLine's own exit-1 default for that case
    /// (see CliAppTests' equivalent fact), so this goes through the same overload Program.cs actually
    /// runs.</summary>
    [Fact]
    public void Missing_shell_argument_is_a_config_error()
    {
        var exit = CliApp.Run(CliApp.Build(), ["completion"], new StringWriter(), _ => null);
        Assert.Equal(ExitCodes.ConfigError, exit);
    }

    private static string Capture(Func<int> action, out int exit)
    {
        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        try
        {
            exit = action();
        }
        finally
        {
            Console.SetError(original);
        }

        return stderr.ToString();
    }

    private static string CaptureOut(Func<int> action, out int exit)
    {
        var stdout = new StringWriter();
        var original = Console.Out;
        Console.SetOut(stdout);
        try
        {
            exit = action();
        }
        finally
        {
            Console.SetOut(original);
        }

        return stdout.ToString();
    }
}
