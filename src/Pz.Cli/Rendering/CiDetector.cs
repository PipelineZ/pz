namespace Pz.Cli.Rendering;

/// <summary>`text` output renders the live Spectre tree only when stdout is an interactive TTY AND the
/// process is not running under CI — otherwise (redirected stdout, or the `CI` env var set to anything)
/// it falls back to plain sequential lines, which is what every CLI/e2e assertion depends on.
/// The env/redirect checks are seams (delegates)
/// so tests can simulate each combination without touching the real environment or process streams.</summary>
public static class CiDetector
{
    /// <summary>True when the run should render interactively (Spectre live tree): stdout is not
    /// redirected/piped/captured AND no `CI` environment variable is set (any value, including empty,
    /// counts as CI — matching the common convention most CI providers use).</summary>
    public static bool IsInteractive(Func<string, string?> getEnvironmentVariable, Func<bool> isOutputRedirected) =>
        getEnvironmentVariable("CI") is null && !isOutputRedirected();

    public static bool IsInteractive() =>
        IsInteractive(Environment.GetEnvironmentVariable, () => Console.IsOutputRedirected);
}
