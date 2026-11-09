using System.Reflection;
using System.Text.RegularExpressions;

namespace Pz.TestSupport;

/// <summary>
/// Testcontainers parses every image reference with one shared compiled regex that carries a fixed 5 s match
/// timeout. That includes the resource reaper's image, which is parsed inside the static initializer behind
/// <c>TestcontainersSettings</c>. The timeout is wall-clock time and includes JIT-compiling the regex on its first
/// match, so a starved CPU (a full parallel test run) can exceed it. When that happens inside a type initializer,
/// Testcontainers stays broken for the rest of the test process and the whole suite fails.
/// <para>
/// No Testcontainers setting, environment variable or AppContext switch reaches an explicit timeout, and the parse
/// cannot be avoided: the reaper type parses its default image in its own static initializer, and every builder
/// that takes an image name parses that name with the same regex. So this gives the one shared
/// instance an infinite timeout through the protected <see cref="Regex"/> field that holds it. The image names
/// parsed are short and fixed, so the timeout guards nothing here.
/// </para>
/// </summary>
public static class TestcontainersRegex
{
    private const string AnchorType = "DotNet.Testcontainers.Images.DockerImage, Testcontainers";
    private const string RegexType = "DotNet.Testcontainers.Images.MatchImage+ReferenceRegex, Testcontainers";

    /// <summary>Lifts the timeout. Must run before anything in the process parses an image reference: call it from
    /// a <c>[ModuleInitializer]</c> in every test assembly that references Testcontainers. It does nothing where
    /// Testcontainers is not loadable, and throws when Testcontainers no longer has the shape it relies on, so an
    /// upgrade that changes it fails every docker suite loudly instead of bringing the flake back.</summary>
    public static void LiftMatchTimeout()
    {
        if (Type.GetType(AnchorType) is null)
        {
            return;
        }

        var instance = Type.GetType(RegexType)
            ?.GetProperty("Instance", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
            ?.GetValue(null) as Regex
            ?? throw new InvalidOperationException(
                $"Testcontainers no longer has '{RegexType}' with a static Regex 'Instance'; " +
                "re-check how it parses image references and update TestcontainersRegex.");

        var timeout = typeof(Regex).GetField("internalMatchTimeout", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("System.Text.RegularExpressions.Regex has no 'internalMatchTimeout' field.");
        timeout.SetValue(instance, Regex.InfiniteMatchTimeout);
    }

    /// <summary>The match timeout of Testcontainers' image-reference regex; null where Testcontainers is not
    /// loadable.</summary>
    public static TimeSpan? CurrentMatchTimeout() =>
        Type.GetType(AnchorType) is null
            ? null
            : (Type.GetType(RegexType)
                ?.GetProperty("Instance", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
                ?.GetValue(null) as Regex)?.MatchTimeout;
}
