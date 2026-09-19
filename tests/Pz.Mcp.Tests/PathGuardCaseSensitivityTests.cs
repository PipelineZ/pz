using Pz.Mcp.Handlers;

namespace Pz.Mcp.Tests;

/// <summary>Proves both branches of <see cref="PathGuard"/>'s containment comparison directly, via
/// <see cref="PathGuard.EscapesForTests"/> -- independent of which OS actually runs this suite, since
/// production picks the comparison from <see cref="OperatingSystem.IsWindows"/>/<see
/// cref="OperatingSystem.IsMacOS"/> but the check itself is pure text comparison with no disk I/O
/// (<see cref="Path.GetFullPath(string)"/> never touches the filesystem or canonicalizes case), so an
/// explicit <see cref="StringComparison"/> exercises exactly what each platform would compute without
/// needing to actually run on it.</summary>
public class PathGuardCaseSensitivityTests
{
    // A value that resolves to the project root itself but with different casing than the projectDir
    // string carries -- this can legitimately happen (a symlink, an inconsistently-cased projectDir
    // argument, or simply an agent typing the path differently) and never touches the filesystem to
    // canonicalize, since Path.GetFullPath is purely lexical.
    [Fact]
    public void OrdinalIgnoreCase_accepts_a_path_that_differs_only_in_casing_from_the_project_root()
    {
        var root = Path.Combine(Path.GetTempPath(), "PzCaseTest", "MyProject");

        // "myproject" vs "MyProject": same directory on a case-insensitive filesystem, which is what
        // OrdinalIgnoreCase models here.
        var differentlyCasedRoot = Path.Combine(Path.GetTempPath(), "PzCaseTest", "myproject");
        var value = Path.GetRelativePath(differentlyCasedRoot, Path.Combine(root, "data", "orders.csv"));

        Assert.False(PathGuard.EscapesForTests(differentlyCasedRoot, value, StringComparison.OrdinalIgnoreCase));
    }

    // Same inputs, but Ordinal (the case-sensitive-filesystem behavior): the casing difference makes
    // the resolved path fail the prefix check, so it is (wrongly, on a real case-insensitive
    // filesystem) flagged as escaping -- this is the bug the fix's platform switch avoids on
    // Windows/macOS, proven here so a future regression that always uses Ordinal is caught.
    [Fact]
    public void Ordinal_flags_the_same_casing_only_difference_as_escaping()
    {
        var root = Path.Combine(Path.GetTempPath(), "PzCaseTest", "MyProject");
        var differentlyCasedRoot = Path.Combine(Path.GetTempPath(), "PzCaseTest", "myproject");
        var value = Path.GetRelativePath(differentlyCasedRoot, Path.Combine(root, "data", "orders.csv"));

        Assert.True(PathGuard.EscapesForTests(differentlyCasedRoot, value, StringComparison.Ordinal));
    }

    // The security case the coordinator flagged: a genuine sibling directory whose name differs only in
    // case (`Project2` vs `project2`) must stay refused under Ordinal -- these really are two different
    // directories on a case-sensitive filesystem, and going case-insensitive there would let a
    // `path: ../project2/secret` escape into the sibling undetected.
    [Fact]
    public void Ordinal_still_refuses_escaping_into_a_differently_cased_sibling_directory()
    {
        var root = Path.Combine(Path.GetTempPath(), "PzCaseTest", "Project2");
        var value = Path.Combine("..", "project2", "secret.csv");

        Assert.True(PathGuard.EscapesForTests(root, value, StringComparison.Ordinal));
    }

    // A genuine escape -- an entirely different directory, not merely a casing difference -- must be
    // refused under EITHER comparison: the fix narrows a false positive, it must never widen what is
    // accepted as "inside the project".
    [Fact]
    public void A_genuine_escape_is_refused_under_both_comparisons()
    {
        var root = Path.Combine(Path.GetTempPath(), "PzCaseTest", "Project");
        var value = Path.Combine("..", "..", "etc", "hostname");

        Assert.True(PathGuard.EscapesForTests(root, value, StringComparison.Ordinal));
        Assert.True(PathGuard.EscapesForTests(root, value, StringComparison.OrdinalIgnoreCase));
    }

    // A path genuinely inside the project, same casing throughout, must stay accepted under either
    // comparison -- the common case is unaffected by the fix.
    [Fact]
    public void A_path_inside_the_project_is_accepted_under_both_comparisons()
    {
        var root = Path.Combine(Path.GetTempPath(), "PzCaseTest", "Project");
        var value = Path.Combine("data", "orders.csv");

        Assert.False(PathGuard.EscapesForTests(root, value, StringComparison.Ordinal));
        Assert.False(PathGuard.EscapesForTests(root, value, StringComparison.OrdinalIgnoreCase));
    }
}
