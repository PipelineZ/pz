namespace Pz.Connectors.Toolkit;

/// <summary>Resolves a connection option that names a local file (a credential file, a private key --
/// anything the connector itself reads off disk rather than handing to a remote system) against the
/// CLI-injected <c>base_dir</c> connection option -- the same anchor <c>localfiles</c>' <c>root</c> and
/// <c>sqlite</c>'s <c>path</c> already resolve against
/// (<see cref="Pz.Core.Loading.ProjectDirectoryAnchor"/>). A relative value joins the project directory
/// instead of wherever <c>pz</c> happened to be invoked from; an absolute path, a <c>~</c>-prefixed
/// home-directory shorthand (a shell/library expansion this resolver must not pre-empt), or a URL-shaped
/// value (a scheme other than a single-letter Windows drive) passes through exactly as written.</summary>
public static class ProjectRelativePath
{
    /// <summary>Null/empty <paramref name="value"/> passes through unchanged. <paramref name="baseDir"/>
    /// missing (the connector opened directly, outside the CLI's anchoring) falls back to the process
    /// working directory -- the same fallback <c>localfiles</c>/<c>sqlite</c> use.</summary>
    public static string? Resolve(string? value, string? baseDir)
    {
        if (string.IsNullOrEmpty(value) || IsAnchorExempt(value))
        {
            return value;
        }

        return Path.GetFullPath(Path.Combine(baseDir ?? Directory.GetCurrentDirectory(), value));
    }

    private static bool IsAnchorExempt(string value) =>
        Path.IsPathRooted(value) ||
        value.StartsWith('~') ||
        (Uri.TryCreate(value, UriKind.Absolute, out var uri) && uri.Scheme.Length > 1);
}
