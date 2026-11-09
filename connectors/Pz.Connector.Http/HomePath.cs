namespace Pz.Connector.Http;

/// <summary>Neither <see cref="Pz.Connectors.Toolkit.ProjectRelativePath"/> nor the credential and TLS
/// libraries expand '~', so a home-relative file option would never be found. Expand it before
/// resolving against the project directory. The result is a full path in the platform's own form
/// (separators normalised, as a project-relative path is), so messages and comparisons see one spelling.</summary>
internal static class HomePath
{
    public static string? Expand(string? path)
    {
        if (string.IsNullOrEmpty(path) || path[0] != '~'
            || (path.Length > 1 && path[1] != Path.DirectorySeparatorChar && path[1] != Path.AltDirectorySeparatorChar))
        {
            return path;
        }

        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        return home.Length == 0
            ? path
            : Path.GetFullPath(Path.Combine(home, path[1..].TrimStart(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)));
    }
}
