using System.Text;
using System.Text.RegularExpressions;

namespace Pz.Shared.Paths;

/// <summary>Translates an object-path glob to a full-match regex: <c>*</c> matches any run of
/// non-<c>/</c> characters, <c>**</c> any run including <c>/</c>, and <c>?</c> one non-<c>/</c>
/// character. Everything else is escaped, so a literal name (one containing <c>+</c>, say) never acts
/// as a regex metacharacter.
///
/// <para>Compiled into the azureblob and sftp connectors from this one source file: connectors never
/// reference each other.</para></summary>
internal static class PathGlob
{
    internal static string ToRegexPattern(string pattern)
    {
        var sb = new StringBuilder("^");
        for (var i = 0; i < pattern.Length; i++)
        {
            var c = pattern[i];
            if (c == '*')
            {
                if (i + 1 < pattern.Length && pattern[i + 1] == '*')
                {
                    sb.Append(".*");
                    i++;
                }
                else
                {
                    sb.Append("[^/]*");
                }
            }
            else if (c == '?')
            {
                sb.Append("[^/]");
            }
            else
            {
                sb.Append(Regex.Escape(c.ToString()));
            }
        }

        sb.Append('$');
        return sb.ToString();
    }
}
