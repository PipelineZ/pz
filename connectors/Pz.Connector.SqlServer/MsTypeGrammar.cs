namespace Pz.Connector.SqlServer;

/// <summary>The whitelist grammar for declared `columns:` types.
/// A user string is parsed here and RE-RENDERED canonically -- the raw input never reaches DDL, which
/// is what keeps the option injection-safe. Not SQL parsing -- the ban on hand-rolled SQL parsing
/// governs deriving meaning from a pipeline's query; this is option-value validation, the same
/// family as ParseTablock/ParsePartitionCount.</summary>
internal static class MsTypeGrammar
{
    private const string Accepted =
        "accepted types: int, bigint, float, bit, date, datetime2(0..7), decimal(p,s) with " +
        "1<=p<=38 and 0<=s<=p, nvarchar(1..4000|max), varchar(1..8000|max)";

    public static bool TryParse(string input, out string canonical, out string? error)
    {
        canonical = "";
        error = null;
        var text = input.Trim().ToLowerInvariant();
        var paren = text.IndexOf('(');
        if (paren < 0)
        {
            if (text is "int" or "bigint" or "float" or "bit" or "date")
            {
                canonical = text;
                return true;
            }

            error = $"'{input}' is not a recognized type ({Accepted})";
            return false;
        }

        var name = text[..paren].TrimEnd();
        if (!text.EndsWith(')') || text.IndexOf(')') != text.Length - 1)
        {
            error = $"'{input}' is malformed ({Accepted})";
            return false;
        }

        var args = text[(paren + 1)..^1].Split(',');
        switch (name)
        {
            case "datetime2" when args.Length == 1 && TryInt(args[0], out var p) && p is >= 0 and <= 7:
                canonical = $"datetime2({p})";
                return true;
            case "decimal" when args.Length == 2 && TryInt(args[0], out var dp) && TryInt(args[1], out var ds)
                && dp is >= 1 and <= 38 && ds >= 0 && ds <= dp:
                canonical = $"decimal({dp},{ds})";
                return true;
            case "nvarchar" when args.Length == 1:
                if (args[0].Trim() == "max") { canonical = "nvarchar(max)"; return true; }
                if (TryInt(args[0], out var nn) && nn is >= 1 and <= 4000) { canonical = $"nvarchar({nn})"; return true; }
                break;
            case "varchar" when args.Length == 1:
                if (args[0].Trim() == "max") { canonical = "varchar(max)"; return true; }
                if (TryInt(args[0], out var vn) && vn is >= 1 and <= 8000) { canonical = $"varchar({vn})"; return true; }
                break;
        }

        error = $"'{input}' is not a recognized type ({Accepted})";
        return false;
    }

    private static bool TryInt(string s, out int value) =>
        int.TryParse(s.Trim(), System.Globalization.NumberStyles.None,
            System.Globalization.CultureInfo.InvariantCulture, out value);
}
