using System.Security.Cryptography;
using System.Text;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connectors.Toolkit.Sql;

/// <summary>Text building blocks for SQL a connector hands DuckDB: native-scan fragments, setup
/// statements and copy statements. Every value spliced into such SQL goes through one of these, so
/// identifier quoting, literal escaping and watermark rendering cannot drift between connectors.</summary>
public static class DuckDbSqlText
{
    /// <summary>Double-quotes <paramref name="name"/> as one identifier, doubling any embedded quote.
    /// The ANSI form DuckDB, SQLite and Postgres all accept; MySQL's backtick form is not this.</summary>
    public static string QuoteIdentifier(string name) => $"\"{name.Replace("\"", "\"\"")}\"";

    /// <summary>Escapes <paramref name="value"/> for the inside of a single-quoted literal by doubling
    /// every single quote. The caller supplies the surrounding quotes.</summary>
    public static string EscapeLiteral(string value) => value.Replace("'", "''");

    /// <summary>Renders the engine's canonical watermark string (plain digits for int/bigint/decimal,
    /// <c>yyyy-MM-dd</c> for date, <c>yyyy-MM-ddTHH:mm:ss.ffffff</c> for timestamp) as a literal.
    /// Numerics stay bare; anything else is quoted, with a canonical timestamp's <c>T</c> separator
    /// becoming a space so the literal coerces against DATE/TIMESTAMP columns and still compares
    /// lexically against a text-typed cursor.</summary>
    public static string WatermarkLiteral(string canonical)
    {
        if (IsCanonicalNumeric(canonical))
        {
            return canonical;
        }

        var value = IsCanonicalTimestamp(canonical)
            ? canonical[..10] + ' ' + canonical[11..]
            : canonical;
        return $"'{EscapeLiteral(value)}'";
    }

    /// <summary>The native-scan fragment over <paramref name="relation"/> (a qualified table or a table
    /// function call): a declared <c>columns:</c> contract projects only the declared columns, the
    /// watermark's lower bound (strict or inclusive) and the window's upper bound become predicates. The
    /// engine embeds the fragment after FROM, so a plain scan stays the bare relation and anything with a
    /// projection or predicate becomes a parenthesized subquery.</summary>
    public static string ScanFragment(string relation, DatasetSpec spec)
    {
        var columns = ColumnTypeCatalog.ContractColumns(spec);
        var projection = columns is { Count: > 0 }
            ? string.Join(", ", columns.Keys.Select(QuoteIdentifier))
            : "*";

        var predicates = new List<string>();
        if (spec is { WatermarkCursor: { } cursor, WatermarkValue: { } low })
        {
            var op = spec.WatermarkLowerInclusive ? ">=" : ">";
            predicates.Add($"{QuoteIdentifier(cursor)} {op} {WatermarkLiteral(low)}");
        }

        if (spec is { WatermarkCursor: { } upperCursor, WatermarkUpperBound: { } high })
        {
            predicates.Add($"{QuoteIdentifier(upperCursor)} <= {WatermarkLiteral(high)}");
        }

        if (projection == "*" && predicates.Count == 0)
        {
            return relation;
        }

        var where = predicates.Count > 0 ? $" where {string.Join(" and ", predicates)}" : "";
        return $"(select {projection} from {relation}{where})";
    }

    /// <summary>A catalog alias or secret name for one connection: <c>{prefix}_{sanitized}_{hash}</c>.
    /// Sanitizing maps every non-alphanumeric character to <c>_</c>, so distinct names such as
    /// <c>a-b</c> and <c>a_b</c> would collide; the hash suffix (see <see cref="StableSuffix"/>) of the
    /// raw name keeps them apart. Deterministic, so the alias is stable across runs and processes and
    /// a once-per-run setup statement keyed by its text is recognized as the same statement.</summary>
    public static string ConnectionAlias(string prefix, string connectionName) =>
        $"{prefix}_{Sanitize(connectionName)}_{StableSuffix(connectionName)}";

    /// <summary>The first 8 lowercase hex characters of the SHA-256 of <paramref name="name"/>'s UTF-8
    /// bytes: a short, deterministic discriminator for generated object names.</summary>
    public static string StableSuffix(string name) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(name)))[..8];

    private static string Sanitize(string name) =>
        new([.. name.Select(c => char.IsAsciiLetterOrDigit(c) ? c : '_')]);

    /// <summary>Optional leading '-', digits, at most one '.'. A canonical date ("2020-01-01") has an
    /// interior '-', so it never matches and falls through to the quoted form.</summary>
    private static bool IsCanonicalNumeric(string value)
    {
        var i = value.StartsWith('-') ? 1 : 0;
        var sawDigit = false;
        var sawDot = false;
        for (; i < value.Length; i++)
        {
            if (value[i] == '.')
            {
                if (sawDot)
                {
                    return false;
                }

                sawDot = true;
            }
            else if (char.IsAsciiDigit(value[i]))
            {
                sawDigit = true;
            }
            else
            {
                return false;
            }
        }

        return sawDigit;
    }

    private static bool IsCanonicalTimestamp(string value) =>
        value.Length >= 19 && value[10] == 'T' &&
        value[4] == '-' && value[7] == '-' && value[13] == ':' && value[16] == ':';
}
