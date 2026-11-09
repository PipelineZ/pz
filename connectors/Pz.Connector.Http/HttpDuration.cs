using System.Globalization;

namespace Pz.Connector.Http;

/// <summary>The http connector's duration grammar for config (a session <c>ttl</c>, a rule's <c>retry_after</c>): a
/// non-negative integer and one of <c>ms|s|m|h|d</c>, lowercase, with no spaces, sign, fraction or compound. The
/// connector's JSON schema patterns mirror it.</summary>
internal static class HttpDuration
{
    public static bool TryParse(string? text, out TimeSpan value)
    {
        value = default;
        if (string.IsNullOrEmpty(text))
        {
            return false;
        }

        var unitStart = 0;
        while (unitStart < text.Length && char.IsAsciiDigit(text[unitStart]))
        {
            unitStart++;
        }

        if (unitStart == 0 || !long.TryParse(text.AsSpan(0, unitStart), NumberStyles.None,
                CultureInfo.InvariantCulture, out var magnitude))
        {
            return false;
        }

        try
        {
            switch (text[unitStart..])
            {
                case "ms": value = TimeSpan.FromMilliseconds(magnitude); return true;
                case "s": value = TimeSpan.FromSeconds(magnitude); return true;
                case "m": value = TimeSpan.FromMinutes(magnitude); return true;
                case "h": value = TimeSpan.FromHours(magnitude); return true;
                case "d": value = TimeSpan.FromDays(magnitude); return true;
                default: return false;
            }
        }
        catch (OverflowException)
        {
            return false;
        }
    }
}
