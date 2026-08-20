using System.Globalization;

namespace Pz.Core.Loading;

/// <summary>The one duration grammar for user-facing config (retry delays, windows, breaker
/// cool-downs) — no per-feature dialects. Accepts
/// <c>&lt;non-negative integer&gt;&lt;unit&gt;</c> with unit one of <c>ms|s|m|h|d</c> — lowercase only,
/// no whitespace, no sign, no fractions, no compounds (<c>5m2s</c> is rejected; write <c>302s</c>).
/// Positivity is deliberately NOT enforced here — <c>0s</c> parses fine; whether zero is allowed is the
/// caller's validation rule, so the parser stays reusable for future fields where zero is legal.</summary>
public static class DurationParser
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

        var digits = text[..unitStart];
        var unit = text[unitStart..];
        if (digits.Length == 0 ||
            !long.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out var magnitude))
        {
            return false;
        }

        // TimeSpan.From* throws OverflowException past TimeSpan.MaxValue (~10.6M days) — a config typo
        // like an extra digit run must surface as "unparseable", never as an engine crash.
        try
        {
            switch (unit)
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
