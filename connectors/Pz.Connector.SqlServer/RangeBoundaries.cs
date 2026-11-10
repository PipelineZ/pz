using System.Globalization;
using Pz.Shared.Partitioning;

namespace Pz.Connector.SqlServer;

/// <summary>Equal-width partition boundary literals in T-SQL (the values come from
/// <see cref="EqualWidth"/>). Temporal literals render as casts on the LITERAL, never the column,
/// keeping range predicates sargable.</summary>
internal static class RangeBoundaries
{
    private static readonly HashSet<Type> Orderable =
    [
        typeof(int), typeof(long), typeof(double), typeof(decimal),
        typeof(DateOnly), typeof(DateTime), typeof(DateTimeOffset),
    ];

    public static bool IsOrderable(Type clrType) => Orderable.Contains(clrType);

    public static string[] ComputeLiterals(object min, object max, int n) => min switch
    {
        int or long => EqualWidth.Longs(Convert.ToInt64(min, CultureInfo.InvariantCulture),
                Convert.ToInt64(max, CultureInfo.InvariantCulture), n)
            .Select(v => v.ToString(CultureInfo.InvariantCulture)).ToArray(),
        double d => EqualWidth.Doubles(d, (double)max, n)
            .Select(v => v.ToString("G17", CultureInfo.InvariantCulture)).ToArray(),
        decimal m => EqualWidth.Decimals(m, (decimal)max, n)
            .Select(v => v.ToString(CultureInfo.InvariantCulture)).ToArray(),
        DateOnly lo => EqualWidth.Longs(lo.DayNumber, ((DateOnly)max).DayNumber, n)
            .Select(day => $"cast('{DateOnly.FromDayNumber((int)day).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)}' as date)")
            .ToArray(),
        DateTime dt => EqualWidth.Longs(dt.Ticks, ((DateTime)max).Ticks, n)
            .Select(ticks => $"cast('{new DateTime(ticks, DateTimeKind.Unspecified)
                .ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)}' as datetime2(6))")
            .ToArray(),
        DateTimeOffset dto => EqualWidth.Longs(dto.UtcDateTime.Ticks, ((DateTimeOffset)max).UtcDateTime.Ticks, n)
            .Select(ticks => $"cast('{new DateTime(ticks, DateTimeKind.Utc)
                .ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)} +00:00' as datetimeoffset(6))")
            .ToArray(),
        _ => throw new InvalidOperationException(
            $"partition boundary type {min.GetType().Name} is not orderable -- callers must check RangeBoundaries.IsOrderable first"),
    };

}
