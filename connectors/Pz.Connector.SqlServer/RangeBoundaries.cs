using System.Globalization;
using System.Numerics;

namespace Pz.Connector.SqlServer;

/// <summary>Equal-width partition boundary literals. boundary(i) = min + floor(width*i/n),
/// a pure function of i, so partition k's hi IS partition k+1's lo. Integer domains interpolate via
/// BigInteger (full-range bigint times a partition count must not overflow). Temporal literals render
/// as casts on the LITERAL, never the column, keeping range predicates sargable.</summary>
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
        int or long => Longs(Convert.ToInt64(min, CultureInfo.InvariantCulture),
                Convert.ToInt64(max, CultureInfo.InvariantCulture), n)
            .Select(v => v.ToString(CultureInfo.InvariantCulture)).ToArray(),
        double d => Doubles(d, (double)max, n),
        decimal m => Decimals(m, (decimal)max, n),
        DateOnly lo => Longs(lo.DayNumber, ((DateOnly)max).DayNumber, n)
            .Select(day => $"cast('{DateOnly.FromDayNumber((int)day).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)}' as date)")
            .ToArray(),
        DateTime dt => Longs(dt.Ticks, ((DateTime)max).Ticks, n)
            .Select(ticks => $"cast('{new DateTime(ticks, DateTimeKind.Unspecified)
                .ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)}' as datetime2(6))")
            .ToArray(),
        DateTimeOffset dto => Longs(dto.UtcDateTime.Ticks, ((DateTimeOffset)max).UtcDateTime.Ticks, n)
            .Select(ticks => $"cast('{new DateTime(ticks, DateTimeKind.Utc)
                .ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)} +00:00' as datetimeoffset(6))")
            .ToArray(),
        _ => throw new InvalidOperationException(
            $"partition boundary type {min.GetType().Name} is not orderable -- callers must check RangeBoundaries.IsOrderable first"),
    };

    private static long[] Longs(long min, long max, int n)
    {
        var result = new long[n + 1];
        var width = (BigInteger)max - min;
        for (var i = 0; i <= n; i++)
        {
            result[i] = i == n ? max : (long)(min + (width * i / n));
        }

        return result;
    }

    private static string[] Doubles(double min, double max, int n)
    {
        var result = new string[n + 1];
        // Divide-before-multiply: (max-min)*i overflows to Infinity for extreme ranges even when the
        // true boundary fits. step is computed once, so boundary(i) = min + step*i stays a pure,
        // monotonic function of i (adjacent partitions still share the identical literal). i == n
        // is special-cased to the exact max, so step*(n-1) cannot overflow.
        var step = (max - min) / n;
        for (var i = 0; i <= n; i++)
        {
            var boundary = i == n ? max : min + (step * i);
            result[i] = boundary.ToString("G17", CultureInfo.InvariantCulture);
        }

        return result;
    }

    private static string[] Decimals(decimal min, decimal max, int n)
    {
        var result = new string[n + 1];
        // Divide-before-multiply: (max-min)*i overflows decimal for extreme ranges even when the
        // true boundary fits. step is computed once, so boundary(i) = min + step*i stays a pure,
        // monotonic function of i (adjacent partitions still share the identical literal). i == n
        // is special-cased to the exact max, so step*(n-1) cannot overflow.
        var step = (max - min) / n;
        for (var i = 0; i <= n; i++)
        {
            var boundary = i == n ? max : min + (step * i);
            result[i] = boundary.ToString(CultureInfo.InvariantCulture);
        }

        return result;
    }
}
