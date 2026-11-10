using System.Numerics;

namespace Pz.Shared.Partitioning;

/// <summary>Equal-width range-partition boundaries: <c>n + 1</c> values, the first exactly
/// <c>min</c> and the last exactly <c>max</c>. Each boundary is a pure function of its index, so
/// partition k's upper bound and partition k+1's lower bound are the same value. The caller renders
/// the values as literals in its own SQL dialect.
///
/// <para>Compiled into the postgres and sqlserver connectors from this one source file: connectors
/// never reference each other.</para></summary>
internal static class EqualWidth
{
    /// <summary>Integer-domain boundaries, <c>min + floor(width * i / n)</c>. The product goes through
    /// <see cref="BigInteger"/>, since a full-range bigint width times a partition count overflows
    /// <c>long</c>.</summary>
    internal static long[] Longs(long min, long max, int n)
    {
        var result = new long[n + 1];
        var width = (BigInteger)max - min;
        for (var i = 0; i <= n; i++)
        {
            result[i] = i == n ? max : (long)(min + (width * i / n));
        }

        return result;
    }

    // Divide before multiplying: (max - min) * i overflows (to Infinity for double, an exception for
    // decimal) on extreme ranges even when every true boundary fits. The step is computed once, so
    // min + step * i stays a pure, monotonic function of i, and the last boundary is the exact max,
    // so step * (n - 1) is the largest product taken.

    internal static double[] Doubles(double min, double max, int n)
    {
        var result = new double[n + 1];
        var step = (max - min) / n;
        for (var i = 0; i <= n; i++)
        {
            result[i] = i == n ? max : min + (step * i);
        }

        return result;
    }

    internal static decimal[] Decimals(decimal min, decimal max, int n)
    {
        var result = new decimal[n + 1];
        var step = (max - min) / n;
        for (var i = 0; i <= n; i++)
        {
            result[i] = i == n ? max : min + (step * i);
        }

        return result;
    }
}
