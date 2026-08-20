using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Benchmarks;

/// <summary>Deterministic (fixed-seed) in-memory row data shared by the benchmark classes --
/// a 4-column matrix (int64/utf8/double/bool) representative of the v0 type matrix without exercising
/// every one of its eight types. Row VALUES are generated once per benchmark's GlobalSetup; the Arrow
/// batches themselves are built inside the measured region, since building batches from row-shaped
/// values is exactly the cost "ingest/egress N rows" is meant to capture.</summary>
internal static class BenchData
{
    public static Schema BuildSchema() => new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("name").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("amount").DataType(DoubleType.Default).Nullable(false))
        .Field(f => f.Name("active").DataType(BooleanType.Default).Nullable(false))
        .Build();

    /// <summary>Generates <paramref name="rowCount"/> rows with a fixed seed, so repeated runs (and
    /// repeated BenchmarkDotNet invocations within a run) always see byte-identical input data.</summary>
    public static object?[][] GenerateRows(int rowCount, int seed = 42)
    {
        var random = new Random(seed);
        var rows = new object?[rowCount][];
        for (var i = 0; i < rowCount; i++)
        {
            rows[i] = [(long)i, $"name-{i % 10_000}", random.NextDouble() * 1000, i % 2 == 0];
        }

        return rows;
    }
}
