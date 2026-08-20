using Pz.Core.Model;
using Pz.Engine.Planning;

namespace Pz.Engine.Tests.Planning;

/// <summary>The budget formula: <c>duckdb.memory_limit (parsed) + engine.threads * 6 * batch_bytes
/// + 256MB fixed overhead</c>. 6 = bounded channel capacity 4 + 1 producing + 1 ingesting batch.</summary>
public sealed class MemoryBudgetTests
{
    /// <summary>engine.duckdb.threads is a DIFFERENT key from engine.threads and is unset by default, so
    /// DuckDB picks the machine's core count — and DuckDB's memory floor for materialising a table scales
    /// with columns times threads. A 20k-row x 1000-column table can therefore exhaust a 1GiB
    /// memory_limit while this budget reports 1.63 GB of headroom. The formula cannot
    /// simply grow a column term: a contract-less csv dataset has no declared schema at plan time, which
    /// is exactly the case at risk. So the budget says what it does not know, the way it already does
    /// for an unset memory_limit, rather than printing a number that looks authoritative and is not.</summary>
    [Fact]
    public void Budget_with_unset_duckdb_threads_carries_a_thread_disclaimer()
    {
        var engine = new EngineConfig(Threads: 2, DuckDb: new DuckOptionsConfig(MemoryLimit: "1GiB"));

        var budget = MemoryBudget.Compute(engine);

        Assert.NotNull(budget.DuckDbThreadsDisclaimer);
        Assert.Contains("engine.duckdb.threads", budget.DuckDbThreadsDisclaimer);
    }

    /// <summary>Set the key and the caveat goes away — the thread count is then deterministic and the
    /// project's own, not the machine's.</summary>
    [Fact]
    public void Budget_with_configured_duckdb_threads_has_no_thread_disclaimer()
    {
        var engine = new EngineConfig(Threads: 2, DuckDb: new DuckOptionsConfig(MemoryLimit: "1GiB", Threads: 1));

        var budget = MemoryBudget.Compute(engine);

        Assert.Null(budget.DuckDbThreadsDisclaimer);
    }

    [Fact]
    public void Budget_formula_matches_decision8()
    {
        var engine = new EngineConfig(Threads: 2, DuckDb: new DuckOptionsConfig(MemoryLimit: "1GiB"));

        var budget = MemoryBudget.Compute(engine);

        Assert.Equal(1_073_741_824L, budget.DuckDbBytes);
        Assert.Null(budget.DuckDbDisclaimer);
        Assert.Equal(402_653_184L, budget.ChannelBytes); // 2 threads * 6 * 32MiB default batch_bytes
        Assert.Equal(268_435_456L, budget.FixedOverheadBytes); // 256MB
        Assert.Equal(1_744_830_464L, budget.TotalBytes); // sum of the three above
    }

    [Fact]
    public void Budget_formula_honors_configured_batch_bytes()
    {
        var engine = new EngineConfig(Threads: 3, DuckDb: new DuckOptionsConfig(MemoryLimit: "2GiB"), BatchBytes: 10_000_000);

        var budget = MemoryBudget.Compute(engine);

        Assert.Equal(2_147_483_648L, budget.DuckDbBytes);
        Assert.Equal(180_000_000L, budget.ChannelBytes); // 3 * 6 * 10_000_000
        Assert.Equal(268_435_456L, budget.FixedOverheadBytes);
        Assert.Equal(2_147_483_648L + 180_000_000L + 268_435_456L, budget.TotalBytes);
    }

    [Fact]
    public void Budget_with_unset_memory_limit_carries_disclaimer()
    {
        var engine = new EngineConfig(); // DuckDb null -> memory_limit unset

        var budget = MemoryBudget.Compute(engine);

        Assert.Null(budget.DuckDbBytes);
        Assert.False(string.IsNullOrWhiteSpace(budget.DuckDbDisclaimer));
        Assert.Equal(4L * 6 * 33_554_432, budget.ChannelBytes); // default threads=4, default batch_bytes=32MiB
        Assert.Equal(268_435_456L, budget.FixedOverheadBytes);
        Assert.Equal(budget.ChannelBytes + budget.FixedOverheadBytes, budget.TotalBytes);
    }

    [Fact]
    public void Budget_with_percentage_memory_limit_carries_disclaimer()
    {
        // DuckDB itself accepts a bare percentage ("80%") for memory_limit -- not a fixed byte size, so
        // this must be disclaimed the same way an unset memory_limit is, never silently mis-parsed.
        var engine = new EngineConfig(DuckDb: new DuckOptionsConfig(MemoryLimit: "80%"));

        var budget = MemoryBudget.Compute(engine);

        Assert.Null(budget.DuckDbBytes);
        Assert.False(string.IsNullOrWhiteSpace(budget.DuckDbDisclaimer));
        Assert.Contains("80%", budget.DuckDbDisclaimer);
    }

    [Theory]
    [InlineData("512MiB", 536_870_912L)]
    [InlineData("1GB", 1_000_000_000L)]
    [InlineData("2TiB", 2L * 1024 * 1024 * 1024 * 1024)]
    [InlineData("1048576", 1_048_576L)]
    public void Byte_size_parser_handles_decimal_and_binary_units(string text, long expectedBytes)
    {
        Assert.True(MemoryBudget.TryParseByteSize(text, out var bytes));
        Assert.Equal(expectedBytes, bytes);
    }
}
