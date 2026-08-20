using Pz.DuckDb;

namespace Pz.DuckDb.Tests;

public sealed class DuckSessionTests : IDisposable
{
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));

    public DuckSessionTests()
    {
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task Open_creates_database_file_on_disk()
    {
        var path = Path.Combine(_tempDir, "smoke.duckdb");

        await using var session = DuckSession.Open(path);

        Assert.True(File.Exists(path));
    }

    [Fact]
    public async Task Scalar_query_roundtrips()
    {
        var path = Path.Combine(_tempDir, "scalar.duckdb");

        await using var session = DuckSession.Open(path);
        var result = await session.ScalarAsync<int>("select 40 + 2");

        Assert.Equal(42, result);
    }

    [Fact]
    public async Task Options_apply()
    {
        var path = Path.Combine(_tempDir, "options.duckdb");

        // DuckDB >= 1.5 distinguishes decimal "GB" (10^9 bytes, normalized "953.6 MiB")
        // from binary "GiB" (normalized "1.0 GiB"), so "1GiB" is what keeps the asserted
        // tolerance ("1.0 GiB" or "1GB") satisfiable on the current native library.
        var options = new DuckOptions(MemoryLimit: "1GiB");

        await using var session = DuckSession.Open(path, options);
        var result = await session.ScalarAsync<string>("select current_setting('memory_limit')");

        Assert.True(
            result.Contains("1.0 GiB", StringComparison.Ordinal) || result.Contains("1GB", StringComparison.Ordinal),
            $"Expected memory_limit to normalize to '1.0 GiB' or '1GB', got '{result}'.");
    }
}
