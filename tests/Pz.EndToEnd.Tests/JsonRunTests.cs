using System.Text.Json;
using Pz.Cli;
using Pz.DuckDb;

namespace Pz.EndToEnd.Tests;

/// <summary>`pz run` moves real data end-to-end with json on BOTH
/// localfiles edges — NDJSON source -> DuckDB staging -> SQL transform -> NDJSON sink — through the
/// real CLI entry point: connector unit tests alone miss the validate/plan/run seams. Same
/// throwaway-temp-dir and console-redirection conventions as
/// <see cref="HelloRunTests"/>.</summary>
[Collection("console-redirection")]
public sealed class JsonRunTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-e2e-tests", Guid.NewGuid().ToString("N"));

    public JsonRunTests() => CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "json-to-json"), _work);

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task Run_moves_json_through_transform_to_json_natively()
    {
        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);

        // Both edges must plan native: read_json scan in, COPY TO json out.
        var planPath = Path.Combine(_work, ".pz", "target", "plan.json");
        using var plan = JsonDocument.Parse(File.ReadAllText(planPath));
        var byKind = new Dictionary<string, JsonElement>();
        foreach (var node in plan.RootElement.GetProperty("nodes").EnumerateArray())
        {
            byKind[node.GetProperty("kind").GetString()!] = node;
        }

        Assert.Equal("native_scan", byKind["SourceLoad"].GetProperty("strategy").GetString());
        Assert.Equal("native_copy", byKind["SinkWrite"].GetProperty("strategy").GetString());

        var jsonPath = Path.Combine(_work, "out", "customer_totals.json");
        Assert.True(File.Exists(jsonPath));

        await using var duck = DuckSession.Open(Path.Combine(Path.GetTempPath(), $"pz-e2e-readback-{Guid.NewGuid():N}.duckdb"));
        var quoted = jsonPath.Replace("'", "''");
        var readBack = $"read_json('{quoted}', format = 'newline_delimited')";

        Assert.Equal(3, await duck.ScalarAsync<long>($"select count(*) from {readBack}"));
        Assert.Equal(468.50, await duck.ScalarAsync<double>($"select sum(total) from {readBack}"), precision: 2);
        Assert.Equal(2, await duck.ScalarAsync<long>($"select orders from {readBack} where customer = 'Ada'"));
    }

    private static void CopyTree(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (var dir in Directory.GetDirectories(from, "*", SearchOption.AllDirectories))
        {
            Directory.CreateDirectory(Path.Combine(to, Path.GetRelativePath(from, dir)));
        }

        foreach (var file in Directory.GetFiles(from, "*", SearchOption.AllDirectories))
        {
            File.Copy(file, Path.Combine(to, Path.GetRelativePath(from, file)));
        }
    }
}
