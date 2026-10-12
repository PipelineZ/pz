using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Tests.ProcessHosting;

/// <summary>The Rust SDK's remote statistics against the built <c>memory_sink</c> example, which
/// reports one request per batch and one more at commit. Skips when the example is not built
/// (scripts/rust-conformance.sh builds it and then runs this category).</summary>
[Trait("Category", "RustPcp")]
public sealed class RustSinkStatsTests : IDisposable
{
    private readonly List<string> _tempDirs = [];

    [SkippableFact]
    public async Task Commit_response_carries_one_request_per_batch_plus_the_commit()
    {
        var binary = MemorySinkPath();
        Skip.If(binary is null, "rust/target/debug/examples/memory_sink is not built (cargo build --example memory_sink)");

        var dir = Path.Combine(Path.GetTempPath(), "pz-rsstats-" + Guid.NewGuid().ToString("N")[..8]);
        _tempDirs.Add(dir);
        await using var process = ConnectorProcess.Spawn(binary!, dir, "memory-sink", null);
        var config = new ConnectorConfig(new Dictionary<string, object?>());
        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, null, "mem", config, CancellationToken.None);
        var connector = new ProcessSinkConnector(client, process);
        await using var sink = await connector.OpenAsync(config, CancellationToken.None);

        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Build();
        var spec = new OutputSpec("mem", "out", "replace", "match", new Dictionary<string, object?>());
        var accumulator = new ConnectorStatsAccumulator();
        const int batches = 3;
        using (ConnectorStats.Use(accumulator))
        {
            await using var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
            for (var i = 0; i < batches; i++)
            {
                var ids = new Int64Array.Builder().Append(i).Build();
                using var batch = new RecordBatch(schema, [ids], 1);
                await session.WriteBatchAsync(batch, CancellationToken.None);
            }

            var result = await session.CommitAsync(CancellationToken.None);
            Assert.Equal(batches, result.BatchesWritten);
        }

        Assert.Equal(StatsDrop.None, accumulator.Drops);
        Assert.Equal(batches + 1, accumulator.Snapshot().Requests);
    }

    private static string? MemorySinkPath()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Pz.slnx")))
        {
            dir = dir.Parent;
        }

        if (dir is null)
        {
            return null;
        }

        var path = Path.Combine(dir.FullName, "rust", "target", "debug", "examples", OperatingSystem.IsWindows() ? "memory_sink.exe" : "memory_sink");
        return File.Exists(path) ? path : null;
    }

    public void Dispose()
    {
        foreach (var dir in _tempDirs)
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort */ }
        }
    }
}
