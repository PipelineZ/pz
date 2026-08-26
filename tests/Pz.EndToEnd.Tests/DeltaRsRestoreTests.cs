using System.Text.Json;
using Pz.Cli;
using Pz.DuckDb;

namespace Pz.EndToEnd.Tests;

/// <summary>The packaging + restore proof for the Rust PCP SDK: a real <c>pz restore</c> against a
/// local NuGet feed containing the <c>.nupkg</c> <c>scripts/pack-deltalake-rs.sh</c> produces, followed
/// by a real <c>pz run</c> that writes rows through the <c>pz-deltalake</c> binary out of process.
///
/// <para>Gated on the <c>PZ_DELTALAKE_RS_NUPKG</c> env var (a path to the packed .nupkg) rather than on
/// <c>cargo</c>/toolchain probing directly: packing is a separate, slower step
/// (<c>scripts/pack-deltalake-rs.sh</c>, itself cargo-gated) that a plain `dotnet test` should not
/// silently trigger. Unset in every CI leg except the dedicated `rust` job (see
/// <c>.github/workflows/ci.yml</c>), so `dotnet test Pz.slnx` stays green without cargo — this is the
/// ONLY thing keeping this suite from needing a Rust toolchain.</para>
///
/// <para>Row verification reads <c>_delta_log/*.json</c> directly (never <c>delta_scan()</c>, DuckDB's
/// own Delta reader, which needs a network download of its extension) and counts the LIVE parquet
/// files' rows through DuckDB's built-in (no-extension, no-network) <c>read_parquet()</c> — offline in
/// the same sense every other suite gated by <c>PZ_TESTS_OFFLINE=1</c> is, even though this particular
/// suite is not itself gated by that variable (it has no network dependency to skip).</para></summary>
public sealed class DeltaRsRestoreTests : IDisposable
{
    private const string PackageId = "Pz.Connector.DeltaLakeRs";
    private const string ConnectorName = "deltalake-rs";

    private readonly List<string> _dirs = [];

    [SkippableFact]
    public async Task Packed_deltalake_rs_connector_restores_and_writes_a_delta_table_through_pz_run()
    {
        var nupkgPath = Environment.GetEnvironmentVariable("PZ_DELTALAKE_RS_NUPKG");
        Skip.If(string.IsNullOrEmpty(nupkgPath),
            "PZ_DELTALAKE_RS_NUPKG is not set; run scripts/pack-deltalake-rs.sh and export its output " +
            "path to exercise this test");
        Skip.If(!File.Exists(nupkgPath), $"PZ_DELTALAKE_RS_NUPKG names a file that does not exist: {nupkgPath}");

        var version = VersionFromNupkgFileName(nupkgPath!);

        // A local-folder feed containing only this one package, mirroring
        // tests/Pz.PackageManagement.Tests/Restore's FeedFixture pattern: NuGetResolver.ResolveAsync
        // treats any folder path as a flat local v3 feed, no nuget.config involved.
        var feedDir = NewDir();
        File.Copy(nupkgPath!, Path.Combine(feedDir, Path.GetFileName(nupkgPath!)));

        var projectDir = NewDir();
        var lakeRoot = Path.Combine(projectDir, "lake");
        WriteProject(projectDir, version, lakeRoot);

        Assert.Equal(
            ExitCodes.Ok,
            CliApp.Build().Parse(["restore", "--project", projectDir, "--feeds", feedDir]).Invoke());
        Assert.True(File.Exists(Path.Combine(projectDir, "pz.lock.json")), "restore did not write pz.lock.json");
        Assert.True(
            File.Exists(Path.Combine(projectDir, ".pz", "packages", PackageId, version, "pz.connector.json")),
            "restore did not materialize the connector package");

        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", projectDir]).Invoke());

        var tableDir = Path.Combine(lakeRoot, "orders");
        Assert.True(Directory.Exists(Path.Combine(tableDir, "_delta_log")), "no Delta table was created at lake/orders");

        Assert.Equal(4, await CountLiveParquetRowsAsync(tableDir));
    }

    /// <summary><c>Pz.Connector.DeltaLakeRs.0.1.0.nupkg</c> -&gt; <c>0.1.0</c>. Deliberately re-derived
    /// from the file the packer actually produced rather than read off Cargo.toml a second time here --
    /// this test's contract is "whatever scripts/pack-deltalake-rs.sh built restores," not "whatever
    /// Cargo.toml currently says."</summary>
    private static string VersionFromNupkgFileName(string nupkgPath)
    {
        var fileName = Path.GetFileNameWithoutExtension(nupkgPath);
        const string prefix = PackageId + ".";
        Assert.StartsWith(prefix, fileName, StringComparison.Ordinal);
        return fileName[prefix.Length..];
    }

    private static void WriteProject(string projectDir, string version, string lakeRoot)
    {
        File.WriteAllText(Path.Combine(projectDir, "project.yml"),
            "name: deltalake_rs_restore_e2e\n" +
            "version: 0.1.0\n" +
            "connectors:\n" +
            "  - package: Pz.Connector.LocalFiles\n" +
            "    version: 0.1.0\n" +
            $"  - package: {PackageId}\n" +
            $"    version: {version}\n" +
            "engine:\n" +
            "  threads: 2\n");

        File.WriteAllText(Path.Combine(projectDir, "connections.yml"), $"""
            files:
              connector: localfiles
              entities:
                orders:
                  read:
                    path: data/orders.csv
                    format: csv
                    columns:
                      id: bigint
                      customer: varchar
                      amount: double

            lake:
              connector: {ConnectorName}
              root: {lakeRoot}
            """);

        Directory.CreateDirectory(Path.Combine(projectDir, "pipelines"));
        // Default strategy (no `strategy:` kwarg) is 'append' -- the one write mode
        // rust/pz-connector-deltalake/README.md documents as memory-safest and the mode
        // scripts/rust-conformance-deltalake.sh does NOT already cover (that probe uses 'replace'), so
        // this proof exercises the one path the conformance suite leaves untouched.
        File.WriteAllText(Path.Combine(projectDir, "pipelines", "orders.sql"),
            "INSERT INTO {{ sink('lake', 'orders') }}\n" +
            "select id, customer, amount\n" +
            "from {{ source('files', 'orders') }}\n");

        Directory.CreateDirectory(Path.Combine(projectDir, "data"));
        File.WriteAllText(Path.Combine(projectDir, "data", "orders.csv"),
            "id,customer,amount\n1,ann,10.5\n2,bob,20.25\n3,ann,5.25\n4,cy,100.0\n");
    }

    /// <summary>The live (added, not later removed) parquet file set recorded across every
    /// <c>_delta_log/*.json</c> commit, row-counted via DuckDB's built-in <c>read_parquet()</c> -- no
    /// Delta protocol support needed on the reading side, just the physical files the log's own `add`
    /// actions name.</summary>
    private static async Task<long> CountLiveParquetRowsAsync(string tableDir)
    {
        var logDir = Path.Combine(tableDir, "_delta_log");
        var live = new HashSet<string>(StringComparer.Ordinal);
        foreach (var logFile in Directory.EnumerateFiles(logDir, "*.json").OrderBy(f => f, StringComparer.Ordinal))
        {
            foreach (var line in File.ReadLines(logFile))
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                using var commit = JsonDocument.Parse(line);
                if (commit.RootElement.TryGetProperty("add", out var add))
                {
                    live.Add(Uri.UnescapeDataString(add.GetProperty("path").GetString()!));
                }
                else if (commit.RootElement.TryGetProperty("remove", out var remove))
                {
                    live.Remove(Uri.UnescapeDataString(remove.GetProperty("path").GetString()!));
                }
            }
        }

        Assert.NotEmpty(live);

        var absolutePaths = live.Select(relative => Path.Combine(tableDir, relative)).ToArray();
        foreach (var path in absolutePaths)
        {
            Assert.True(File.Exists(path), $"_delta_log names '{path}' but it is not on disk");
        }

        await using var session = DuckSession.Open(":memory:");
        var files = string.Join(", ", absolutePaths.Select(p => "'" + p.Replace("'", "''", StringComparison.Ordinal) + "'"));
        return await session.ScalarAsync<long>($"select count(*)::bigint from read_parquet([{files}])");
    }

    private string NewDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pzdlrs" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);
        return dir;
    }

    public void Dispose()
    {
        foreach (var dir in _dirs)
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
