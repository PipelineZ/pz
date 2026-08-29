using System.Globalization;
using System.Text;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Pz.TestSupport;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

/// <summary>Runs the TestKit's source acceptance suite against the real <see cref="SftpConnector"/>
/// over a live atmoz/sftp container (<see cref="SftpContainerFixture"/>). Every seed file this class
/// writes goes through <see cref="SftpClientFactory.Open"/> directly (the internal test seam), never
/// through <see cref="SftpConnector"/> itself -- reads under test stay independent of any sink under
/// test. <see cref="SmallDataset"/> matches several files under one glob, so
/// <c>Partitions_union_equals_single_partition_read</c> actually exercises the multi-partition/
/// ground-truth path (via <see cref="GetSpecWithPartitionOverride"/>) rather than the weak re-plan
/// fallback. A unique per-instance remote prefix (xunit constructs a fresh instance per [Fact]) keeps
/// one fact's seeded files from colliding with another's on the shared container.</summary>
[Collection("sftp")]
public sealed class SftpSourceAcceptance : SourceConnectorAcceptanceTests
{
    private const int FileCount = 3;
    private const int RowsPerFile = 50;

    private readonly SftpContainerFixture _fixture;
    private readonly string _prefix = $"source-accept-{Guid.NewGuid():N}";

    public SftpSourceAcceptance(SftpContainerFixture fixture)
    {
        _fixture = fixture;
        SeedSmallDataset();
        SeedWindowDataset();
    }

    // Every inherited [SkippableFact] calls this before doing any work, so a docker-less run SKIPs
    // cleanly instead of failing when docker (and therefore the atmoz/sftp container) is absent.
    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISourceConnector CreateSource() => new SftpConnector();

    protected override ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["host"] = _fixture.Host,
        ["port"] = _fixture.Port,
        ["username"] = SftpContainerFixture.PasswordUser,
        ["password"] = SftpContainerFixture.Password,
        ["root"] = "upload",
    });

    private static readonly Dictionary<string, string> OrderColumns = new()
    {
        ["id"] = "bigint",
        ["name"] = "varchar",
        ["amount"] = "double",
        ["flag"] = "boolean",
        ["created"] = "timestamp",
    };

    // files_per_partition unset -> SftpSource's own default of 1: FileCount files under this glob plan
    // into FileCount partitions, so the base suite's multi-partition facts actually exercise more than
    // one partition instead of trivially short-circuiting.
    protected override DatasetSpec SmallDataset => new("sftp", "orders", new Dictionary<string, object?>
    {
        ["path"] = $"{_prefix}/orders-*.csv",
        ["format"] = "csv",
        ["columns"] = OrderColumns,
    });

    // Forces every matched file into ONE partition -- the ground-truth single-partition read
    // Partitions_union_equals_single_partition_read compares the multi-partition read against.
    protected override DatasetSpec? GetSpecWithPartitionOverride(int partitions) =>
        partitions == 1
            ? SmallDataset with
            {
                Options = new Dictionary<string, object?>(SmallDataset.Options) { ["files_per_partition"] = FileCount },
            }
            : null;

    private static readonly Dictionary<string, string> WindowColumns = new()
    {
        ["cursor"] = "bigint",
        ["val"] = "varchar",
    };

    // Seed cursor values 0..10 (see SeedWindowDataset); lower=3 (exclusive)/upper=7 (inclusive) must
    // yield exactly cursor values 4,5,6,7 -- the BoundedWindow_* fact's fixed contract.
    protected override DatasetSpec? BoundedWindowDataset => new DatasetSpec("sftp", "window", new Dictionary<string, object?>
    {
        ["path"] = $"{_prefix}/window.csv",
        ["format"] = "csv",
        ["columns"] = WindowColumns,
    })
    {
        WatermarkCursor = "cursor",
        WatermarkValue = "3",
        WatermarkUpperBound = "7",
    };

    private void SeedSmallDataset()
    {
        using var fs = SftpClientFactory.Open(SeedSettings());
        fs.CreateDirectories($"upload/{_prefix}");

        var id = 0;
        var start = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        for (var file = 0; file < FileCount; file++)
        {
            var sb = new StringBuilder("id,name,amount,flag,created\n");
            for (var row = 0; row < RowsPerFile; row++)
            {
                var ts = start.AddMinutes(id);
                sb.Append(string.Join(',',
                    id.ToString(CultureInfo.InvariantCulture),
                    $"row-{id}",
                    (id * 1.5).ToString(CultureInfo.InvariantCulture),
                    (id % 2 == 0).ToString(),
                    ts.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture)));
                sb.Append('\n');
                id++;
            }

            WriteFile(fs, $"upload/{_prefix}/orders-{file}.csv", sb.ToString());
        }
    }

    private void SeedWindowDataset()
    {
        using var fs = SftpClientFactory.Open(SeedSettings());
        var sb = new StringBuilder("cursor,val\n");
        for (var i = 0; i <= 10; i++)
        {
            sb.Append(i.ToString(CultureInfo.InvariantCulture)).Append(',').Append('v').Append(i).Append('\n');
        }

        WriteFile(fs, $"upload/{_prefix}/window.csv", sb.ToString());
    }

    private static void WriteFile(ISftpFileSystem fs, string path, string content)
    {
        using var stream = fs.OpenWrite(path);
        var bytes = Encoding.UTF8.GetBytes(content);
        stream.Write(bytes, 0, bytes.Length);
    }

    private SftpConnectionSettings SeedSettings() => new(
        _fixture.Host, _fixture.Port, SftpContainerFixture.PasswordUser, SftpContainerFixture.Password,
        null, null, null, Root: null);
}
