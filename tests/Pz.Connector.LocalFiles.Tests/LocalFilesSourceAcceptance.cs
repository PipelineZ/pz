using System.Globalization;
using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.LocalFiles.Tests;

/// <summary>Runs the TestKit's source acceptance suite against the real <see cref="LocalFilesConnector"/>
/// CSV source, with fixture CSVs generated in a throwaway temp dir per test instance (xunit creates a
/// fresh instance per [Fact]). v0 is single-partition, so <see cref="GetSpecWithPartitionOverride"/>
/// stays null — the weak re-plan-idempotency fallback applies.</summary>
public sealed class LocalFilesSourceAcceptance : SourceConnectorAcceptanceTests, IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-localfiles-tests", Guid.NewGuid().ToString("N"));

    public LocalFilesSourceAcceptance()
    {
        Directory.CreateDirectory(_dir);
        WriteSmallCsv(Path.Combine(_dir, "small.csv"), rows: 150);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    protected override ISourceConnector CreateSource() => new LocalFilesConnector();

    protected override ConnectorConfig ValidConfig =>
        new(new Dictionary<string, object?> { ["base_dir"] = _dir });

    private static readonly Dictionary<string, string> Columns = new()
    {
        ["id"] = "bigint",
        ["name"] = "varchar",
        ["amount"] = "double",
        ["flag"] = "boolean",
        ["created"] = "timestamp",
    };

    protected override DatasetSpec SmallDataset => new("files", "orders", new Dictionary<string, object?>
    {
        ["path"] = "small.csv",
        ["format"] = "csv",
        ["columns"] = Columns,
    });

    protected override DatasetSpec? GetSpecWithPartitionOverride(int partitions) => null;

    private static void WriteSmallCsv(string path, int rows)
    {
        using var writer = new StreamWriter(path);
        writer.NewLine = "\n";
        writer.WriteLine("id,name,amount,flag,created");
        var start = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        for (var i = 0; i < rows; i++)
        {
            var ts = start.AddMinutes(i);
            writer.WriteLine(string.Join(',',
                i.ToString(CultureInfo.InvariantCulture),
                $"row-{i}",
                (i * 1.5).ToString(CultureInfo.InvariantCulture),
                (i % 2 == 0).ToString(),
                ts.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture)));
        }
    }
}
