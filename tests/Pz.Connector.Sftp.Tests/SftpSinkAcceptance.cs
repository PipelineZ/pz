using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Pz.TestSupport;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

/// <summary>Runs the TestKit's sink acceptance suite against the real <see cref="SftpConnector"/> over
/// a live atmoz/sftp container (<see cref="SftpContainerFixture"/>). <see cref="SmallOutput"/>/<see
/// cref="ReplaceOutput"/> both write csv (not the default parquet) so <see cref="ReadCommittedAsync"/>
/// can read the committed file straight back through <see cref="SftpConnector"/>'s OWN source side
/// (<see cref="SftpConnector"/> implements both <see cref="ISourceConnector"/> and
/// <see cref="ISinkConnector"/>) with a typed <c>columns:</c> contract, rather than parsing bytes by
/// hand the way <c>LocalFilesSinkAcceptance</c>/<c>AzureSinkAcceptance</c> read parquet directly off
/// disk/blob -- sftp has no local/SDK-level file access to piggyback on, but it does have a source of
/// its own. "replace" mode is required for that read-back: it commits to the SAME stable
/// <c>&lt;output&gt;.csv</c> name every time (unlike "append"'s guid-suffixed name), which is what lets
/// <see cref="ReadCommittedAsync"/> find one deterministic object across the multiple sessions several
/// facts open against <see cref="SmallOutput"/>. A unique per-instance remote prefix (xunit constructs a
/// fresh instance per [Fact]) keeps one fact's writes from colliding with another's on the shared
/// container.</summary>
[Collection("sftp")]
public sealed class SftpSinkAcceptance : SinkConnectorAcceptanceTests
{
    private static readonly Dictionary<string, string> ReadBackColumns = new()
    {
        ["id"] = "bigint",
        ["name"] = "varchar",
    };

    private readonly SftpContainerFixture _fixture;
    private readonly string _prefix = $"sink-accept-{Guid.NewGuid():N}";

    public SftpSinkAcceptance(SftpContainerFixture fixture) => _fixture = fixture;

    // Every inherited [SkippableFact] calls this before doing any work, so a docker-less run SKIPs
    // cleanly instead of failing when docker (and therefore the atmoz/sftp container) is absent.
    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISinkConnector CreateSink() => new SftpConnector();

    protected override ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["host"] = _fixture.Host,
        ["port"] = _fixture.Port,
        ["username"] = SftpContainerFixture.PasswordUser,
        ["password"] = SftpContainerFixture.Password,
        ["root"] = "upload",
    });

    protected override OutputSpec SmallOutput => new("sftp", "out", "replace", "fail_on_change",
        new Dictionary<string, object?> { ["path"] = _prefix, ["format"] = "csv" });

    protected override OutputSpec? ReplaceOutput => new("sftp", "replace-out", "replace", "fail_on_change",
        new Dictionary<string, object?> { ["path"] = _prefix, ["format"] = "csv" });

    protected override async ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec)
    {
        // SftpConnector implements ISourceConnector too -- CreateSink() (the base suite's only
        // construction path) always hands back that same concrete type, so this cast is safe.
        var sourceConnector = (ISourceConnector)connector;
        await using var source = await sourceConnector.OpenAsync(ValidConfig, CancellationToken.None);

        var pathPrefix = spec.Options["path"]!.ToString()!;
        var readSpec = new DatasetSpec(spec.Sink, spec.Output, new Dictionary<string, object?>
        {
            ["path"] = $"{pathPrefix}/{spec.Output}.csv",
            ["format"] = "csv",
            ["columns"] = ReadBackColumns,
        });

        IReadOnlyList<IDatasetPartition> partitions;
        try
        {
            partitions = await source.PlanReadAsync(readSpec, ReadHints.None, CancellationToken.None);
        }
        catch (PzConnectorException)
        {
            // No committed file yet (nothing has ever been written, or the last session aborted) --
            // ReadCommittedAsync's contract is to report that as "nothing committed", not to throw.
            return [];
        }

        var batches = new List<RecordBatch>();
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batches.Add(batch);
            }
        }

        return batches;
    }
}
