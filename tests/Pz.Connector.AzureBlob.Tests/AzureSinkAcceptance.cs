using Apache.Arrow;
using Apache.Arrow.Types;
using Azure.Storage.Blobs;
using Parquet;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Pz.TestSupport;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary>Runs the TestKit's sink acceptance suite against the real <see cref="AzureConnector"/> parquet
/// sink over a live Azurite instance (<see cref="AzuriteFixture"/>). Azure's sink has the SAME shape as
/// LocalFiles' (see <c>LocalFilesSinkAcceptance</c>): an object/blob write, append/replace modes only, no
/// merge -- <see cref="MergeOutput"/> is left at its inherited null default so the 4 <c>Merge_*</c> facts
/// become Skip-free no-ops, exactly like <c>LocalFilesSinkAcceptance</c>.</summary>
[Collection("azurite")]
public sealed class AzureSinkAcceptance(AzuriteFixture fixture) : SinkConnectorAcceptanceTests
{
    // Mirrors LocalFilesSinkAcceptance's FixedSchema -- SinkConnectorAcceptanceTests always writes this
    // exact id/name shape, so verification can read the committed parquet blob directly via Parquet.Net.
    private static readonly Schema FixedSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: false),
        new Field("name", StringType.Default, nullable: false),
    ], null);

    // Unique per-instance prefix: the shared "pz-e2e" container (AzuriteFixture.Container) is not cleaned
    // between test runs/collections, so a fixed blob path would eventually collide with blobs left over
    // from a prior run (the same guid-prefix convention used throughout this suite, e.g.
    // AzureUniversalSinkEndToEndTests). xunit creates a fresh AzureSinkAcceptance instance per [Fact], so
    // each fact gets its own prefix -- one fact's committed blob can never leak into another fact's
    // ReadCommittedAsync.
    private readonly string _prefix = $"sink-accept-{Guid.NewGuid():N}";

    // Every inherited [SkippableFact] calls this before doing any work, so a
    // docker-less run SKIPs cleanly instead of failing when docker (and therefore Azurite) is absent.
    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISinkConnector CreateSink() => new AzureConnector();

    protected override ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["auth"] = "connection_string",
        ["connection_string"] = fixture.ConnectionString,
    });

    // "replace" mode (AzureSink.ResolveFinalLocation) commits to a STABLE object name (<output>.parquet),
    // unlike "append" which lands under a run-unique guid-suffixed name each commit -- the base class's
    // facts open more than one session against this SAME SmallOutput expecting to find (or not find) one
    // deterministic object, so "replace" is required for ReadCommittedAsync to locate it.
    protected override OutputSpec SmallOutput => new("sink", "out", "replace", "fail_on_change",
        new Dictionary<string, object?>
        {
            ["container"] = AzuriteFixture.Container,
            ["path"] = _prefix,
            ["format"] = "parquet",
        });

    protected override OutputSpec? ReplaceOutput => new("sink", "replace-out", "replace", "fail_on_change",
        new Dictionary<string, object?>
        {
            ["container"] = AzuriteFixture.Container,
            ["path"] = _prefix,
            ["format"] = "parquet",
        });

    protected override async ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec)
    {
        var container = new BlobServiceClient(fixture.ConnectionString).GetBlobContainerClient(AzuriteFixture.Container);
        var pathPrefix = spec.Options["path"]!.ToString()!;
        var finalBlob = container.GetBlobClient($"{pathPrefix}/{spec.Output}.parquet");
        if (!(await finalBlob.ExistsAsync().ConfigureAwait(false)).Value)
        {
            return [];
        }

        var download = await finalBlob.OpenReadAsync().ConfigureAwait(false);
        await using (download.ConfigureAwait(false))
        {
            await using var reader = await ParquetReader.CreateAsync(download, leaveStreamOpen: true);
            var idField = reader.Schema.DataFields.Single(f => f.Name == "id");
            var nameField = reader.Schema.DataFields.Single(f => f.Name == "name");

            var batches = new List<RecordBatch>();
            for (var rg = 0; rg < reader.RowGroupCount; rg++)
            {
                using var rowGroup = reader.OpenRowGroupReader(rg);
                var rowCount = checked((int)rowGroup.RowCount);

                // The written DataField declares IsNullable: true regardless of the Arrow field's own
                // nullability (see AzureWriteSession/ParquetSinkWriteSession's BuildDataField), so reads
                // must go through the nullable overload even though this fixture never writes an actual
                // null.
                var ids = new long?[rowCount];
                await rowGroup.ReadAsync<long>(idField, ids);
                var names = new string[rowCount];
                await rowGroup.ReadAsync(nameField, names);

                var idBuilder = new Int64Array.Builder();
                var nameBuilder = new StringArray.Builder();
                for (var i = 0; i < rowCount; i++)
                {
                    idBuilder.Append(ids[i]!.Value);
                    nameBuilder.Append(names[i]);
                }

                batches.Add(new RecordBatch(FixedSchema, [idBuilder.Build(), nameBuilder.Build()], rowCount));
            }

            return batches;
        }
    }
}
