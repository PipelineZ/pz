using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet;
using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.LocalFiles.Tests;

/// <summary>Runs the TestKit's sink acceptance suite against the real <see cref="LocalFilesConnector"/>
/// parquet sink. Each test gets its own throwaway temp dir (xunit creates a fresh instance per [Fact]).</summary>
public sealed class LocalFilesSinkAcceptance : SinkConnectorAcceptanceTests, IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-localfiles-tests", Guid.NewGuid().ToString("N"));

    private static readonly Schema FixedSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: false),
        new Field("name", StringType.Default, nullable: false),
    ], null);

    public LocalFilesSinkAcceptance() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    protected override ISinkConnector CreateSink() => new LocalFilesConnector();

    protected override ConnectorConfig ValidConfig =>
        new(new Dictionary<string, object?> { ["base_dir"] = _dir });

    protected override OutputSpec SmallOutput => new("lake", "out", "replace", "fail_on_change",
        new Dictionary<string, object?> { ["path"] = "out", ["format"] = "parquet" });

    protected override OutputSpec? ReplaceOutput => new("lake", "replace-out", "replace", "fail_on_change",
        new Dictionary<string, object?> { ["path"] = "replace-out", ["format"] = "parquet" });

    protected override async ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec)
    {
        var relDir = spec.Options["path"]!.ToString()!;
        var path = Path.Combine(_dir, relDir, $"{spec.Output}.parquet");
        if (!File.Exists(path))
        {
            return [];
        }

        await using var reader = await ParquetReader.CreateAsync(path);
        var idField = reader.Schema.DataFields.Single(f => f.Name == "id");
        var nameField = reader.Schema.DataFields.Single(f => f.Name == "name");

        var batches = new List<RecordBatch>();
        foreach (var rowGroup in reader.RowGroups)
        {
            var rowCount = (int)rowGroup.RowCount;
            // The written DataField declares IsNullable: true regardless of the Arrow field's own
            // nullability (see ParquetSinkWriteSession.BuildDataField), so reads must go through the
            // nullable overload even though this fixture never writes an actual null.
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
