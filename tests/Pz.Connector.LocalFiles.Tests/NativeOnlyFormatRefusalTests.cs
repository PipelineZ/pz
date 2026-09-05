using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.LocalFiles.Tests;

/// <summary>Native-only-format refusals that never touch DuckDB or an extension -- they throw
/// synchronously from format resolution/validation before any scan/copy runs -- so unlike
/// <see cref="ExtensionFormatTests"/> these carry no <c>DuckDbExtension</c> trait and need no
/// docker/network skip: they run in every offline suite.</summary>
public sealed class NativeOnlyFormatRefusalTests
{
    private static ConnectorConfig Config => new(new Dictionary<string, object?>());

    [Fact]
    public async Task Avro_sink_is_PZ0361_read_only()
    {
        await using var sink = await ((ISinkConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var spec = new OutputSpec("files", "users", "replace", "fail_on_change", new Dictionary<string, object?> { ["format"] = "avro" });
        var ex = Assert.Throws<PzConnectorException>(() => sink.TryGetNativeCopy(spec, out _));
        Assert.Equal("PZ0361: output 'users': format 'avro' is read-only on localfiles -- write parquet, csv or json instead", ex.Message);
    }

    [Fact]
    public async Task Xlsx_universal_write_and_read_are_PZ0361()
    {
        await using var sink = await ((ISinkConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var outSpec = new OutputSpec("files", "people", "replace", "fail_on_change", new Dictionary<string, object?> { ["format"] = "xlsx" });
        var schema = new Schema([new Field("id", Int64Type.Default, true)], null);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await sink.BeginWriteAsync(outSpec, schema, CancellationToken.None));
        Assert.StartsWith("PZ0361: output 'people': format 'xlsx' is native-only", ex.Message, StringComparison.Ordinal);

        await using var source = await ((ISourceConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var spec = new DatasetSpec("files", "people", new Dictionary<string, object?> { ["format"] = "xlsx" });
        var ex2 = await Assert.ThrowsAsync<PzConnectorException>(async () => await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));
        Assert.StartsWith("PZ0312: dataset 'people': localfiles xlsx source is native-scan only", ex2.Message, StringComparison.Ordinal);
    }

    /// <summary>Pins that the localfiles xlsx COPY path is unaffected by the remote-write refusal
    /// (<c>FileFormatCatalog.EnsureRemoteWritable</c>): localfiles is the one connector xlsx write is
    /// allowed on, and <see cref="LocalFilesSink.TryGetNativeCopy"/> never calls that guard. A pure,
    /// offline probe -- no DuckDB session runs; the real xlsx COPY round-trip is
    /// <see cref="ExtensionFormatTests.Xlsx_native_copy_then_native_scan_roundtrips_with_sheet"/>.</summary>
    [Fact]
    public async Task Xlsx_native_copy_is_unaffected_by_the_remote_write_refusal()
    {
        await using var sink = await ((ISinkConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var spec = new OutputSpec("files", "people", "replace", "fail_on_change", new Dictionary<string, object?> { ["format"] = "xlsx" });
        Assert.True(sink.TryGetNativeCopy(spec, out var copy));
        Assert.Equal(["install excel", "load excel"], copy!.SetupStatements);
        Assert.Contains("(format xlsx, header true)", copy.CopySql, StringComparison.Ordinal);
    }

    /// <summary>Pins <c>NativeOnlySource</c>'s exact schema-fetch wording for xlsx and avro without a
    /// declared <c>columns:</c> contract -- each names the format and its own reason none of the three
    /// formats gives schema fetch anything to infer a schema from. A pure, offline probe: the file
    /// exists (so this exercises the contract check, not the not-found path) but is never opened -- the
    /// contract is checked before any bytes are read.</summary>
    [Theory]
    [InlineData("xlsx", "a workbook's header row names columns but not their types")]
    [InlineData("avro", "avro's embedded schema is not read here -- schema fetch never opens the file bytes")]
    public async Task GetSchemaAsync_without_a_contract_names_the_format_and_reason(string format, string reason)
    {
        var dir = Directory.CreateTempSubdirectory("pz-localfiles-nativeonly-schema-");
        try
        {
            File.WriteAllBytes(Path.Combine(dir.FullName, $"events.{format}"), []);
            var config = new ConnectorConfig(new Dictionary<string, object?> { ["base_dir"] = dir.FullName });

            await using var source = await ((ISourceConnector)new LocalFilesConnector()).OpenAsync(config, CancellationToken.None);
            var spec = new DatasetSpec("files", "events", new Dictionary<string, object?>
            {
                ["path"] = $"events.{format}",
                ["format"] = format,
            });

            var ex = await Assert.ThrowsAsync<PzConnectorException>(
                async () => await source.GetSchemaAsync(spec, CancellationToken.None));

            Assert.False(ex.IsTransient);
            Assert.StartsWith(
                $"dataset 'events': localfiles {format} requires a declared columns: contract for schema fetch -- {reason}",
                ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            try { dir.Delete(recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
