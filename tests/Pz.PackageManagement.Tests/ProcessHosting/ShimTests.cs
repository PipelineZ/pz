using System.Globalization;
using System.Runtime.Versioning;
using Apache.Arrow;
using Apache.Arrow.Types;
using Google.Protobuf;
using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Tests.ProcessHosting;

/// <summary>Drives <see cref="ProcessSourceConnector"/>/<see cref="ProcessSinkConnector"/> against the
/// real out-of-process <c>PcpFakeConnector</c> fixture exactly as the engine would: through the ABI
/// interfaces (<see cref="ISourceConnector"/>/<see cref="ISinkConnector"/>), never the raw
/// <see cref="PcpClient.Grpc"/> client <see cref="DataPlaneTests"/> and <see cref="HandshakeTests"/>
/// use -- this is the proof that the shim, not just the wire underneath it, behaves like an in-process
/// connector. Unix-only, same reasoning as its siblings: the fixture serves unix domain sockets
/// only.</summary>
[SupportedOSPlatform("linux")]
public sealed class ShimTests : IDisposable
{
    private readonly List<string> _tempDirs = [];

    // ---- Step 1: read path, through the shim end to end -------------------------------------

    [SkippableFact]
    public async Task Read_path_round_trips_schema_and_rows_through_the_shim()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        const int rowCount = 150;
        var dataDir = NewTempDir();
        WriteCsv(Path.Combine(dataDir, "small.csv"), rowCount);

        await using var process = ConnectorProcess.Spawn(FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp");
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dataDir });
        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);

        var connector = new ProcessSourceConnector(client, process);
        Assert.Equal("localfiles-pcp", connector.Info.Name);
        Assert.Equal((long)new LocalFilesConnector().Capabilities, (long)connector.Capabilities);

        await using var source = await connector.OpenAsync(config, CancellationToken.None);

        var spec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
        {
            ["path"] = "small.csv",
            ["format"] = "csv",
            ["columns"] = CsvColumns,
        });

        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);
        Assert.Equal(CsvColumns.Keys, schema.Schema.FieldsList.Select(f => f.Name));

        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var partition = Assert.Single(partitions);

        var batches = new List<RecordBatch>();
        try
        {
            // A small target batch keeps the fixture's real CsvArrowReader splitting rows across
            // several batches, so this proves multi-batch framing works end to end through the shim.
            await foreach (var batch in partition.ReadAsync(
                new BatchOptions(TargetBatchBytes: 2_000), CancellationToken.None))
            {
                batches.Add(batch);
            }

            Assert.True(batches.Count > 1, $"expected a multi-batch stream, got {batches.Count} batch(es)");
            Assert.Equal(rowCount, batches.Sum(b => b.Length));
            foreach (var batch in batches)
            {
                Assert.Equal(CsvColumns.Keys, batch.Schema.FieldsList.Select(f => f.Name));
            }
        }
        finally
        {
            foreach (var batch in batches)
            {
                batch.Dispose();
            }
        }
    }

    [SkippableFact]
    public async Task Native_scan_probe_round_trips_through_the_shim()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        var dataDir = NewTempDir();
        WriteCsv(Path.Combine(dataDir, "small.csv"), 5);

        await using var process = ConnectorProcess.Spawn(FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp");
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dataDir });
        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);

        var connector = new ProcessSourceConnector(client, process);
        await using var source = await connector.OpenAsync(config, CancellationToken.None);

        var spec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
        {
            ["path"] = "small.csv",
            ["format"] = "csv",
            ["columns"] = CsvColumns,
        });

        // ISource.TryGetNativeScan has no CancellationToken -- this is the synchronous,
        // .GetAwaiter().GetResult()-under-a-deadline path documented on ProcessSource.TryGetNativeScan.
        var found = source.TryGetNativeScan(spec, out var scan);

        Assert.True(found);
        Assert.NotNull(scan);
        Assert.False(scan.SchemaInferred, "a declared columns: contract is not an inferred schema");
        Assert.Contains("small.csv", scan.SqlFragment, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task Capabilities_the_shims_do_not_implement_are_masked_out_of_their_surface()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        // Manifest and Hello AGREE that the connector has CheckpointableReads, so the handshake's own
        // set-equality gate is satisfied -- this is not a misdeclaration. The shims still must not
        // surface it: nothing here implements ICheckpointingPartition, and the planner would accept a
        // checkpointed dataset and silently get a plain full read.
        var declared = new LocalFilesConnector().Capabilities | ConnectorCapabilities.CheckpointableReads;
        await using var process = ConnectorProcess.Spawn(
            FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp", ["--declare-checkpointable-reads"]);
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = NewTempDir() });
        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(declared), "test-instance", config, CancellationToken.None);

        // The connector really did claim it -- so the mask is the HOST's decision, not the connector
        // failing to report the flag.
        Assert.Equal(
            (long)ConnectorCapabilities.CheckpointableReads,
            client.Hello.Capabilities & (long)ConnectorCapabilities.CheckpointableReads);

        var sourceConnector = new ProcessSourceConnector(client, process);
        var sinkConnector = new ProcessSinkConnector(client, process);
        Assert.Equal(
            ConnectorCapabilities.None,
            sourceConnector.Capabilities & ConnectorCapabilities.CheckpointableReads);
        Assert.Equal(
            ConnectorCapabilities.None,
            sinkConnector.Capabilities & ConnectorCapabilities.CheckpointableReads);

        // Everything the connector legitimately declares still crosses untouched.
        Assert.Equal(new LocalFilesConnector().Capabilities, sourceConnector.Capabilities);
    }

    // ---- Step 1: write path, through the shim end to end -------------------------------------

    [SkippableFact]
    public async Task Write_path_commits_two_batches_through_the_shim()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        var dataDir = NewTempDir();
        await using var process = ConnectorProcess.Spawn(FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp");
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dataDir });
        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);

        var connector = new ProcessSinkConnector(client, process);
        await using var sink = await connector.OpenAsync(config, CancellationToken.None);

        var schema = BuildSchema();
        var outputSpec = new OutputSpec("lake", "out", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "out", ["format"] = "parquet" });

        await using (var session = await sink.BeginWriteAsync(outputSpec, schema, CancellationToken.None))
        {
            // AbortSemantics is DiscardsAll for a temp-write-and-swap owned destination (LocalFiles):
            // the wrapped sink's own declaration must have crossed the wire, not a hardcoded default.
            Assert.Equal(AbortSemantics.DiscardsAll, sink.AbortSemantics);

            using (var batch1 = BuildBatch(schema, 0, 3))
            {
                await session.WriteBatchAsync(batch1, CancellationToken.None);
            }

            using (var batch2 = BuildBatch(schema, 3, 2))
            {
                await session.WriteBatchAsync(batch2, CancellationToken.None);
            }

            var result = await session.CommitAsync(CancellationToken.None);
            Assert.Equal(5, result.RowsWritten);
            Assert.Equal(2, result.BatchesWritten);
        }

        // Same path Abort_path_leaves_no_destination_file asserts absent -- BeginWriteAsync creates the
        // parent `out/` directory itself before any row is written, so asserting the directory alone
        // would pass for an aborted write too. The FILE is what committing (vs. aborting) actually
        // decides.
        Assert.True(
            File.Exists(Path.Combine(dataDir, "out", "out.parquet")), "committed write should have landed a file");
    }

    [SkippableFact]
    public async Task Abort_path_leaves_no_destination_file()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        var dataDir = NewTempDir();
        await using var process = ConnectorProcess.Spawn(FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp");
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dataDir });
        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);

        var connector = new ProcessSinkConnector(client, process);
        await using var sink = await connector.OpenAsync(config, CancellationToken.None);

        var schema = BuildSchema();
        var outputSpec = new OutputSpec("lake", "out", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "out", ["format"] = "parquet" });

        await using (var session = await sink.BeginWriteAsync(outputSpec, schema, CancellationToken.None))
        {
            using (var batch = BuildBatch(schema, 0, 3))
            {
                await session.WriteBatchAsync(batch, CancellationToken.None);
            }

            await session.AbortAsync(CancellationToken.None);
        }

        // LocalFilesSink.BeginWriteAsync creates the OUTPUT directory itself before any row is
        // written (a precondition for the temp-dir-under-it it stages into) -- DiscardsAll is about
        // the destination FILE never appearing, not about that empty parent directory going away.
        Assert.False(
            File.Exists(Path.Combine(dataDir, "out", "out.parquet")),
            "an aborted replace write must leave no destination file behind (DiscardsAll)");
    }

    // ---- Step 1: process death mid-read is a transient PzConnectorException ------------------

    [SkippableFact]
    public async Task Killing_the_process_mid_read_surfaces_a_transient_PzConnectorException()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        var dataDir = NewTempDir();
        WriteCsv(Path.Combine(dataDir, "big.csv"), 20_000);

        var process = ConnectorProcess.Spawn(FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp");
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dataDir });
        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);

        var connector = new ProcessSourceConnector(client, process);
        await using var source = await connector.OpenAsync(config, CancellationToken.None);

        var spec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
        {
            ["path"] = "big.csv",
            ["format"] = "csv",
            ["columns"] = CsvColumns,
        });

        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var partition = Assert.Single(partitions);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            var sawOneBatch = false;
            await foreach (var batch in partition.ReadAsync(
                new BatchOptions(TargetBatchBytes: 2_000), CancellationToken.None))
            {
                batch.Dispose();
                if (!sawOneBatch)
                {
                    sawOneBatch = true;
                    // Kill the connector process while the read is still in progress -- this is the
                    // scenario layer 1 (DataPlane's own EOS-marker check) exists for, surfaced through
                    // the shim's ABI-mandated PzConnectorException rather than DataPlane's internal
                    // ConnectorHostException.
                    await process.DisposeAsync();
                }
            }
        });

        Assert.True(ex.IsTransient, "a mid-read process death should retry, not permanently fail the node");
        // A SIGKILLed process rarely has a chance to write anything to stderr before it dies, so this
        // only asserts the append actually happened when there was something to append -- the code path
        // that matters (ProcessFailureMapping) is exercised either way.
        if (process.StderrTail.Length > 0)
        {
            Assert.Contains(process.StderrTail, ex.Message, StringComparison.Ordinal);
        }

        await process.DisposeAsync();
    }

    [SkippableFact]
    public async Task Killing_the_process_mid_write_surfaces_a_transient_PzConnectorException()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        var dataDir = NewTempDir();
        var process = ConnectorProcess.Spawn(FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp");
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dataDir });
        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);

        var connector = new ProcessSinkConnector(client, process);
        await using var sink = await connector.OpenAsync(config, CancellationToken.None);

        var schema = BuildSchema();
        var outputSpec = new OutputSpec("lake", "out", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "out", ["format"] = "parquet" });

        var session = await sink.BeginWriteAsync(outputSpec, schema, CancellationToken.None);
        try
        {
            using (var batch1 = BuildBatch(schema, 0, 3))
            {
                await session.WriteBatchAsync(batch1, CancellationToken.None);
            }

            // Kill the connector process, then keep writing: WriteBatchAsync forwards straight to
            // DataPlaneWriter, which wraps nothing of its own, so the write side of the crash-detection
            // gap (mirrors Killing_the_process_mid_read_... above) is a raw broken-pipe
            // IOException/SocketException off the socket rather than any PCP-level exception -- a
            // handful of attempts covers the write that lands after the peer's fd is actually gone
            // rather than merely buffered by the OS.
            await process.DisposeAsync();

            var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            {
                for (var i = 0; i < 5; i++)
                {
                    using var batch = BuildBatch(schema, 3 + i, 1);
                    await session.WriteBatchAsync(batch, CancellationToken.None);
                }
            });

            Assert.True(ex.IsTransient, "a mid-write process death should retry, not permanently fail the node");
        }
        finally
        {
            // Local-only cleanup (no RPC): safe to run against an already-dead connector.
            await session.DisposeAsync();
        }
    }

    // ---- Step 1/7: AbortSemantics genuinely crosses the wire, not the shim's own default -----

    [SkippableFact]
    public async Task AbortSemantics_surfaces_the_connectors_reported_value_not_the_shims_default()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        var dataDir = NewTempDir();
        // The wrapped LocalFilesConnector is always DiscardsAll -- the same value ProcessSink defaults
        // to before any session opens -- so asserting DiscardsAll after BeginWriteAsync can't tell "the
        // field crossed the wire" apart from "the shim's hardcoded default was never overwritten". This
        // fixture switch reports AbortSemantics.None instead (unrelated to what LocalFiles actually is),
        // so only a genuine wire round trip can make the assertion below pass.
        await using var process = ConnectorProcess.Spawn(
            FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp", ["--report-abort-semantics-none"]);
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dataDir });
        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);

        var connector = new ProcessSinkConnector(client, process);
        await using var sink = await connector.OpenAsync(config, CancellationToken.None);

        Assert.Equal(AbortSemantics.DiscardsAll, sink.AbortSemantics);

        var schema = BuildSchema();
        var outputSpec = new OutputSpec("lake", "out", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "out", ["format"] = "parquet" });
        await using var session = await sink.BeginWriteAsync(outputSpec, schema, CancellationToken.None);

        Assert.Equal(AbortSemantics.None, sink.AbortSemantics);

        await session.AbortAsync(CancellationToken.None);
    }

    // ---- Step 4: MessageMapping round trips, both directions, every record -------------------

    // NOTE ON THESE ASSERTIONS: a record's generated Equals compares an IReadOnlyList</IReadOnlyDictionary
    // -typed property by reference (neither interface overrides Equals), so `Assert.Equal(original,
    // roundTripped)` on the WHOLE record would pass only by luck (two empty collections can share the
    // same cached instance) and fail on any non-empty one even when the contents genuinely match. Every
    // record below that carries a list/dictionary property is therefore asserted field by field, using
    // xunit's own Assert.Equal on each collection (which DOES do element-wise/deep comparison) instead
    // of relying on the record's Equals for those fields.

    [Fact]
    public void DatasetSpec_round_trips_null_heavy()
    {
        var spec = new DatasetSpec("src", "ds", new Dictionary<string, object?> { ["a"] = 1.0 });
        var roundTripped = MessageMapping.ToDatasetSpec(MessageMapping.ToDatasetSpecMsg(spec));
        AssertDatasetSpecEqual(spec, roundTripped);
    }

    [Fact]
    public void DatasetSpec_round_trips_full_featured()
    {
        var spec = new DatasetSpec("src", "ds", new Dictionary<string, object?> { ["a"] = 1.0 })
        {
            WatermarkCursor = "updated_at",
            WatermarkValue = "2026-01-01T00:00:00.000000",
            WatermarkUpperBound = "2026-01-02T00:00:00.000000",
            WatermarkLowerInclusive = true,
            PriorSyncState = "opaque-token",
            ChangeCapture = true,
            ChangeCaptureSlot = "slot-1",
        };
        var roundTripped = MessageMapping.ToDatasetSpec(MessageMapping.ToDatasetSpecMsg(spec));
        AssertDatasetSpecEqual(spec, roundTripped);
    }

    private static void AssertDatasetSpecEqual(DatasetSpec expected, DatasetSpec actual)
    {
        Assert.Equal(expected.Source, actual.Source);
        Assert.Equal(expected.Dataset, actual.Dataset);
        Assert.Equal(expected.Options, actual.Options);
        Assert.Equal(expected.WatermarkCursor, actual.WatermarkCursor);
        Assert.Equal(expected.WatermarkValue, actual.WatermarkValue);
        Assert.Equal(expected.WatermarkUpperBound, actual.WatermarkUpperBound);
        Assert.Equal(expected.WatermarkLowerInclusive, actual.WatermarkLowerInclusive);
        Assert.Equal(expected.PriorSyncState, actual.PriorSyncState);
        Assert.Equal(expected.ChangeCapture, actual.ChangeCapture);
        Assert.Equal(expected.ChangeCaptureSlot, actual.ChangeCaptureSlot);
    }

    [Fact]
    public void OutputSpec_round_trips_null_heavy()
    {
        var spec = new OutputSpec("sink", "out", "append", "fail_on_change", new Dictionary<string, object?>());
        var roundTripped = MessageMapping.ToOutputSpec(MessageMapping.ToOutputSpecMsg(spec));
        AssertOutputSpecEqual(spec, roundTripped);
        Assert.Empty(roundTripped.Keys);
        Assert.Null(roundTripped.MaxTextLengths);
    }

    [Fact]
    public void OutputSpec_round_trips_full_featured()
    {
        var spec = new OutputSpec("sink", "out", "merge", "fail_on_change", new Dictionary<string, object?> { ["path"] = "x" })
        {
            Keys = ["id", "tenant"],
            OnDelete = "soft",
            MaxTextLengths = new Dictionary<string, long> { ["name"] = 255, ["notes"] = 4000 },
            Attempt = new WriteAttempt("node-1", "run-1", 3),
        };
        var roundTripped = MessageMapping.ToOutputSpec(MessageMapping.ToOutputSpecMsg(spec));
        AssertOutputSpecEqual(spec, roundTripped);
    }

    private static void AssertOutputSpecEqual(OutputSpec expected, OutputSpec actual)
    {
        Assert.Equal(expected.Sink, actual.Sink);
        Assert.Equal(expected.Output, actual.Output);
        Assert.Equal(expected.Mode, actual.Mode);
        Assert.Equal(expected.SchemaPolicy, actual.SchemaPolicy);
        Assert.Equal(expected.Options, actual.Options);
        Assert.Equal(expected.Keys, actual.Keys);
        Assert.Equal(expected.OnDelete, actual.OnDelete);
        Assert.Equal(expected.MaxTextLengths, actual.MaxTextLengths);
        Assert.Equal(expected.Attempt, actual.Attempt);
    }

    [Fact]
    public void ReadHints_round_trips_none()
    {
        var roundTripped = MessageMapping.ToReadHints(MessageMapping.ToReadHintsMsg(ReadHints.None));
        Assert.Null(roundTripped.Columns);
        Assert.Null(roundTripped.PredicateSql);
        Assert.Null(roundTripped.Limit);
    }

    [Fact]
    public void ReadHints_round_trips_full_featured()
    {
        var hints = new ReadHints(["id", "name"], "id > 10", 500);
        var roundTripped = MessageMapping.ToReadHints(MessageMapping.ToReadHintsMsg(hints));
        Assert.Equal(hints.Columns, roundTripped.Columns);
        Assert.Equal(hints.PredicateSql, roundTripped.PredicateSql);
        Assert.Equal(hints.Limit, roundTripped.Limit);
    }

    [Fact]
    public void ReadHints_distinguishes_null_columns_from_empty_columns()
    {
        var empty = new ReadHints([], null, null);
        var roundTrippedEmpty = MessageMapping.ToReadHints(MessageMapping.ToReadHintsMsg(empty));
        Assert.NotNull(roundTrippedEmpty.Columns);
        Assert.Empty(roundTrippedEmpty.Columns);

        var none = new ReadHints(null, null, null);
        var roundTrippedNone = MessageMapping.ToReadHints(MessageMapping.ToReadHintsMsg(none));
        Assert.Null(roundTrippedNone.Columns);
    }

    [Fact]
    public void BatchOptions_round_trips()
    {
        var options = new BatchOptions(4096, 1000);
        var roundTripped = MessageMapping.ToBatchOptions(MessageMapping.ToBatchOptionsMsg(options));
        Assert.Equal(options, roundTripped);
    }

    [Fact]
    public void WriteAttempt_round_trips()
    {
        var attempt = new WriteAttempt("node", "run", 7);
        var roundTripped = MessageMapping.ToWriteAttempt(MessageMapping.ToWriteAttemptMsg(attempt));
        Assert.Equal(attempt, roundTripped);
    }

    [Fact]
    public void ConnectorInfo_round_trips()
    {
        var info = new ConnectorInfo("my-connector", "1.2.3", ProtocolVersion.Major);
        var roundTripped = MessageMapping.ToConnectorInfo(MessageMapping.ToConnectorInfoMsg(info));
        Assert.Equal(info, roundTripped);
    }

    [Fact]
    public void ValidationResult_round_trips_empty_and_populated()
    {
        var empty = ValidationResult.Success;
        var roundTrippedEmpty = MessageMapping.ToValidationResult(MessageMapping.ToValidationResultMsg(empty));
        Assert.Equal(empty.Errors, roundTrippedEmpty.Errors);
        Assert.True(roundTrippedEmpty.IsValid);

        var failed = ValidationResult.Failed("bad root", "missing format");
        var roundTrippedFailed = MessageMapping.ToValidationResult(MessageMapping.ToValidationResultMsg(failed));
        Assert.Equal(failed.Errors, roundTrippedFailed.Errors);
    }

    [Fact]
    public void ConnectionCheck_round_trips_null_and_populated_message()
    {
        var okNoMessage = new ConnectionCheck(true);
        Assert.Equal(okNoMessage, MessageMapping.ToConnectionCheck(MessageMapping.ToConnectionCheckMsg(okNoMessage)));

        var failedWithMessage = new ConnectionCheck(false, "connection refused");
        Assert.Equal(
            failedWithMessage, MessageMapping.ToConnectionCheck(MessageMapping.ToConnectionCheckMsg(failedWithMessage)));
    }

    [Fact]
    public void NativeScan_round_trips_null_heavy()
    {
        var scan = new NativeScan("select * from t", []);
        var roundTripped = MessageMapping.ToNativeScan(MessageMapping.ToNativeScanResponse(scan));
        AssertNativeScanEqual(scan, roundTripped);
        Assert.Null(roundTripped.Mechanism);
        Assert.Null(roundTripped.SniffFragment);
    }

    [Fact]
    public void NativeScan_round_trips_full_featured()
    {
        var scan = new NativeScan("select * from read_csv('x')", ["SET x=1"])
        {
            Mechanism = "read_csv",
            SchemaInferred = true,
            SniffFragment = "sniff_csv('x')",
        };
        var roundTripped = MessageMapping.ToNativeScan(MessageMapping.ToNativeScanResponse(scan));
        AssertNativeScanEqual(scan, roundTripped);
    }

    private static void AssertNativeScanEqual(NativeScan expected, NativeScan actual)
    {
        Assert.Equal(expected.SqlFragment, actual.SqlFragment);
        Assert.Equal(expected.SetupStatements, actual.SetupStatements);
        Assert.Equal(expected.Mechanism, actual.Mechanism);
        Assert.Equal(expected.SchemaInferred, actual.SchemaInferred);
        Assert.Equal(expected.SniffFragment, actual.SniffFragment);
    }

    [Fact]
    public void NativeCopy_round_trips_null_heavy()
    {
        var copy = new NativeCopy("copy t to 'x'", []);
        var roundTripped = MessageMapping.ToNativeCopy(MessageMapping.ToNativeCopyResponse(copy));
        AssertNativeCopyEqual(copy, roundTripped);
        Assert.Null(roundTripped.Mechanism);
        Assert.Empty(roundTripped.Finalizations);
    }

    [Fact]
    public void NativeCopy_round_trips_full_featured()
    {
        var copy = new NativeCopy("copy t to 'x.tmp'", ["SET y=1"])
        {
            Mechanism = "COPY ... (FORMAT parquet)",
            Finalizations = [new FileMove("x.tmp", "x")],
        };
        var roundTripped = MessageMapping.ToNativeCopy(MessageMapping.ToNativeCopyResponse(copy));
        AssertNativeCopyEqual(copy, roundTripped);
    }

    private static void AssertNativeCopyEqual(NativeCopy expected, NativeCopy actual)
    {
        Assert.Equal(expected.CopySql, actual.CopySql);
        Assert.Equal(expected.SetupStatements, actual.SetupStatements);
        Assert.Equal(expected.Mechanism, actual.Mechanism);
        Assert.Equal(expected.Finalizations, actual.Finalizations);
    }

    [Fact]
    public void FileMove_round_trips()
    {
        var move = new FileMove("a.tmp", "a");
        var roundTripped = MessageMapping.ToFileMove(MessageMapping.ToFileMoveMsg(move));
        Assert.Equal(move, roundTripped);
    }

    [Fact]
    public void WriteResult_round_trips()
    {
        var result = new WriteResult(42, 3);
        var roundTripped = MessageMapping.ToWriteResult(MessageMapping.ToWriteResultMsg(result));
        Assert.Equal(result, roundTripped);
    }

    [Theory]
    [InlineData(AbortSemantics.DiscardsAll)]
    [InlineData(AbortSemantics.BestEffort)]
    [InlineData(AbortSemantics.None)]
    public void AbortSemantics_round_trips(AbortSemantics semantics)
    {
        var roundTripped = MessageMapping.ToAbortSemantics(MessageMapping.ToAbortSemanticsMsg(semantics));
        Assert.Equal(semantics, roundTripped);
    }

    [Fact]
    public void Struct_round_trips_every_value_kind()
    {
        var values = new Dictionary<string, object?>
        {
            ["s"] = "hello",
            ["n"] = 3.5,
            ["b"] = true,
            ["nil"] = null,
            ["list"] = new List<object?> { 1.0, "x", false, null },
            ["map"] = new Dictionary<string, object?> { ["nested"] = "value", ["deep"] = 2.0 },
        };

        var roundTripped = MessageMapping.ToDictionary(MessageMapping.ToStruct(values));

        Assert.Equal("hello", roundTripped["s"]);
        Assert.Equal(3.5, roundTripped["n"]);
        Assert.Equal(true, roundTripped["b"]);
        Assert.Null(roundTripped["nil"]);
        Assert.Equal(new List<object?> { 1.0, "x", false, null }, roundTripped["list"]);
        var nested = Assert.IsAssignableFrom<IReadOnlyDictionary<string, object?>>(roundTripped["map"]);
        Assert.Equal("value", nested["nested"]);
        Assert.Equal(2.0, nested["deep"]);
    }

    [Fact]
    public void Struct_round_trips_an_all_string_nested_map_preserving_insertion_order_and_string_view()
    {
        // `columns:` contracts bind to the csv header BY POSITION -- order is load-bearing, and the
        // rebuilt map must still answer to IReadOnlyDictionary<string, string> the way LocalFiles reads
        // it, not just IReadOnlyDictionary<string, object?>.
        var columns = new Dictionary<string, object?>
        {
            ["options"] = new Dictionary<string, object?>
            {
                ["z_first"] = "bigint",
                ["a_second"] = "varchar",
                ["m_third"] = "double",
            },
        };

        var roundTripped = MessageMapping.ToDictionary(MessageMapping.ToStruct(columns));
        var nested = Assert.IsAssignableFrom<IReadOnlyDictionary<string, object?>>(roundTripped["options"]);
        Assert.Equal(["z_first", "a_second", "m_third"], nested.Keys);

        var asStrings = Assert.IsAssignableFrom<IReadOnlyDictionary<string, string>>(nested);
        Assert.Equal("bigint", asStrings["z_first"]);
        Assert.Equal(["z_first", "a_second", "m_third"], asStrings.Keys);
    }

    // ---- shared fixtures ------------------------------------------------------------------------

    private static readonly Dictionary<string, string> CsvColumns = new()
    {
        ["id"] = "bigint",
        ["name"] = "varchar",
        ["amount"] = "double",
        ["flag"] = "boolean",
        ["created"] = "timestamp",
    };

    private static void WriteCsv(string path, int rows)
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

    private static Schema BuildSchema() => new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("name").DataType(StringType.Default).Nullable(false))
        .Build();

    private static RecordBatch BuildBatch(Schema schema, int startId, int rows)
    {
        var idBuilder = new Int64Array.Builder();
        var nameBuilder = new StringArray.Builder();
        for (var i = 0; i < rows; i++)
        {
            idBuilder.Append(startId + i);
            nameBuilder.Append($"row-{startId + i}");
        }

        return new RecordBatch(schema, [idBuilder.Build(), nameBuilder.Build()], rows);
    }

    private static ConnectorManifest LocalFilesManifest(ConnectorCapabilities? capabilities = null) => new(
        Name: "localfiles-pcp",
        ProtocolMajorMin: ProtocolVersion.Major,
        ProtocolMajorMax: ProtocolVersion.Major,
        Capabilities: (capabilities ?? new LocalFilesConnector().Capabilities).ToString()
            .Split(", ", StringSplitOptions.RemoveEmptyEntries));

    /// <summary>Mirrors HandshakeTests/DataPlaneTests: the fixture builds to its own bin dir, a sibling
    /// of this test project's under <c>tests/</c>, resolved relative to
    /// <see cref="AppContext.BaseDirectory"/> so it tracks whichever configuration actually ran.</summary>
    private static string FixtureExecutablePath()
    {
        var baseDir = new DirectoryInfo(AppContext.BaseDirectory);
        var tfm = baseDir.Name;
        var config = baseDir.Parent!.Name;
        var testsDir = baseDir.Parent!.Parent!.Parent!.Parent!.FullName;
        var exeName = OperatingSystem.IsWindows() ? "PcpFakeConnector.exe" : "PcpFakeConnector";
        return Path.Combine(testsDir, "fixtures", "PcpFakeConnector", "bin", config, tfm, exeName);
    }

    /// <summary>Short, outside the test output tree: a unix domain socket path is capped at roughly 104
    /// bytes (<c>sun_path</c>), and the deep <c>tests/.../bin/Release/net10.0/...</c> tree this
    /// assembly lives under leaves no room for <c>control.sock</c>/<c>control.sock.data</c> on top.</summary>
    private string NewSocketDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-shim-" + Guid.NewGuid().ToString("N")[..8]);
        _tempDirs.Add(dir);
        return dir;
    }

    private string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-shim-data-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        _tempDirs.Add(dir);
        return dir;
    }

    public void Dispose()
    {
        foreach (var dir in _tempDirs)
        {
            try
            {
                if (Directory.Exists(dir))
                {
                    Directory.Delete(dir, recursive: true);
                }
            }
            catch
            {
                // best-effort cleanup
            }
        }
    }
}
