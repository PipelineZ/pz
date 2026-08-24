using System.Globalization;
using System.Net.Sockets;
using System.Runtime.Versioning;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Tests.ProcessHosting;

/// <summary>Drives <see cref="DataPlane"/> against the real out-of-process <c>PcpFakeConnector</c>
/// fixture: control-plane RPCs (via <see cref="PcpClient"/>) mint the tickets, <see cref="DataPlane"/>
/// then dials the raw <c>.data</c> socket the same way a connector-hosted <c>ISource</c>/<c>ISink</c>
/// would. Unix-only, same reasoning as <c>HandshakeTests</c>: the fixture serves unix domain sockets
/// only.</summary>
[SupportedOSPlatform("linux")]
public sealed class DataPlaneTests : IDisposable
{
    private readonly List<string> _tempDirs = [];

    [SkippableFact]
    public async Task Read_stream_round_trips_rows_and_schema()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        const int rowCount = 150;
        var dataDir = NewTempDir();
        WriteCsv(Path.Combine(dataDir, "small.csv"), rowCount);

        await using var process = ConnectorProcess.Spawn(FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp");
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dataDir });
        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);

        var spec = new DatasetSpecMsg
        {
            Source = "files",
            Dataset = "orders",
            Options = BuildStruct(new Dictionary<string, object?>
            {
                ["path"] = "small.csv",
                ["format"] = "csv",
                ["columns"] = CsvColumns,
            }),
        };

        const string opId = "read-op";
        using var planCall = client.Grpc.PlanRead(new PlanReadRequest { OpId = opId, Spec = spec });
        var partitions = new List<PartitionMsg>();
        await foreach (var partition in planCall.ResponseStream.ReadAllAsync())
        {
            partitions.Add(partition);
        }

        Assert.Single(partitions);

        // A small target keeps the fixture's real CsvArrowReader splitting the 150 rows across several
        // batches, so the round-trip proves multi-batch framing works, not just a single-message stream.
        var openResponse = await client.Grpc.OpenReadStreamAsync(new OpenReadRequest
        {
            OpId = opId,
            PartitionId = partitions[0].PartitionId,
            Options = new BatchOptionsMsg { TargetBatchBytes = 2_000 },
        });

        var batches = new List<RecordBatch>();
        try
        {
            await foreach (var batch in DataPlane.ReadStreamAsync(
                process.DataSocketPath, openResponse.Ticket.Memory, CancellationToken.None))
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
    public async Task Write_stream_round_trips_rows_and_commit_reports_them()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        var dataDir = NewTempDir();
        await using var process = ConnectorProcess.Spawn(FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp");
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dataDir });
        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);

        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("name").DataType(StringType.Default).Nullable(false))
            .Build();

        var outputSpec = new OutputSpecMsg
        {
            Sink = "lake",
            Output = "out",
            Mode = "replace",
            SchemaPolicy = "fail_on_change",
            Options = BuildStruct(new Dictionary<string, object?> { ["path"] = "out", ["format"] = "parquet" }),
        };

        const string opId = "write-op";
        var beginResponse = await client.Grpc.BeginWriteAsync(new BeginWriteRequest
        {
            OpId = opId,
            Spec = outputSpec,
            ArrowSchemaIpc = await SerializeSchemaAsync(schema),
        });

        await using (var writer = await DataPlane.OpenWriteStreamAsync(
            process.DataSocketPath, beginResponse.Ticket.Memory, schema, CancellationToken.None))
        {
            using (var batch1 = BuildBatch(schema, 0, 3))
            {
                await writer.WriteBatchAsync(batch1, CancellationToken.None);
            }

            using (var batch2 = BuildBatch(schema, 3, 2))
            {
                await writer.WriteBatchAsync(batch2, CancellationToken.None);
            }

            await writer.CompleteAsync(CancellationToken.None);
        }

        var result = await client.Grpc.CommitWriteAsync(new SessionRef { SessionId = beginResponse.SessionId });

        Assert.Equal(5, result.RowsWritten);
        Assert.Equal(2, result.BatchesWritten);
    }

    [SkippableFact]
    public async Task Bad_ticket_read_surfaces_PZ0357()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        await using var process = ConnectorProcess.Spawn(FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp");
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = NewTempDir() });
        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);

        // Never minted by the fixture -- the data-plane listener burns nothing and closes without
        // writing a byte, so no schema is ever seen.
        var badTicket = new byte[16];

        var ex = await Assert.ThrowsAsync<ConnectorHostException>(async () =>
        {
            await foreach (var batch in DataPlane.ReadStreamAsync(
                process.DataSocketPath, badTicket, CancellationToken.None))
            {
                batch.Dispose();
            }
        });

        Assert.Equal("PZ0357", ex.Code);
        Assert.Contains("schema", ex.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task Cancelled_read_connect_does_not_leak_the_socket()
    {
        Skip.If(OperatingSystem.IsWindows(), "the raw-socket peer below speaks unix domain sockets only");
        Skip.If(!Directory.Exists("/proc/self/fd"), "no /proc/self/fd fd-table introspection on this platform");

        var socketDir = NewSocketDir();
        Directory.CreateDirectory(socketDir);
        var socketPath = Path.Combine(socketDir, "data.sock");
        using var listener = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        listener.Bind(new UnixDomainSocketEndPoint(socketPath));
        listener.Listen(32);

        const int attempts = 25;
        var before = Directory.GetFiles("/proc/self/fd").Length;
        for (var i = 0; i < attempts; i++)
        {
            using var cts = new CancellationTokenSource();
            await cts.CancelAsync();

            // Pre-cancelled: connect (or the ticket write right after it) observes a token that is
            // already cancelled, which is exactly the path that used to skip `socket.Dispose()` --
            // `catch (Exception ex) when (ex is not OperationCanceledException)` never ran for a
            // cancellation. ThrowsAnyAsync (not ThrowsAsync): Socket.ConnectAsync raises the
            // OperationCanceledException subtype TaskCanceledException, and rethrowing it unwrapped --
            // "as-is" -- is the point of the fix, not normalizing it to the exact base type.
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                await foreach (var batch in DataPlane.ReadStreamAsync(socketPath, new byte[16], cts.Token))
                {
                    batch.Dispose();
                }
            });
        }

        var after = Directory.GetFiles("/proc/self/fd").Length;

        // A per-call socket leak grows this roughly linearly with `attempts`; ordinary fd churn
        // elsewhere in this (possibly parallel-running) test process is bounded noise, so a wide slack
        // still catches a real leak without the assertion being flaky.
        Assert.True(
            after - before < attempts,
            $"fd count grew from {before} to {after} over {attempts} cancelled connects -- looks like a per-call socket leak");
    }

    [Fact]
    public async Task Wrong_length_ticket_throws_ArgumentException_before_touching_the_wire()
    {
        // No socket needs to exist for this: the length check must fail fast before any I/O, so an
        // unreachable path (a socket dir with no listener) still proves the guard fired first rather
        // than the connect racing ahead of it.
        var socketPath = Path.Combine(NewSocketDir(), "data.sock");

        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await foreach (var batch in DataPlane.ReadStreamAsync(socketPath, new byte[15], CancellationToken.None))
            {
                batch.Dispose();
            }
        });
    }

    [SkippableFact]
    public async Task Truncated_stream_surfaces_as_PZ0357_not_a_clean_completion()
    {
        Skip.If(OperatingSystem.IsWindows(), "the raw-socket peer below speaks unix domain sockets only");

        // No fixture read-failure switch exists (PcpFakeConnector's argv only stages handshake/check
        // failures), so this simulates the connector side of the NORMATIVE truncation convention
        // directly: schema, one good batch, then the IPC continuation marker with a body that never
        // arrives -- the exact shape DataPlaneListener.SignalTruncatedAsync writes on a torn read.
        var socketDir = NewSocketDir();
        Directory.CreateDirectory(socketDir);
        var socketPath = Path.Combine(socketDir, "data.sock");

        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Build();

        using var listener = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        listener.Bind(new UnixDomainSocketEndPoint(socketPath));
        listener.Listen(1);

        var serverTask = Task.Run(async () =>
        {
            using var connection = await listener.AcceptAsync();
            await using var stream = new NetworkStream(connection, ownsSocket: false);
            var ticket = new byte[16];
            await ReadExactlyAsync(stream, ticket);

            using (var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true))
            {
                await writer.WriteStartAsync();
                using var goodBatch = BuildBatch(schema, 0, 2);
                await writer.WriteRecordBatchAsync(goodBatch);
            }

            // Continuation marker (0xFFFFFFFF) + a non-zero metadata length that promises a body which
            // never follows -- the ONLY truncation signal this transport has (see
            // DataPlaneListener.SignalTruncatedAsync). A plain close here would read as a clean
            // (if short) end of stream, which is exactly the silent-data-loss shape this guards against.
            await stream.WriteAsync(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0x10, 0x00, 0x00, 0x00 });
            await stream.FlushAsync();
        });

        var seen = new List<RecordBatch>();
        var ex = await Assert.ThrowsAsync<ConnectorHostException>(async () =>
        {
            await foreach (var batch in DataPlane.ReadStreamAsync(socketPath, new byte[16], CancellationToken.None))
            {
                seen.Add(batch);
            }
        });

        await serverTask;

        Assert.Equal("PZ0357", ex.Code);
        // The batch that DID arrive intact before the truncation is still surfaced (no data is
        // fabricated or dropped silently) -- but the enumeration ends in a thrown exception, never in a
        // clean `await foreach` completion, which is the "not silently yielded as if complete" half of
        // the contract.
        Assert.Single(seen);
        Assert.Equal(2, seen[0].Length);
        foreach (var batch in seen)
        {
            batch.Dispose();
        }
    }

    [SkippableFact]
    public async Task Crash_mid_stream_without_end_marker_surfaces_as_PZ0357()
    {
        Skip.If(OperatingSystem.IsWindows(), "the raw-socket peer below speaks unix domain sockets only");

        // Schema + one COMPLETE batch, then a plain close with no Arrow end-of-stream marker at all --
        // the shape a SIGKILLed connector leaves behind between two well-formed messages. Apache.Arrow's
        // reader cannot tell this apart from a legitimate WriteEndAsync-terminated stream on its own
        // (ReadNextRecordBatchAsync returns null with a non-null Schema either way, verified against
        // Apache.Arrow 23.0.0), which is exactly the silent-partial-read hazard this test pins.
        var socketDir = NewSocketDir();
        Directory.CreateDirectory(socketDir);
        var socketPath = Path.Combine(socketDir, "data.sock");

        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Build();

        using var listener = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        listener.Bind(new UnixDomainSocketEndPoint(socketPath));
        listener.Listen(1);

        var serverTask = Task.Run(async () =>
        {
            using var connection = await listener.AcceptAsync();
            await using var stream = new NetworkStream(connection, ownsSocket: false);
            var ticket = new byte[16];
            await ReadExactlyAsync(stream, ticket);

            using var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true);
            await writer.WriteStartAsync();
            using var goodBatch = BuildBatch(schema, 0, 2);
            await writer.WriteRecordBatchAsync(goodBatch);
            await stream.FlushAsync();
            // No WriteEndAsync, no truncation marker -- just stop. The connection closes (and sends its
            // FIN) when `connection`/`stream` go out of scope below.
        });

        var seen = new List<RecordBatch>();
        var ex = await Assert.ThrowsAsync<ConnectorHostException>(async () =>
        {
            await foreach (var batch in DataPlane.ReadStreamAsync(socketPath, new byte[16], CancellationToken.None))
            {
                seen.Add(batch);
            }
        });

        await serverTask;

        Assert.Equal("PZ0357", ex.Code);
        Assert.Contains("end-of-stream marker", ex.Message, StringComparison.Ordinal);
        // The one complete batch that DID arrive is still surfaced -- this never completes cleanly, but
        // it also never fabricates or silently drops the data that genuinely made it across.
        Assert.Single(seen);
        Assert.Equal(2, seen[0].Length);
        foreach (var batch in seen)
        {
            batch.Dispose();
        }
    }

    [SkippableFact]
    public async Task Proper_end_of_stream_marker_completes_cleanly()
    {
        Skip.If(OperatingSystem.IsWindows(), "the raw-socket peer below speaks unix domain sockets only");

        // The positive twin of Crash_mid_stream_without_end_marker_surfaces_as_PZ0357: the same schema
        // and one complete batch, but terminated with a real WriteEndAsync -- proves the tail-marker
        // check does not false-positive on a legitimately closed stream.
        var socketDir = NewSocketDir();
        Directory.CreateDirectory(socketDir);
        var socketPath = Path.Combine(socketDir, "data.sock");

        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Build();

        using var listener = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        listener.Bind(new UnixDomainSocketEndPoint(socketPath));
        listener.Listen(1);

        var serverTask = Task.Run(async () =>
        {
            using var connection = await listener.AcceptAsync();
            await using var stream = new NetworkStream(connection, ownsSocket: false);
            var ticket = new byte[16];
            await ReadExactlyAsync(stream, ticket);

            using var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true);
            await writer.WriteStartAsync();
            using (var goodBatch = BuildBatch(schema, 0, 2))
            {
                await writer.WriteRecordBatchAsync(goodBatch);
            }

            await writer.WriteEndAsync();
            await stream.FlushAsync();
        });

        var seen = new List<RecordBatch>();
        await foreach (var batch in DataPlane.ReadStreamAsync(socketPath, new byte[16], CancellationToken.None))
        {
            seen.Add(batch);
        }

        await serverTask;

        Assert.Single(seen);
        Assert.Equal(2, seen[0].Length);
        foreach (var batch in seen)
        {
            batch.Dispose();
        }
    }

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

    private static async Task<ByteString> SerializeSchemaAsync(Schema schema)
    {
        using var buffer = new MemoryStream();
        using (var writer = new ArrowStreamWriter(buffer, schema, leaveOpen: true))
        {
            await writer.WriteStartAsync();
            await writer.WriteEndAsync();
        }

        return ByteString.CopyFrom(buffer.ToArray());
    }

    private static async Task ReadExactlyAsync(Stream stream, byte[] buffer)
    {
        var read = 0;
        while (read < buffer.Length)
        {
            var n = await stream.ReadAsync(buffer.AsMemory(read));
            if (n == 0)
            {
                throw new EndOfStreamException();
            }

            read += n;
        }
    }

    /// <summary>Config crosses only through the Configure RPC's <c>google.protobuf.Struct</c>; mirrors
    /// <c>PcpClient.ToStruct</c>'s shape (string, bool, numeric, null, nested map) since that is the only
    /// mapping the fixture's other end (<c>StructMapping.ToDictionary</c>) understands.</summary>
    private static Struct BuildStruct(IReadOnlyDictionary<string, object?> values)
    {
        var result = new Struct();
        foreach (var (key, value) in values)
        {
            result.Fields[key] = ToValue(value);
        }

        return result;
    }

    private static Value ToValue(object? value) => value switch
    {
        null => Value.ForNull(),
        bool b => Value.ForBool(b),
        string s => Value.ForString(s),
        IReadOnlyDictionary<string, string> strings => Value.ForStruct(
            BuildStruct(strings.ToDictionary(pair => pair.Key, object? (pair) => pair.Value, StringComparer.Ordinal))),
        _ => Value.ForString(value.ToString() ?? string.Empty),
    };

    private static ConnectorManifest LocalFilesManifest() => new(
        Name: "localfiles-pcp",
        ProtocolMajorMin: ProtocolVersion.Major,
        ProtocolMajorMax: ProtocolVersion.Major,
        Capabilities: new LocalFilesConnector().Capabilities.ToString()
            .Split(", ", StringSplitOptions.RemoveEmptyEntries));

    /// <summary>Mirrors <c>HandshakeTests.FixtureExecutablePath</c>: the fixture builds to its own bin
    /// dir, a sibling of this test project's under <c>tests/</c>, resolved relative to
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
    /// bytes (<c>sun_path</c>), and the deep <c>tests/.../bin/Release/net10.0/...</c> tree this assembly
    /// lives under leaves no room for <c>control.sock</c>/<c>control.sock.data</c> on top.</summary>
    private string NewSocketDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-" + Guid.NewGuid().ToString("N")[..8]);
        _tempDirs.Add(dir);
        return dir;
    }

    private string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-dp-" + Guid.NewGuid().ToString("N")[..8]);
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
