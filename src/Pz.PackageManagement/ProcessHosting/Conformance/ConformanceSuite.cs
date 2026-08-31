using System.Security.Cryptography;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Grpc.Core;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol;
using Pz.Connectors.Protocol.V1;
using Pz.PackageManagement.Hosting;

namespace Pz.PackageManagement.ProcessHosting.Conformance;

/// <summary>What <see cref="ConformanceSuite.RunAsync"/> probes an out-of-process source direction
/// with: a dataset name plus its dataset-level options, exactly the shape a PlanRead needs. Absence
/// (no <c>read:</c> supplied) is what tells the suite this connector has nothing to probe as a source
/// -- every source-side vector reports <see cref="ConformanceOutcome.Skipped"/> rather than running
/// against a spec nobody asked for.</summary>
public sealed record ConformanceReadProbe(string Dataset, IReadOnlyDictionary<string, object?> Options);

/// <summary>The sink-direction twin of <see cref="ConformanceReadProbe"/>: an output name, write mode
/// and schema policy, plus output-level options. Absence means the suite has nothing to probe as a
/// sink.</summary>
public sealed record ConformanceWriteProbe(
    string Output, string Mode, string SchemaPolicy, IReadOnlyDictionary<string, object?> Options);

/// <summary>Everything <see cref="ConformanceSuite.RunAsync"/> needs to spawn and probe one connector
/// instance. <paramref name="Manifest"/> is optional -- a bare entrypoint path has none, and the
/// identity/capability handshake gates <c>PcpClient</c> would otherwise run against it simply do not
/// run; a package directory's manifest is passed through so those gates are exercised exactly as they
/// would be for a real restored package.</summary>
public sealed record ConformanceRequest(
    string EntrypointPath,
    string PackageName,
    ConnectorManifest? Manifest,
    string InstanceId,
    ConnectorConfig ConnectionConfig,
    ConformanceReadProbe? ReadProbe,
    ConformanceWriteProbe? WriteProbe);

public enum ConformanceOutcome
{
    Passed,
    Failed,
    Skipped,
}

/// <summary><paramref name="Detail"/> is the observed protocol fact (or skip reason), never a
/// connector config value or SQL text -- the secret/PII hygiene rule applies to conformance output
/// exactly as it does to every other user-facing surface.</summary>
public sealed record ConformanceVectorResult(string Name, ConformanceOutcome Outcome, string? Detail);

public sealed record ConformanceReport(IReadOnlyList<ConformanceVectorResult> Vectors)
{
    public bool AnyFailed => Vectors.Any(v => v.Outcome == ConformanceOutcome.Failed);
}

/// <summary>Black-box protocol conformance checks for one out-of-process connector, ported from the
/// TestKit's protocol-relevant acceptance assertions (one vector per assertion family) but run against
/// the wire itself -- raw <see cref="PcpClient.Grpc"/> calls and <see cref="DataPlane"/> connections --
/// rather than through an in-process <c>ISourceConnector</c>/<c>ISinkConnector</c>.
///
/// <para>Aggregate-all-failures: every applicable vector runs regardless of earlier failures (the error
/// philosophy binding convention), except when the handshake itself never completes -- with no
/// connected client there is nothing left to probe, so that is the one case where the report holds a
/// single "handshake" entry and nothing else.</para>
///
/// <para>Direction gating: a vector that needs a dataset to read runs only when
/// <see cref="ConformanceRequest.ReadProbe"/> was supplied; one that needs an output to write runs only
/// when <see cref="ConformanceRequest.WriteProbe"/> was. This is the practical stand-in for "Hello
/// declares a source/sink" -- PCP's <c>Hello</c> carries no direction flag, so the config-named dataset
/// itself is what proves which directions there is anything to probe.</para></summary>
public static class ConformanceSuite
{
    private static readonly TimeSpan ProbeRpcTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan PrematureCommitTimeout = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan CancellationDeadline = TimeSpan.FromSeconds(5);
    private const long ControlPlaneSizeCapBytes = 1024 * 1024;
    private const int ProbeRowCount = 5;

    public static async Task<ConformanceReport> RunAsync(
        ConformanceRequest request, string socketRootDir, CancellationToken ct)
    {
        var vectors = new List<ConformanceVectorResult>();

        // Mirrors LazyProcessConnector.SpawnAsync's own per-instance naming: one socket directory per
        // spawn, never the bare root (ConnectorProcess.Spawn does not mint this itself -- every caller
        // that spawns more than one process under one root has to).
        var socketDir = Path.Combine(socketRootDir, "pcp-" + Guid.NewGuid().ToString("N")[..8]);
        var process = ConnectorProcess.Spawn(request.EntrypointPath, socketDir, request.PackageName);

        PcpClient? client = null;
        HostChannelPump? pump = null;
        try
        {
            try
            {
                client = await PcpClient
                    .ConnectAndConfigureAsync(process, request.Manifest, request.InstanceId, request.ConnectionConfig, ct)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Nothing else in the suite can run without a live client -- spawn succeeded (an
                // entrypoint/socket-root problem is a setup failure, not a vector, and propagates past
                // this catch instead) but the handshake/Configure sequence itself did not.
                vectors.Add(new ConformanceVectorResult("handshake", ConformanceOutcome.Failed, ex.Message));
                return new ConformanceReport(vectors);
            }

            vectors.Add(RunSync("handshake", () => VerifyHandshake(client.Hello)));

            // Opened so a connector declaring GatedOperations can complete its GateAcquire/GateGrant/
            // GateComplete round trip during the vectors below instead of hanging with no host pump to
            // answer it -- SimpleGate applies no pacing/retry of its own, it only unblocks the exchange.
            pump = HostChannelPump.Start(client, process, new SimpleGate());

            await AddVectorAsync(vectors, "schema-batch-equality", request.ReadProbe is null
                ? SkippedAsync("no read: probe supplied in --config")
                : SchemaBatchEqualityAsync(client, process, request.ConnectionConfig, request.ReadProbe, ct));

            await AddVectorAsync(vectors, "commit-abort-session-rules", request.WriteProbe is null
                ? SkippedAsync("no write: probe supplied in --config")
                : CommitAbortSessionRulesAsync(client, process, request.ConnectionConfig, request.WriteProbe, ct));

            await AddVectorAsync(vectors, "cancellation", request.ReadProbe is null
                ? SkippedAsync("no read: probe supplied in --config")
                : CancellationAsync(client, process, request.ConnectionConfig, request.ReadProbe, ct));

            await AddVectorAsync(vectors, "transient-error-shape",
                TransientErrorShapeAsync(client, request.ReadProbe, request.WriteProbe, ct));

            await AddVectorAsync(vectors, "validation-aggregation", ValidationAggregationAsync(client, ct));

            await AddVectorAsync(vectors, "partition-id-stability", request.ReadProbe is null
                ? SkippedAsync("no read: probe supplied in --config")
                : PartitionIdStabilityAsync(client, request.ReadProbe, ct));

            await AddVectorAsync(vectors, "ticket-handling", request.ReadProbe is null
                ? SkippedAsync("ticket-handling requires a read: probe in --config")
                : TicketHandlingAsync(client, process, request.ReadProbe, ct));

            await AddVectorAsync(vectors, "control-plane-message-size",
                ControlPlaneMessageSizeAsync(client, request.ReadProbe, request.WriteProbe, ct));

            // Last, always: a passing run ends the process, so nothing after this can still talk to it.
            await AddVectorAsync(vectors, "clean-exit-on-shutdown", CleanExitOnShutdownAsync(client, process, ct));
        }
        finally
        {
            if (pump is not null)
            {
                await pump.DisposeAsync().ConfigureAwait(false);
            }

            // Idempotent either way: if clean-exit-on-shutdown already shut the connector down, this
            // just joins the already-exited process; if it did not run or did not pass, this is the
            // ladder that actually reaps it.
            if (client is not null)
            {
                await client.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                await process.DisposeAsync().ConfigureAwait(false);
            }
        }

        return new ConformanceReport(vectors);
    }

    // ---- vector 1: handshake discipline -------------------------------------------------------

    private static VectorVerdict VerifyHandshake(Hello hello)
    {
        if (string.IsNullOrEmpty(hello.Info.Name))
        {
            return VectorVerdict.Fail("Hello.Info.Name is empty");
        }

        if (hello.Info.ProtocolMajor != ProtocolVersion.Major)
        {
            return VectorVerdict.Fail(
                $"Hello reported protocol major {hello.Info.ProtocolMajor}, this host speaks {ProtocolVersion.Major}");
        }

        foreach (var (label, schema) in new[]
                 { ("connection", hello.ConnectionConfigSchema), ("dataset", hello.DatasetConfigSchema) })
        {
            if (string.IsNullOrEmpty(schema))
            {
                continue;
            }

            try
            {
                using var _ = JsonDocument.Parse(schema);
            }
            catch (JsonException ex)
            {
                return VectorVerdict.Fail($"{label} config schema is not valid JSON: {ex.Message}");
            }
        }

        return VectorVerdict.Pass();
    }

    // ---- vector 2: schema/batch equality -------------------------------------------------------

    private static async Task<VectorVerdict> SchemaBatchEqualityAsync(
        PcpClient client, ConnectorProcess process, ConnectorConfig connectionConfig,
        ConformanceReadProbe probe, CancellationToken ct)
    {
        var spec = new DatasetSpec("conformance", probe.Dataset, probe.Options);
        await using var source = await new ProcessSourceConnector(client, process)
            .OpenAsync(connectionConfig, ct).ConfigureAwait(false);
        var declared = await source.GetSchemaAsync(spec, ct).ConfigureAwait(false);
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, ct).ConfigureAwait(false);

        var comparedBatches = 0;
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, ct).ConfigureAwait(false))
            {
                using (batch)
                {
                    if (CompareSchemas(declared.Schema, batch.Schema) is { } mismatch)
                    {
                        return mismatch;
                    }

                    comparedBatches++;
                }
            }
        }

        return comparedBatches > 0
            ? VectorVerdict.Pass()
            : VectorVerdict.Pass("the probe dataset produced no batches; nothing to compare against the declared schema");
    }

    private static VectorVerdict? CompareSchemas(Schema declared, Schema batch)
    {
        if (declared.FieldsList.Count != batch.FieldsList.Count)
        {
            return VectorVerdict.Fail(
                $"declared schema has {declared.FieldsList.Count} field(s), a batch's schema has {batch.FieldsList.Count}");
        }

        for (var i = 0; i < declared.FieldsList.Count; i++)
        {
            if (!string.Equals(declared.FieldsList[i].Name, batch.FieldsList[i].Name, StringComparison.Ordinal))
            {
                return VectorVerdict.Fail($"field {i}: declared and batch schema disagree on field name");
            }

            if (declared.FieldsList[i].DataType.TypeId != batch.FieldsList[i].DataType.TypeId)
            {
                return VectorVerdict.Fail(
                    $"field {i}: declared type {declared.FieldsList[i].DataType.TypeId} " +
                    $"!= batch type {batch.FieldsList[i].DataType.TypeId}");
            }
        }

        return null;
    }

    // ---- vector 3: commit/abort session rules --------------------------------------------------

    private static async Task<VectorVerdict> CommitAbortSessionRulesAsync(
        PcpClient client, ConnectorProcess process, ConnectorConfig connectionConfig,
        ConformanceWriteProbe probe, CancellationToken ct)
    {
        var spec = new OutputSpec("conformance", probe.Output, probe.Mode, probe.SchemaPolicy, probe.Options);
        await using var sink = await new ProcessSinkConnector(client, process)
            .OpenAsync(connectionConfig, ct).ConfigureAwait(false);

        // (a) commit returns counts.
        var (commitSchema, commitBatch) = BuildProbeBatch();
        try
        {
            await using var session = await sink.BeginWriteAsync(spec, commitSchema, ct).ConfigureAwait(false);
            await session.WriteBatchAsync(commitBatch, ct).ConfigureAwait(false);
            var result = await session.CommitAsync(ct).ConfigureAwait(false);
            if (result.RowsWritten != ProbeRowCount)
            {
                return VectorVerdict.Fail(
                    $"commit reported {result.RowsWritten} row(s) written, the probe wrote {ProbeRowCount}");
            }
        }
        finally
        {
            commitBatch.Dispose();
        }

        // (b) abort after a write completes without throwing (DiscardsAll/BestEffort/None all promise
        // this much; verifying the destination actually discarded the rows is out of scope for a
        // generic black-box probe that has no way to read an arbitrary connector's own destination).
        var (abortSchema, abortBatch) = BuildProbeBatch();
        try
        {
            await using var session = await sink.BeginWriteAsync(spec, abortSchema, ct).ConfigureAwait(false);
            await session.WriteBatchAsync(abortBatch, ct).ConfigureAwait(false);
            await session.AbortAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return VectorVerdict.Fail($"abort after a write threw: {ex.Message}");
        }
        finally
        {
            abortBatch.Dispose();
        }

        // (c) CommitWrite before the data stream is half-closed: never opened here at all, so the
        // connector has no way to see an end-of-stream. A short deadline is the "send it early" probe --
        // a well-behaved connector must not accept the commit within it.
        var opId = ProcessSource.NewOpId();
        var (prematureSchema, _) = BuildProbeBatch();
        var beginResponse = await client.Grpc
            .BeginWriteAsync(
                new BeginWriteRequest
                {
                    OpId = opId,
                    Spec = MessageMapping.ToOutputSpecMsg(spec),
                    ArrowSchemaIpc = await MessageMapping.SerializeSchemaAsync(prematureSchema, ct).ConfigureAwait(false),
                },
                cancellationToken: ct)
            .ConfigureAwait(false);
        try
        {
            await client.Grpc
                .CommitWriteAsync(
                    new SessionRef { SessionId = beginResponse.SessionId },
                    deadline: DateTime.UtcNow.Add(PrematureCommitTimeout),
                    cancellationToken: ct)
                .ConfigureAwait(false);
            return VectorVerdict.Fail(
                "CommitWrite succeeded before the write's data stream was ever opened, let alone half-closed");
        }
        catch (RpcException)
        {
            // Expected: rejected, or the deadline above fired first -- either is "did not accept it".
        }
        finally
        {
            await TryAbortAsync(client, beginResponse.SessionId).ConfigureAwait(false);
        }

        return VectorVerdict.Pass();
    }

    private static async Task TryAbortAsync(PcpClient client, string sessionId)
    {
        try
        {
            await client.Grpc.AbortWriteAsync(new SessionRef { SessionId = sessionId }, cancellationToken: CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch
        {
            // Best-effort cleanup; the vector's own verdict does not depend on this succeeding.
        }
    }

    private static (Schema Schema, RecordBatch Batch) BuildProbeBatch()
    {
        var schema = new Schema([new Field("value", Int64Type.Default, nullable: false)], null);
        var builder = new Int64Array.Builder();
        for (var i = 0; i < ProbeRowCount; i++)
        {
            builder.Append(i);
        }

        return (schema, new RecordBatch(schema, [builder.Build()], ProbeRowCount));
    }

    // ---- vector 4: cancellation within 5s -------------------------------------------------------

    private static async Task<VectorVerdict> CancellationAsync(
        PcpClient client, ConnectorProcess process, ConnectorConfig connectionConfig,
        ConformanceReadProbe probe, CancellationToken ct)
    {
        var spec = new DatasetSpec("conformance", probe.Dataset, probe.Options);
        await using var source = await new ProcessSourceConnector(client, process)
            .OpenAsync(connectionConfig, ct).ConfigureAwait(false);
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, ct).ConfigureAwait(false);
        if (partitions.Count == 0)
        {
            return VectorVerdict.Pass("the probe dataset planned no partitions; nothing to cancel a read against");
        }

        using var cancel = new CancellationTokenSource();
        var readTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, cancel.Token).ConfigureAwait(false))
                {
                    batch.Dispose();
                    // Cancel as soon as there is something to cancel mid-stream; a dataset that finishes
                    // in one batch just lets the loop end on its own, which is an equally honest way to
                    // stay inside the 5s deadline.
                    cancel.Cancel();
                }
            }
            catch (OperationCanceledException)
            {
                // Clean cancellation is the expected outcome.
            }
        }, ct);

        // Deadline, not a sleep-based assertion: whichever of the read or the 5s timer finishes first
        // decides the vector, and only the read task's own completion counts as "honored cancellation".
        var winner = await Task.WhenAny(readTask, Task.Delay(CancellationDeadline, ct)).ConfigureAwait(false);
        if (!ReferenceEquals(winner, readTask))
        {
            return VectorVerdict.Fail($"the read did not stop within {CancellationDeadline.TotalSeconds:0}s of cancellation");
        }

        await readTask.ConfigureAwait(false); // propagate any unexpected (non-cancellation) failure
        return VectorVerdict.Pass();
    }

    // ---- vector 5: transient-error shape ---------------------------------------------------------

    private static async Task<VectorVerdict> TransientErrorShapeAsync(
        PcpClient client, ConformanceReadProbe? readProbe, ConformanceWriteProbe? writeProbe, CancellationToken ct)
    {
        const string missingName = "__pz_conformance_probe_missing__";
        RpcException? failure;
        if (readProbe is not null)
        {
            var spec = new DatasetSpecMsg
            {
                Source = "conformance", Dataset = missingName, Options = MessageMapping.ToStruct(readProbe.Options),
            };
            failure = await TryCatchAsync(() => client.Grpc.GetSchemaAsync(
                new GetSchemaRequest { OpId = ProcessSource.NewOpId(), Spec = spec },
                deadline: DateTime.UtcNow.Add(ProbeRpcTimeout), cancellationToken: ct)).ConfigureAwait(false);
        }
        else if (writeProbe is not null)
        {
            var (schema, _) = BuildProbeBatch();
            var spec = MessageMapping.ToOutputSpecMsg(
                new OutputSpec("conformance", missingName, writeProbe.Mode, writeProbe.SchemaPolicy, writeProbe.Options));
            var schemaIpc = await MessageMapping.SerializeSchemaAsync(schema, ct).ConfigureAwait(false);
            failure = await TryCatchAsync(() => client.Grpc.BeginWriteAsync(
                new BeginWriteRequest { OpId = ProcessSource.NewOpId(), Spec = spec, ArrowSchemaIpc = schemaIpc },
                deadline: DateTime.UtcNow.Add(ProbeRpcTimeout), cancellationToken: ct)).ConfigureAwait(false);
        }
        else
        {
            return VectorVerdict.Skip("no read: or write: probe supplied in --config");
        }

        if (failure is null)
        {
            // TestKit precedent (Transient_failures_carry_is_transient): a connector that never fails
            // this probe gives the mechanism nothing to check the shape of -- this vector tested
            // nothing, so it must report Skip, not a Pass that admits it verified no trailer at all.
            return VectorVerdict.Skip(
                "connector accepted an unknown dataset/output name without an error; the pz-error-bin " +
                "trailer's shape could not be observed");
        }

        var trailer = failure.Trailers.FirstOrDefault(entry =>
            string.Equals(entry.Key, ProtocolConstants.ErrorDetailTrailerKey, StringComparison.Ordinal));
        if (trailer is null)
        {
            return VectorVerdict.Fail("connector reported a failure with no pz-error-bin trailer");
        }

        PzErrorDetail detail;
        try
        {
            detail = PzErrorDetail.Parser.ParseFrom(trailer.ValueBytes);
        }
        catch (Exception ex)
        {
            return VectorVerdict.Fail($"pz-error-bin trailer failed to parse: {ex.Message}");
        }

        return string.IsNullOrEmpty(detail.Message)
            ? VectorVerdict.Fail("pz-error-bin trailer parsed but its message was empty")
            : VectorVerdict.Pass();
    }

    private static async Task<RpcException?> TryCatchAsync<T>(Func<AsyncUnaryCall<T>> call)
    {
        try
        {
            await call().ConfigureAwait(false);
            return null;
        }
        catch (RpcException ex)
        {
            return ex;
        }
    }

    // ---- vector 6: validation aggregation --------------------------------------------------------

    private static async Task<VectorVerdict> ValidationAggregationAsync(PcpClient client, CancellationToken ct)
    {
        // Deliberately empty: whatever the connector's own required fields are, an empty config is a
        // config no real connector should consider complete. The hard requirement this checks is that
        // Validate never surfaces as an RPC status failure regardless -- aggregation, not throw-per-error.
        var request = new ValidateRequest { Config = MessageMapping.ToStruct(new Dictionary<string, object?>()) };
        ValidationResultMsg response;
        try
        {
            response = await client.Grpc
                .ValidateAsync(request, deadline: DateTime.UtcNow.Add(ProbeRpcTimeout), cancellationToken: ct)
                .ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            return VectorVerdict.Fail($"Validate surfaced an empty config as an RPC status failure ({ex.StatusCode}) instead of an errors list");
        }

        return response.Errors.Count > 0
            ? VectorVerdict.Pass()
            : VectorVerdict.Pass("connector reported no errors for an empty config");
    }

    // ---- vector 7: partition id stability ---------------------------------------------------------

    private static async Task<VectorVerdict> PartitionIdStabilityAsync(
        PcpClient client, ConformanceReadProbe probe, CancellationToken ct)
    {
        if ((client.Hello.Capabilities & (long)ConnectorCapabilities.StablePartitionIds) == 0)
        {
            return VectorVerdict.Skip("connector does not declare StablePartitionIds");
        }

        var spec = new DatasetSpecMsg { Source = "conformance", Dataset = probe.Dataset, Options = MessageMapping.ToStruct(probe.Options) };
        var first = await PlanIdsAsync(client, ProcessSource.NewOpId(), spec, ct).ConfigureAwait(false);
        var second = await PlanIdsAsync(client, ProcessSource.NewOpId(), spec, ct).ConfigureAwait(false);

        if (first.Count == 0)
        {
            return VectorVerdict.Fail("StablePartitionIds declared but PlanRead produced zero partitions");
        }

        if (first.Distinct(StringComparer.Ordinal).Count() != first.Count)
        {
            return VectorVerdict.Fail("partition ids are not unique within one plan");
        }

        return first.SequenceEqual(second, StringComparer.Ordinal)
            ? VectorVerdict.Pass()
            : VectorVerdict.Fail("partition ids changed across two PlanRead calls for the same spec");
    }

    private static async Task<List<string>> PlanIdsAsync(PcpClient client, string opId, DatasetSpecMsg spec, CancellationToken ct)
    {
        var request = new PlanReadRequest { OpId = opId, Spec = spec, Hints = new ReadHintsMsg() };
        using var call = client.Grpc.PlanRead(request, cancellationToken: ct);
        var ids = new List<string>();
        await foreach (var partition in call.ResponseStream.ReadAllAsync(ct).ConfigureAwait(false))
        {
            ids.Add(partition.PartitionId);
        }

        return ids;
    }

    // ---- vector 8: ticket handling -----------------------------------------------------------------

    private static async Task<VectorVerdict> TicketHandlingAsync(
        PcpClient client, ConnectorProcess process, ConformanceReadProbe probe, CancellationToken ct)
    {
        // OpenReadStream resolves its partition against THIS op's own plan (PcpService's PlannedRead
        // lookup is keyed by op id), so PlanRead and OpenReadStream below must share the same one.
        var opId = ProcessSource.NewOpId();
        var spec = new DatasetSpecMsg { Source = "conformance", Dataset = probe.Dataset, Options = MessageMapping.ToStruct(probe.Options) };
        var partitionIds = await PlanIdsAsync(client, opId, spec, ct).ConfigureAwait(false);
        if (partitionIds.Count == 0)
        {
            return VectorVerdict.Skip("the probe dataset planned no partitions to open a read ticket against");
        }

        var openResponse = await client.Grpc
            .OpenReadStreamAsync(
                new OpenReadRequest { OpId = opId, PartitionId = partitionIds[0], Options = new BatchOptionsMsg() },
                cancellationToken: ct)
            .ConfigureAwait(false);
        var ticket = openResponse.Ticket.Memory;

        // Legitimate single use: drain it fully.
        await foreach (var batch in DataPlane.ReadStreamAsync(process.DataSocketPath, ticket, ct).ConfigureAwait(false))
        {
            batch.Dispose();
        }

        var reusedRejected = await IsTicketRejectedAsync(process.DataSocketPath, ticket, ct).ConfigureAwait(false);
        var unknownTicket = RandomNumberGenerator.GetBytes(ProtocolConstants.TicketLength);
        var unknownRejected = await IsTicketRejectedAsync(process.DataSocketPath, unknownTicket, ct).ConfigureAwait(false);

        if (!reusedRejected)
        {
            return VectorVerdict.Fail("a burned (already-used) ticket was accepted for a second data-plane connection");
        }

        return unknownRejected
            ? VectorVerdict.Pass()
            : VectorVerdict.Fail("a ticket that was never minted was accepted for a data-plane connection");
    }

    private static async Task<bool> IsTicketRejectedAsync(string dataSocketPath, ReadOnlyMemory<byte> ticket, CancellationToken ct)
    {
        try
        {
            await foreach (var batch in DataPlane.ReadStreamAsync(dataSocketPath, ticket, ct).ConfigureAwait(false))
            {
                batch.Dispose();
            }

            // A stream that yielded a schema (however empty) without a matching valid ticket is the
            // unrejected case -- DataPlane itself would have thrown ConnectorHostException otherwise.
            return false;
        }
        catch (ConnectorHostException)
        {
            return true;
        }
    }

    // ---- vector 9: clean exit on shutdown ---------------------------------------------------------

    private static async Task<VectorVerdict> CleanExitOnShutdownAsync(PcpClient client, ConnectorProcess process, CancellationToken ct)
    {
        var exited = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        process.Exited += () => exited.TrySetResult();
        if (process.HasExited)
        {
            exited.TrySetResult();
        }

        try
        {
            await client.Grpc
                .ShutdownAsync(new ShutdownRequest(), deadline: DateTime.UtcNow.Add(ProtocolConstants.ShutdownGrace), cancellationToken: ct)
                .ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            return VectorVerdict.Fail($"Shutdown RPC failed: {ex.StatusCode}");
        }

        var winner = await Task.WhenAny(exited.Task, Task.Delay(ProtocolConstants.ShutdownGrace, ct)).ConfigureAwait(false);
        return ReferenceEquals(winner, exited.Task)
            ? VectorVerdict.Pass()
            : VectorVerdict.Fail(
                $"connector did not exit within the {ProtocolConstants.ShutdownGrace.TotalSeconds:0}s shutdown grace after acknowledging Shutdown");
    }

    // ---- vector 10: no data on the control plane ----------------------------------------------------

    private static async Task<VectorVerdict> ControlPlaneMessageSizeAsync(
        PcpClient client, ConformanceReadProbe? readProbe, ConformanceWriteProbe? writeProbe, CancellationToken ct)
    {
        if (OverCap("Hello", client.Hello.CalculateSize()) is { } helloOverCap)
        {
            return helloOverCap;
        }

        if (readProbe is not null)
        {
            var spec = new DatasetSpecMsg { Source = "conformance", Dataset = readProbe.Dataset, Options = MessageMapping.ToStruct(readProbe.Options) };
            var response = await client.Grpc
                .GetSchemaAsync(
                    new GetSchemaRequest { OpId = ProcessSource.NewOpId(), Spec = spec },
                    deadline: DateTime.UtcNow.Add(ProbeRpcTimeout), cancellationToken: ct)
                .ConfigureAwait(false);
            return OverCap("GetSchema response", response.CalculateSize()) ?? VectorVerdict.Pass();
        }

        if (writeProbe is not null)
        {
            var (schema, _) = BuildProbeBatch();
            var spec = MessageMapping.ToOutputSpecMsg(
                new OutputSpec("conformance", writeProbe.Output, writeProbe.Mode, writeProbe.SchemaPolicy, writeProbe.Options));
            var response = await client.Grpc
                .BeginWriteAsync(
                    new BeginWriteRequest
                    {
                        OpId = ProcessSource.NewOpId(), Spec = spec,
                        ArrowSchemaIpc = await MessageMapping.SerializeSchemaAsync(schema, ct).ConfigureAwait(false),
                    },
                    deadline: DateTime.UtcNow.Add(ProbeRpcTimeout), cancellationToken: ct)
                .ConfigureAwait(false);
            await TryAbortAsync(client, response.SessionId).ConfigureAwait(false);
            return OverCap("BeginWrite response", response.CalculateSize()) ?? VectorVerdict.Pass();
        }

        return VectorVerdict.Pass("no read: or write: probe supplied in --config; only Hello was measured");
    }

    private static VectorVerdict? OverCap(string what, int size) =>
        size > ControlPlaneSizeCapBytes
            ? VectorVerdict.Fail($"{what} was {size} bytes, over the {ControlPlaneSizeCapBytes} byte control-plane sanity cap")
            : null;

    // ---- plumbing -------------------------------------------------------------------------------

    private static ConformanceVectorResult RunSync(string name, Func<VectorVerdict> body)
    {
        try
        {
            var verdict = body();
            return new ConformanceVectorResult(name, verdict.Outcome, verdict.Detail);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new ConformanceVectorResult(name, ConformanceOutcome.Failed, ex.Message);
        }
    }

    /// <summary>Runs one vector body, converting any escaping exception into a failed verdict rather
    /// than letting it abort the whole run -- aggregate-all-failures applies to a vector's own bugs
    /// exactly as it does to a genuine protocol violation.</summary>
    private static async Task AddVectorAsync(List<ConformanceVectorResult> vectors, string name, Task<VectorVerdict> body)
    {
        VectorVerdict verdict;
        try
        {
            verdict = await body.ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            verdict = VectorVerdict.Fail(ex.Message);
        }

        vectors.Add(new ConformanceVectorResult(name, verdict.Outcome, verdict.Detail));
    }

    private static Task<VectorVerdict> SkippedAsync(string reason) => Task.FromResult(VectorVerdict.Skip(reason));

    private readonly struct VectorVerdict(ConformanceOutcome outcome, string? detail)
    {
        public ConformanceOutcome Outcome { get; } = outcome;

        public string? Detail { get; } = detail;

        public static VectorVerdict Pass(string? detail = null) => new(ConformanceOutcome.Passed, detail);

        public static VectorVerdict Fail(string detail) => new(ConformanceOutcome.Failed, detail);

        public static VectorVerdict Skip(string reason) => new(ConformanceOutcome.Skipped, reason);
    }

    /// <summary>No pacing, no retry: just runs the operation. Enough to unblock a gated connector's
    /// GateAcquire/GateGrant/GateComplete round trip during the vectors above; the suite is not
    /// exercising the engine's real resilience policy.</summary>
    private sealed class SimpleGate : IOperationGate
    {
        public Task<T> ExecuteAsync<T>(string opLabel, bool idempotent, Func<CancellationToken, Task<T>> op, CancellationToken ct) =>
            op(ct);

        public void ReportBudget(int remaining, DateTimeOffset resetAt)
        {
        }
    }
}
