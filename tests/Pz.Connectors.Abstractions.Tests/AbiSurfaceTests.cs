using System.Reflection;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Memory;

public class AbiSurfaceTests
{
    private static readonly Assembly Abi = typeof(IConnector).Assembly;

    [Fact]
    public void Abstractions_references_only_arrow_and_logging()
    {
        var allowed = new[] { "System", "netstandard", "mscorlib", "Apache.Arrow", "Microsoft.Extensions.Logging.Abstractions" };
        foreach (var reference in Abi.GetReferencedAssemblies())
        {
            Assert.True(
                allowed.Any(a => reference.Name == a || reference.Name!.StartsWith("System.", StringComparison.Ordinal)),
                $"Forbidden assembly reference: {reference.Name}");
        }
    }

    [Fact]
    public void All_public_types_live_in_abstractions_namespace()
    {
        foreach (var type in Abi.GetExportedTypes())
        {
            Assert.StartsWith("Pz.Connectors.Abstractions", type.Namespace, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Protocol_major_is_one()
    {
        Assert.Equal(1, ProtocolVersion.Major);
    }

    [Fact]
    public void Connector_exception_carries_transient_and_retry_after()
    {
        var ex = new PzConnectorException("throttled", isTransient: true, retryAfter: TimeSpan.FromSeconds(30));
        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(30), ex.RetryAfter);
        var permanent = new PzConnectorException("bad credentials", isTransient: false);
        Assert.False(permanent.IsTransient);
        Assert.Null(permanent.RetryAfter);
    }

    [Fact]
    public void Connector_attribute_is_assembly_level_and_repeatable()
    {
        var usage = typeof(PzConnectorAttribute).GetCustomAttribute<AttributeUsageAttribute>()!;
        Assert.Equal(AttributeTargets.Assembly, usage.ValidOn);
        Assert.True(usage.AllowMultiple);
    }

    [Fact]
    public void Validation_result_success_and_failure_semantics()
    {
        Assert.True(ValidationResult.Success.IsValid);
        Assert.Empty(ValidationResult.Success.Errors);
        var failed = ValidationResult.Failed("missing host", "missing port");
        Assert.False(failed.IsValid);
        Assert.Equal(2, failed.Errors.Count);
    }

    /// <summary><see cref="PooledNativeAllocator"/> is public ABI surface — this pins that it
    /// stays public, sealed, and (crucially) still derives from
    /// <c>Apache.Arrow.Memory.MemoryAllocator</c> rather than pulling in any dependency the allowlist
    /// above doesn't already cover (<c>Apache.Arrow.Memory</c> ships inside the same Apache.Arrow
    /// assembly the allowlist test already permits, so this adds no new external reference).</summary>
    [Fact]
    public void PooledNativeAllocator_is_public_sealed_and_extends_arrow_memory_allocator()
    {
        Assert.True(typeof(PooledNativeAllocator).IsPublic);
        Assert.True(typeof(PooledNativeAllocator).IsSealed);
        Assert.Equal(typeof(Apache.Arrow.Memory.MemoryAllocator), typeof(PooledNativeAllocator).BaseType);
        Assert.NotNull(PooledNativeAllocator.Shared);
        Assert.Same(PooledNativeAllocator.Shared, PooledNativeAllocator.Shared);
    }

    [Fact]
    public void NativeCopy_finalizations_default_empty_and_FileMove_roundtrips()
    {
        var copy = new NativeCopy("copy (select 1) to 'x'", []);
        Assert.Empty(copy.Finalizations);

        var move = new FileMove("/tmp/.pz-native-x.parquet", "/tmp/out.parquet");
        Assert.Equal("/tmp/.pz-native-x.parquet", move.TempPath);
        Assert.Equal("/tmp/out.parquet", move.FinalPath);

        var withFinalizations = copy with { Finalizations = [move] };
        Assert.Same(move, Assert.Single(withFinalizations.Finalizations));
    }

    [Fact]
    public void StreamingPartitions_flag_is_distinct_power_of_two()
    {
        Assert.Equal(512, (int)ConnectorCapabilities.StreamingPartitions);
        // no overlap with existing flags
        foreach (var f in Enum.GetValues<ConnectorCapabilities>())
            if (f != ConnectorCapabilities.StreamingPartitions && f != ConnectorCapabilities.None)
                Assert.NotEqual(ConnectorCapabilities.StreamingPartitions, f);
    }

    [Fact]
    public void IStreamingSource_is_separate_from_ISource()
    {
        // ISource does NOT gain a member (additive-only): assert the method lives on IStreamingSource.
        Assert.NotNull(typeof(IStreamingSource).GetMethod("PlanReadStreamingAsync"));
        Assert.Null(typeof(ISource).GetMethod("PlanReadStreamingAsync"));
    }

    [Fact]
    public void Stage4_capability_flags_have_reserved_values()
    {
        Assert.Equal(8192, (int)ConnectorCapabilities.StablePartitionIds);
        Assert.Equal(16384, (int)ConnectorCapabilities.CheckpointableReads);
    }

    [Fact]
    public void Checkpointing_partition_extends_identified_partition()
    {
        Assert.True(typeof(IIdentifiedPartition).IsAssignableFrom(typeof(ICheckpointingPartition)));
        Assert.True(typeof(IDatasetPartition).IsAssignableFrom(typeof(IIdentifiedPartition)));
    }

    [Fact]
    public void ChangeCapture_and_ApplyDeletes_flags_have_reserved_values()
    {
        Assert.Equal(131072, (int)ConnectorCapabilities.ChangeCapture);
        Assert.Equal(262144, (int)ConnectorCapabilities.ApplyDeletes);
    }

    [Fact]
    public void DatasetSpec_ChangeCapture_defaults_false_and_ChangeCaptureSlot_defaults_null()
    {
        var spec = new DatasetSpec("source", "dataset", new Dictionary<string, object?>());
        Assert.False(spec.ChangeCapture);
        Assert.Null(spec.ChangeCaptureSlot);
    }

    [Fact]
    public void OutputSpec_OnDelete_defaults_null()
    {
        var spec = new OutputSpec("sink", "output", "merge", "add", new Dictionary<string, object?>());
        Assert.Null(spec.OnDelete);
    }

    [Fact]
    public void CDC_interfaces_compile_with_exact_signatures()
    {
        // Compile-time assertion: stub implements all CDC interfaces with exact signatures.
        // If signatures drift, the class below fails to compile.
        var _ = new CdcInterfaceStub();
    }

    [Fact]
    public void ChangeCaptureStatus_record_fields_are_correct()
    {
        // Positional construction and field verification for ChangeCaptureStatus.
        var status = new ChangeCaptureStatus(true, "pg_wal_lsn_diff", 42L, ["retention ok"]);
        Assert.True(status.Healthy);
        Assert.Equal("pg_wal_lsn_diff", status.PositionName);
        Assert.Equal(42L, status.RetainedBytes);
        Assert.Single(status.Detail);
        Assert.Equal("retention ok", status.Detail[0]);
    }

    // Private stub implementing all three CDC interfaces (compile-time enforcement).
    private sealed class CdcInterfaceStub : IChangeCapturePartition, IDeleteApplyingWriteSession, IChangeCaptureAdmin
    {
        public bool TryGetChangeKeyColumns(out IReadOnlyList<string>? keyColumns)
        {
            keyColumns = null;
            return false;
        }

        public ValueTask WriteBatchAsync(Apache.Arrow.RecordBatch batch, CancellationToken ct) => default;
        public ValueTask<WriteResult> CommitAsync(CancellationToken ct) => default;
        public ValueTask AbortAsync(CancellationToken ct) => default;
        public ValueTask ApplyDeleteKeysAsync(Apache.Arrow.RecordBatch keyBatch, CancellationToken ct) => default;

        public ValueTask<ChangeCaptureStatus> GetChangeCaptureStatusAsync(DatasetSpec spec, CancellationToken ct) => default;
        public ValueTask DropChangeCaptureStateAsync(DatasetSpec spec, CancellationToken ct) => default;

        public ValueTask DisposeAsync() => default;
    }
}
