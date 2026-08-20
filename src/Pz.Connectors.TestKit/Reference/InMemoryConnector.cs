using System.Globalization;
using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connectors.TestKit.Reference;

/// <summary>Deterministic, in-process reference connector: a seeded source plus a capturing sink, with
/// fault-injection hooks. The executable spec the TestKit acceptance suite runs against.</summary>
public sealed class InMemoryConnector : ISourceConnector, ISinkConnector
{
    private readonly object _gate = new();
    private readonly List<CommittedWrite> _committed = [];
    private int _abortedSessions;
    private int _commitAttempts;

    public ConnectorInfo Info => new("inmemory", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.Transactional |
        ConnectorCapabilities.Merge | ConnectorCapabilities.ReplaceWrites |
        ConnectorCapabilities.BoundedWindow;

    public string ConnectionConfigSchema =>
        """{ "type": "object", "properties": {}, "additionalProperties": false }""";

    // Every option InMemorySource/InMemorySink actually read (FaultInjection.GetInt/GetBool covers the
    // fail_* keys for both): rows/partitions/partition_sizes size the seeded read; rows_read_hook is a
    // test-only Action<long> observer (unconstrained -- never produced by real YAML, so no "type" is
    // meaningful for it); fail_read_at_batch/fail_write_at_batch/fail_transient/fail_commit are the
    // fault-injection hooks. "columns" is not read by either class today, but -- like Postgres -- the
    // dataset-level columns: contract is a generic mechanism ProjectLoader/SpecBuilder merge into every
    // connector's dataset options, so it is accepted here too. fail_read_retry_limit is the same kind of
    // test-only, unconstrained-type object as rows_read_hook -- a shared RetryCounter reference letting a
    // test simulate "fails N times, then succeeds" across engine retries of the same node. fail_message
    // overrides the fixed "injected read failure" text on InMemorySource's read-fault path, letting a
    // test simulate a connector that echoes a raw engine error into its PzConnectorException message.
    // read_delay_ms is an opt-in per-batch delay simulating a slow read, letting StallAttributionTests
    // prove the consumer side of SourceLoadExecutor's channel dominates when the source itself is the
    // bottleneck. fail_retry_after is an opt-in numeric option -- a count of SECONDS (fractional allowed;
    // matches the Convert.ToDouble idiom every other fail_* numeric option already uses, rather than
    // introducing DurationParser's "5m"/"10s" string grammar, which lives in Pz.Core and is not a
    // dependency TestKit takes on) -- that, when present, becomes the injected PzConnectorException's
    // RetryAfter, letting a test prove the engine's breaker/retry catches forward ex.RetryAfter
    // end-to-end instead of null. Absent: RetryAfter stays null. ignore_watermark_bounds
    // makes InMemorySource ignore the engine-computed WatermarkLowerBound/UpperBound,
    // simulating a misbehaving universal-tier connector for testing the engine's staging trim backstop.
    // fail_foreign is a test-only boolean that
    // makes InMemorySource's SOURCE read-fault path throw a foreign (non-Pz-family) exception type
    // -- InvalidOperationException instead of PzConnectorException -- so a redaction-boundary test can
    // prove MessageRedaction.Redact(Exception) still redacts an exception that didn't come from a
    // Pz-family type (see Node_failure_message_is_redacted).
    public string DatasetConfigSchema =>
        """{ "type": "object", "properties": { "rows": { "type": "integer" }, "partitions": { "type": "integer" }, "partition_sizes": { "type": "array", "items": { "type": "integer" } }, "rows_read_hook": {}, "fail_read_at_batch": { "type": "integer" }, "fail_read_retry_limit": {}, "fail_message": { "type": "string" }, "fail_retry_after": { "type": "number" }, "read_delay_ms": { "type": "integer" }, "fail_write_at_batch": { "type": "integer" }, "fail_transient": { "type": "boolean" }, "fail_commit": { "type": "boolean" }, "ignore_watermark_bounds": { "type": "boolean" }, "fail_foreign": { "type": "boolean" }, "columns": { "type": "object", "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } } }, "additionalProperties": false }""";

    /// <summary>Committed write sessions, in commit order.</summary>
    public IReadOnlyList<CommittedWrite> Committed
    {
        get { lock (_gate) return _committed.ToArray(); }
    }

    /// <summary>Count of sessions that ended without a commit (explicit abort or implicit
    /// abort-on-dispose).</summary>
    public int AbortedSessions => Volatile.Read(ref _abortedSessions);

    /// <summary>Count of <see cref="ISinkWriteSession.CommitAsync"/> calls attempted, regardless of
    /// whether they succeeded — lets tests assert Commit was tried exactly once even when it fails.</summary>
    public int CommitAttempts => Volatile.Read(ref _commitAttempts);

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true));

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new InMemorySource());

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new InMemorySink(this));

    internal void RecordCommit(CommittedWrite write)
    {
        lock (_gate) _committed.Add(write);
    }

    /// <summary>Replaces every previously committed entry for
    /// <paramref name="spec"/>'s output with the single, already-fully-merged <paramref name="batches"/>
    /// -- unlike <see cref="RecordCommit"/>'s append-only log, a merge output's committed state is always
    /// exactly one entry (the current merged snapshot), so reading it back never re-sums stale rows a
    /// later merge has already superseded. Deliberately does NOT dispose the stale entries' own batches:
    /// <see cref="Committed"/> (and every <c>ReadCommittedAsync</c> acceptance-suite override built on it)
    /// hands back the live batch instances, not copies, and a caller may still be holding a reference read
    /// before this merge ran (exactly what <c>Merge_is_idempotent</c> does) -- disposing here would be a
    /// use-after-dispose hazard for that caller. This mirrors <see cref="RecordCommit"/>'s own existing
    /// behavior: this reference connector never disposes ANY committed batch itself, merge or otherwise
    /// (a deliberate simplicity tradeoff -- it is test-only, ephemeral, in-process state).</summary>
    internal void RecordMergeCommit(OutputSpec spec, IReadOnlyList<RecordBatch> batches, WriteResult result)
    {
        lock (_gate)
        {
            _committed.RemoveAll(c => c.Spec.Output == spec.Output);
            _committed.Add(new CommittedWrite(spec, batches, result));
        }
    }

    /// <summary>Replace-mode commit: the committed
    /// state for <paramref name="spec"/>'s output becomes exactly this session's batches -- prior
    /// entries for the same output are dropped, matching what ReplaceWrites declares (and what the
    /// real replace sinks do: TRUNCATE+INSERT, stable-name overwrite). Mirrors
    /// <see cref="RecordMergeCommit"/>'s deliberate no-dispose rule: stale entries' batches are NOT
    /// disposed here, because <see cref="Committed"/> hands back live instances a caller may still
    /// hold (this reference connector never disposes any committed batch itself).</summary>
    internal void RecordReplaceCommit(OutputSpec spec, IReadOnlyList<RecordBatch> batches, WriteResult result)
    {
        lock (_gate)
        {
            _committed.RemoveAll(c => c.Spec.Output == spec.Output);
            _committed.Add(new CommittedWrite(spec, batches, result));
        }
    }

    internal void RecordAbort() => Interlocked.Increment(ref _abortedSessions);

    internal void RecordCommitAttempt() => Interlocked.Increment(ref _commitAttempts);
}

/// <summary>One committed write session: the output it targeted, the batches it captured (deep copies,
/// never the engine-owned instances handed to <see cref="ISinkWriteSession.WriteBatchAsync"/>), and the
/// result the session reported.</summary>
public sealed record CommittedWrite(OutputSpec Spec, IReadOnlyList<RecordBatch> Batches, WriteResult Result);

/// <summary>Shared parsing for the `fail_*` dataset/output options.</summary>
internal static class FaultInjection
{
    public static int? GetInt(IReadOnlyDictionary<string, object?> options, string key) =>
        options.TryGetValue(key, out var value) && value is not null
            ? Convert.ToInt32(value, CultureInfo.InvariantCulture)
            : null;

    public static bool GetBool(IReadOnlyDictionary<string, object?> options, string key) =>
        options.TryGetValue(key, out var value) && value is not null
            && Convert.ToBoolean(value, CultureInfo.InvariantCulture);

    public static RetryCounter? GetRetryCounter(IReadOnlyDictionary<string, object?> options, string key) =>
        options.TryGetValue(key, out var value) ? value as RetryCounter : null;

    /// <summary><paramref name="key"/>'s value, interpreted as a count of SECONDS
    /// (via <see cref="Convert.ToDouble(object, IFormatProvider)"/>, the same conversion idiom
    /// <see cref="GetInt"/>/<see cref="GetBool"/> already use for every other fail_* option) -- e.g. a
    /// test passing 600 or 600.0 means "10 minutes". Returns null when absent, matching
    /// <see cref="PzConnectorException.RetryAfter"/>'s own default.</summary>
    public static TimeSpan? GetRetryAfter(IReadOnlyDictionary<string, object?> options, string key) =>
        options.TryGetValue(key, out var value) && value is not null
            ? TimeSpan.FromSeconds(Convert.ToDouble(value, CultureInfo.InvariantCulture))
            : null;
}

/// <summary>An opt-in "fail N times then succeed" fault-injection lever.
/// A single instance MUST be shared (passed as the same dataset/output option object reference) across
/// every retry attempt of one node — engine retries re-invoke the same executor over the same
/// <c>DatasetSpec</c>/<c>OutputSpec</c>, so the counter observes one decrement per attempt. When absent,
/// the `fail_read_at_batch`/`fail_write_at_batch` hooks fail on every attempt unconditionally.</summary>
public sealed class RetryCounter(int failTimes)
{
    private int _remaining = failTimes;

    /// <summary>True while this call should still fail (decrements first) — the (failTimes+1)-th call
    /// onward returns false, i.e. the fault stops firing and the operation is allowed to succeed.</summary>
    public bool ShouldFail() => Interlocked.Decrement(ref _remaining) >= 0;
}
