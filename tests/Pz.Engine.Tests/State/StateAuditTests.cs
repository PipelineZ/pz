using Pz.Engine.State;

namespace Pz.Engine.Tests.State;

/// <summary>The append-only `pz state` ledger. Field order is fixed and nulls are
/// omitted, so these assert exact bytes. `ts` comes from an injected TimeProvider, which is what lets a
/// byte-for-byte assertion exist at all.</summary>
public sealed class StateAuditTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));

    private static readonly FakeTimeProvider Clock = new(DateTimeOffset.Parse("2026-07-30T14:22:05.123Z"));

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    /// <summary>A TimeProvider pinned to one instant. The engine's determinism rule forbids DateTime.Now,
    /// and a fixed clock is what makes the `ts` field assertable.</summary>
    private sealed class FakeTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    [Fact]
    public void Rollback_renders_every_field_in_the_specified_order()
    {
        var audit = new StateAudit(_dir, Clock);

        var line = audit.Render(new StateAuditEntry(
            Action: "rollback", Key: "erp.dbo.orders", Cursor: "updated_at", Type: "timestamp",
            From: "2026-07-29T02:00:00.000000", FromRunId: "20260729T020013422Z-a91c",
            To: "2026-07-01T00:00:00.000000", Target: "run:20260701T020009111Z-3f2e",
            Reason: "late-arriving source rows"));

        Assert.Equal(
            """{"ts":"2026-07-30T14:22:05.123Z","action":"rollback","key":"erp.dbo.orders","cursor":"updated_at","type":"timestamp","from":"2026-07-29T02:00:00.000000","fromRunId":"20260729T020013422Z-a91c","to":"2026-07-01T00:00:00.000000","target":"run:20260701T020009111Z-3f2e","reason":"late-arriving source rows"}""",
            line);
    }

    [Fact]
    public void Absent_reason_is_omitted_entirely_never_written_as_null()
    {
        var line = new StateAudit(_dir, Clock).Render(new StateAuditEntry(
            "set", "erp.dbo.orders", "id", "bigint", "500", "run-1", "400", "value", Reason: null));

        Assert.DoesNotContain("reason", line);
        Assert.DoesNotContain("null", line);
    }

    [Fact]
    public void Clear_omits_the_value_fields_it_has_no_answer_for()
    {
        var line = new StateAudit(_dir, Clock).Render(new StateAuditEntry(
            "clear", "erp.dbo.orders", "updated_at", "weirdtype", "garbage", "run-1",
            To: null, Target: null, Reason: "unknown cursor type"));

        Assert.DoesNotContain("\"to\"", line);
        Assert.DoesNotContain("\"target\"", line);
        Assert.Contains("\"from\":\"garbage\"", line);
    }

    [Fact]
    public void Append_creates_the_file_and_ends_every_line_with_LF()
    {
        var audit = new StateAudit(_dir, Clock);

        audit.Append(new StateAuditEntry("set", "a.b", "id", "int", "1", "run-1", "2", "value", null));
        audit.Append(new StateAuditEntry("set", "a.b", "id", "int", "2", "manual", "3", "value", null));

        var text = File.ReadAllText(Path.Combine(_dir, StateAudit.FileName));
        Assert.Equal(2, text.Split('\n', StringSplitOptions.RemoveEmptyEntries).Length);
        Assert.EndsWith("\n", text);
        Assert.DoesNotContain("\r", text);
    }

    [Fact]
    public void Append_never_rewrites_earlier_lines()
    {
        var audit = new StateAudit(_dir, Clock);
        audit.Append(new StateAuditEntry("set", "a.b", "id", "int", "1", "run-1", "2", "value", "first"));
        var afterFirst = File.ReadAllText(Path.Combine(_dir, StateAudit.FileName));

        audit.Append(new StateAuditEntry("set", "a.b", "id", "int", "2", "manual", "3", "value", "second"));

        Assert.StartsWith(afterFirst, File.ReadAllText(Path.Combine(_dir, StateAudit.FileName)));
    }

    [Fact]
    public void Read_returns_only_the_requested_key_newest_first()
    {
        var audit = new StateAudit(_dir, Clock);
        audit.Append(new StateAuditEntry("set", "a.b", "id", "int", "1", "run-1", "2", "value", "one"));
        audit.Append(new StateAuditEntry("set", "other.key", "id", "int", "9", "run-1", "8", "value", "skip me"));
        audit.Append(new StateAuditEntry("rollback", "a.b", "id", "int", "2", "manual", "1", "run:r1", "two"));

        var lines = audit.Read("a.b");

        Assert.Equal(2, lines.Count);
        Assert.Equal("rollback", lines[0].Entry.Action);
        Assert.Equal("two", lines[0].Entry.Reason);
        Assert.Equal("2026-07-30T14:22:05.123Z", lines[0].Ts);
    }

    [Fact]
    public void Read_skips_a_torn_line_rather_than_throwing()
    {
        var audit = new StateAudit(_dir, Clock);
        audit.Append(new StateAuditEntry("set", "a.b", "id", "int", "1", "run-1", "2", "value", "good"));
        File.AppendAllText(Path.Combine(_dir, StateAudit.FileName), "{\"ts\":\"2026-\n");

        var lines = audit.Read("a.b");

        Assert.Equal("good", Assert.Single(lines).Entry.Reason);
    }

    [Fact]
    public void Read_of_an_absent_ledger_is_empty()
    {
        Assert.Empty(new StateAudit(_dir, Clock).Read("a.b"));
    }
}
