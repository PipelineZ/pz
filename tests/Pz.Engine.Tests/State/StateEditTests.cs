using Pz.Engine.Artifacts;
using Pz.Engine.State;

namespace Pz.Engine.Tests.State;

/// <summary>The `pz state` edit policy as a table test. The policy is pure — no filesystem — so
/// every rule is a case here rather than a directory fixture, mirroring RunRetentionTests.
///
/// The non-canonical and unknown-type cases are the load-bearing ones: WindowMath throws on both, and
/// they arrive from exactly the hand-edited file this verb repairs.</summary>
public sealed class StateEditTests
{
    private static Watermark Stored(string type = "timestamp", string value = "2026-07-29T02:00:00.000000") =>
        new("updated_at", type, value, "20260729T020013422Z-a91c");

    private static WatermarkHistoryEntry Hist(string runId, string value, string status = "success",
        string cursor = "updated_at", string type = "timestamp") =>
        new(runId, status, cursor, type, value);

    // ---- classification ----

    [Theory]
    [InlineData("int", "42", StateEntryHealth.Ok)]
    [InlineData("bigint", "9223372036854775807", StateEntryHealth.Ok)]
    [InlineData("decimal", "12.50", StateEntryHealth.Ok)]
    [InlineData("date", "2026-07-04", StateEntryHealth.Ok)]
    [InlineData("timestamp", "2026-07-04T10:00:00.000000", StateEntryHealth.Ok)]
    [InlineData("timestamp", "2026-07-04", StateEntryHealth.NonCanonicalValue)]
    [InlineData("int", "not-a-number", StateEntryHealth.NonCanonicalValue)]
    [InlineData("weirdtype", "whatever", StateEntryHealth.UnknownType)]
    public void Classify_sorts_entries_into_the_three_health_states(string type, string value, StateEntryHealth expected)
    {
        Assert.Equal(expected, StateEdit.Classify(Stored(type, value)));
    }

    // ---- set ----

    [Fact]
    public void Set_on_a_missing_entry_is_refused_with_PZ0513()
    {
        var plan = StateEdit.ForSet(existing: null, rawValue: "5");

        Assert.Equal("PZ0513", plan.RefusalCode);
        Assert.Null(plan.NewValue);
    }

    [Fact]
    public void Set_canonicalizes_the_value_and_stamps_the_manual_marker()
    {
        var plan = StateEdit.ForSet(Stored(), "2026-07-01");

        Assert.Null(plan.RefusalCode);
        Assert.Equal("2026-07-01T00:00:00.000000", plan.NewValue!.Value);
        Assert.Equal("updated_at", plan.NewValue.Cursor);
        Assert.Equal("timestamp", plan.NewValue.TypeName);
        Assert.Equal("manual", plan.NewValue.RunId);
    }

    [Fact]
    public void Set_refuses_a_value_that_will_not_parse_for_the_stored_type()
    {
        var plan = StateEdit.ForSet(Stored("bigint", "500"), "yesterday");

        Assert.Equal("PZ0515", plan.RefusalCode);
        Assert.Contains("bigint", plan.RefusalMessage);
    }

    [Fact]
    public void Set_refuses_an_unknown_stored_type_and_points_at_clear()
    {
        var plan = StateEdit.ForSet(Stored("weirdtype", "whatever"), "5");

        Assert.Equal("PZ0515", plan.RefusalCode);
        Assert.Contains("pz state clear", plan.RefusalMessage);
    }

    [Fact]
    public void Set_over_a_non_canonical_stored_value_is_allowed_and_noted()
    {
        // The damage this verb exists to repair must not block its own repair.
        var plan = StateEdit.ForSet(Stored("timestamp", "garbage"), "2026-07-01");

        Assert.Null(plan.RefusalCode);
        Assert.Equal("2026-07-01T00:00:00.000000", plan.NewValue!.Value);
        Assert.Contains(plan.Notes, n => n.Contains("not canonical"));
    }

    [Fact]
    public void Set_forward_is_allowed_because_skipping_bad_source_data_is_a_real_need()
    {
        var plan = StateEdit.ForSet(Stored("bigint", "500"), "9000");

        Assert.Null(plan.RefusalCode);
        Assert.Equal("9000", plan.NewValue!.Value);
    }

    // ---- rollback ----

    [Fact]
    public void Rollback_uses_the_targets_value_and_stamps_the_manual_marker()
    {
        var plan = StateEdit.ForRollback(Stored(), "run-a",
            [Hist("run-a", "2026-07-01T00:00:00.000000")]);

        Assert.Null(plan.RefusalCode);
        Assert.Equal("2026-07-01T00:00:00.000000", plan.NewValue!.Value);
        Assert.Equal("manual", plan.NewValue.RunId);
    }

    [Fact]
    public void Rollback_to_an_unlisted_run_is_refused_with_PZ0514()
    {
        var plan = StateEdit.ForRollback(Stored(), "run-gone",
            [Hist("run-a", "2026-07-01T00:00:00.000000")]);

        Assert.Equal("PZ0514", plan.RefusalCode);
        Assert.Contains("run-gone", plan.RefusalMessage);
    }

    [Fact]
    public void Rollback_forward_is_refused_and_names_set_as_the_next_step()
    {
        var plan = StateEdit.ForRollback(Stored("bigint", "500"), "run-a",
            [Hist("run-a", "9000", type: "bigint")]);

        Assert.Equal("PZ0515", plan.RefusalCode);
        Assert.Contains("pz state set", plan.RefusalMessage);
    }

    [Fact]
    public void Rollback_to_a_run_with_an_unparseable_recorded_value_is_refused()
    {
        // run_results.json is hand-editable too, same as watermarks.json — a corrupted recorded value
        // must refuse cleanly, not throw.
        var plan = StateEdit.ForRollback(Stored(), "run-a", [Hist("run-a", "garbage")]);

        Assert.Equal("PZ0515", plan.RefusalCode);
        Assert.Contains("garbage", plan.RefusalMessage);
    }

    [Fact]
    public void Rollback_to_the_value_already_stored_is_refused_as_a_no_op()
    {
        var plan = StateEdit.ForRollback(Stored("bigint", "500"), "run-a",
            [Hist("run-a", "500", type: "bigint")]);

        Assert.Equal("PZ0515", plan.RefusalCode);
        Assert.Contains("already", plan.RefusalMessage);
    }

    [Fact]
    public void Rollback_over_a_non_canonical_stored_value_skips_the_direction_check_and_says_so()
    {
        var plan = StateEdit.ForRollback(Stored("timestamp", "garbage"), "run-a",
            [Hist("run-a", "2026-07-01T00:00:00.000000")]);

        Assert.Null(plan.RefusalCode);
        Assert.Contains(plan.Notes, n => n.Contains("direction"));
    }

    [Fact]
    public void Rollback_to_a_run_that_tracked_a_different_cursor_is_refused()
    {
        // That run's value measures a different column, so it says nothing about this cursor's position.
        var plan = StateEdit.ForRollback(Stored(), "run-a",
            [Hist("run-a", "2026-07-01T00:00:00.000000", cursor: "created_at")]);

        Assert.Equal("PZ0514", plan.RefusalCode);
        Assert.Contains("created_at", plan.RefusalMessage);
    }

    [Fact]
    public void Rollback_to_a_run_that_did_not_fully_succeed_is_allowed_and_flagged()
    {
        var plan = StateEdit.ForRollback(Stored(), "run-a",
            [Hist("run-a", "2026-07-01T00:00:00.000000", status: "completed_with_failures")]);

        Assert.Null(plan.RefusalCode);
        Assert.Contains(plan.Notes, n => n.Contains("did not fully succeed"));
    }

    [Fact]
    public void Rollback_on_a_missing_entry_is_refused_with_PZ0513()
    {
        var plan = StateEdit.ForRollback(existing: null, "run-a",
            [Hist("run-a", "2026-07-01T00:00:00.000000")]);

        Assert.Equal("PZ0513", plan.RefusalCode);
    }

    [Fact]
    public void Rollback_with_an_unknown_stored_type_is_refused_and_points_at_clear()
    {
        var plan = StateEdit.ForRollback(Stored("weirdtype", "whatever"), "run-a",
            [Hist("run-a", "5", type: "weirdtype")]);

        Assert.Equal("PZ0515", plan.RefusalCode);
        Assert.Contains("pz state clear", plan.RefusalMessage);
    }

    // ---- clear ----

    [Fact]
    public void Clear_removes_the_entry()
    {
        var plan = StateEdit.ForClear(Stored());

        Assert.Null(plan.RefusalCode);
        Assert.True(plan.RemoveEntry);
        Assert.Null(plan.NewValue);
    }

    [Theory]
    [InlineData("weirdtype", "whatever")]
    [InlineData("timestamp", "garbage")]
    public void Clear_works_on_every_broken_entry_because_it_is_their_only_remedy(string type, string value)
    {
        var plan = StateEdit.ForClear(Stored(type, value));

        Assert.Null(plan.RefusalCode);
        Assert.True(plan.RemoveEntry);
    }

    [Fact]
    public void Clear_on_a_missing_entry_is_refused_with_PZ0513()
    {
        Assert.Equal("PZ0513", StateEdit.ForClear(existing: null).RefusalCode);
    }
}
