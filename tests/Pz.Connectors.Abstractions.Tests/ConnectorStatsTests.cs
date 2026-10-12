using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connectors.Abstractions.Tests;

public sealed class ConnectorStatsTests
{
    [Fact]
    public void Current_outside_an_operation_is_a_no_op_and_never_null()
    {
        var current = ConnectorStats.Current;
        Assert.NotNull(current);
        current.AddRequests(5); // must not throw
    }

    [Fact]
    public async Task Current_flows_into_an_async_iterator()
    {
        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            await foreach (var _ in Pages()) { }
        }
        Assert.Equal(3, acc.Snapshot().Requests);
        Assert.NotSame(acc, ConnectorStats.Current);

        static async IAsyncEnumerable<int> Pages()
        {
            for (var i = 0; i < 3; i++)
            {
                await Task.Yield();
                ConnectorStats.Current.AddRequests(1);
                yield return i;
            }
        }
    }

    [Fact]
    public void Use_restores_the_previous_handle()
    {
        var outer = new ConnectorStatsAccumulator();
        var inner = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(outer))
        {
            using (ConnectorStats.Use(inner)) ConnectorStats.Current.AddRequests(1);
            ConnectorStats.Current.AddRequests(2);
        }
        Assert.Equal(1, inner.Snapshot().Requests);
        Assert.Equal(2, outer.Snapshot().Requests);
    }

    [Fact]
    public void Concurrent_adds_sum_exactly()
    {
        var acc = new ConnectorStatsAccumulator();
        Parallel.For(0, 10_000, _ =>
        {
            acc.AddRequests(1);
            acc.AddWireBytes(10);
            acc.AddCost(0.5, "ru");
            acc.AddExtra("x.pages", 1);
        });
        var s = acc.Snapshot();
        Assert.Equal(10_000, s.Requests);
        Assert.Equal(100_000, s.WireBytes);
        Assert.Equal(5_000, s.CostUnits);
        Assert.Equal(10_000, Assert.Single(s.Extras).Value);
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(-1.0)]
    public void Negative_or_non_finite_values_are_dropped(double bad)
    {
        var acc = new ConnectorStatsAccumulator();
        acc.AddRequests(-1);
        acc.AddCost(bad, "ru");
        acc.AddExtra("x.y", bad);
        var s = acc.Snapshot();
        Assert.True(s.IsEmpty);
        Assert.Equal(StatsDrop.NegativeOrNonFinite, acc.Drops);
    }

    [Theory]
    [InlineData("Cost Unit")]
    [InlineData("1ru")]
    [InlineData("")]
    [InlineData("abcdefghijklmnopqrstuvwxy")] // 25 chars
    public void Invalid_cost_units_are_dropped(string unit)
    {
        var acc = new ConnectorStatsAccumulator();
        acc.AddCost(1, unit);
        Assert.Null(acc.Snapshot().CostUnit);
        Assert.Equal(StatsDrop.InvalidCostUnit, acc.Drops);
    }

    [Fact]
    public void The_first_cost_unit_wins()
    {
        var acc = new ConnectorStatsAccumulator();
        acc.AddCost(2, "ru");
        acc.AddCost(3, "credits");
        acc.AddCost(1, "ru");
        var s = acc.Snapshot();
        Assert.Equal(3, s.CostUnits);
        Assert.Equal("ru", s.CostUnit);
        Assert.Equal(StatsDrop.SecondCostUnit, acc.Drops);
    }

    [Theory]
    [InlineData("pages")]
    [InlineData("X.pages")]
    [InlineData("x.")]
    [InlineData("x.Pages")]
    [InlineData("abcdefghijklmnopqrst.abcdefghijklmnopqrst")] // 41 chars
    public void Invalid_extra_names_are_dropped(string name)
    {
        var acc = new ConnectorStatsAccumulator();
        acc.AddExtra(name, 1);
        Assert.Empty(acc.Snapshot().Extras);
        Assert.Equal(StatsDrop.InvalidExtraName, acc.Drops);
    }

    [Fact]
    public void At_most_eight_extras_and_they_are_ordered_ordinally()
    {
        var acc = new ConnectorStatsAccumulator();
        for (var i = 9; i >= 1; i--) acc.AddExtra($"x.e{i}", i);
        acc.AddExtra("x.e1", 1); // a name dropped once stays dropped
        var s = acc.Snapshot();
        Assert.Equal(8, s.Extras.Count);
        Assert.Equal(["x.e2", "x.e3", "x.e4", "x.e5", "x.e6", "x.e7", "x.e8", "x.e9"], s.Extras.Select(e => e.Key));
        Assert.Equal(StatsDrop.TooManyExtras, acc.Drops);
    }

    [Fact]
    public void Add_merges_a_snapshot_through_validation()
    {
        var a = new ConnectorStatsAccumulator();
        a.AddCost(1, "ru");
        var b = new ConnectorStatsAccumulator();
        b.AddRequests(2); b.AddCost(5, "credits"); b.AddSpansDropped(3); b.AddExtra("x.y", 4);
        a.Add(b.Snapshot());
        var s = a.Snapshot();
        Assert.Equal(2, s.Requests);
        Assert.Equal(1, s.CostUnits);
        Assert.Equal(3, s.SpansDropped);
        Assert.Equal(StatsDrop.SecondCostUnit, a.Drops);
    }

    [Fact]
    public void ReportsStats_is_a_new_capability_bit()
    {
        Assert.Equal(1 << 22, (int)ConnectorCapabilities.ReportsStats);
    }
}
