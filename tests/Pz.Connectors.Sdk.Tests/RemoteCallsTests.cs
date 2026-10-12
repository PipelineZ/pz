using System.Diagnostics;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk.Telemetry;

namespace Pz.Connectors.Sdk.Tests;

public sealed class RemoteCallsTests : IDisposable
{
    private readonly ActivitySource _source = new("Pz.Connector.test." + Guid.NewGuid().ToString("n"));
    private readonly List<Activity> _stopped = [];
    private readonly ActivityListener _listener;

    public RemoteCallsTests()
    {
        _listener = new ActivityListener
        {
            ShouldListenTo = s => ReferenceEquals(s, _source),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a => { lock (_stopped) { _stopped.Add(a); } },
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public void Dispose()
    {
        _listener.Dispose();
        _source.Dispose();
    }

    [Fact]
    public void Start_opens_a_remote_span_and_dispose_counts_one_request()
    {
        var acc = new ConnectorStatsAccumulator();
        using var parent = _source.StartActivity("parent")!;
        using (OperationScope.Enter(acc, new RemoteSpanBudget()))
        {
            new RemoteCalls(_source).Start("query").Dispose();
        }

        var span = Assert.Single(_stopped, a => a.OperationName == "remote.query");
        Assert.Equal("query", span.GetTagItem("pz.remote.operation"));
        Assert.Equal(parent.Id, span.ParentId);
        Assert.Equal(1, acc.Snapshot().Requests);
    }

    [Fact]
    public void Dispose_is_idempotent()
    {
        var acc = new ConnectorStatsAccumulator();
        using (OperationScope.Enter(acc, new RemoteSpanBudget()))
        {
            var call = new RemoteCalls(_source).Start("query");
            call.Dispose();
            call.Dispose();
        }

        Assert.Equal(1, acc.Snapshot().Requests);
    }

    [Fact]
    public void Call_methods_feed_the_current_operation_stats()
    {
        var acc = new ConnectorStatsAccumulator();
        using (OperationScope.Enter(acc, new RemoteSpanBudget()))
        {
            using var call = new RemoteCalls(_source).Start("query");
            call.WireBytes(10).RemoteMs(5).Throttled().Cost(2, "bytes_billed").Extra("bq.slot_ms", 3);
        }

        var s = acc.Snapshot();
        Assert.Equal(10, s.WireBytes);
        Assert.Equal(5, s.RemoteMs);
        Assert.Equal(1, s.Throttled);
        Assert.Equal(2, s.CostUnits);
        Assert.Equal("bytes_billed", s.CostUnit);
        Assert.Equal(new KeyValuePair<string, double>("bq.slot_ms", 3), Assert.Single(s.Extras));
    }

    [Fact]
    public void Fail_sets_error_status_and_type_never_message()
    {
        using (OperationScope.Enter(new ConnectorStatsAccumulator(), new RemoteSpanBudget()))
        {
            using var call = new RemoteCalls(_source).Start("query");
            call.Fail(new TimeoutException("secret"));
        }

        var span = Assert.Single(_stopped);
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal("System.TimeoutException", span.GetTagItem("error.type"));
        Assert.Null(span.StatusDescription);
        Assert.DoesNotContain(span.TagObjects, t => t.Value?.ToString()?.Contains("secret") == true);
    }

    [Theory]
    [InlineData("Bad Name")]
    [InlineData("")]
    [InlineData("1abc")]
    [InlineData("this_name_is_far_too_long_for_the_rule")]
    public void An_invalid_operation_name_falls_back_to_call(string name)
    {
        new RemoteCalls(_source).Start(name).Dispose();

        var span = Assert.Single(_stopped);
        Assert.Equal("remote.call", span.OperationName);
        Assert.Equal("call", span.GetTagItem("pz.remote.operation"));
    }

    [Fact]
    public void The_span_cap_holds_at_200_and_counts_drops()
    {
        var acc = new ConnectorStatsAccumulator();
        var budget = new RemoteSpanBudget();
        var remote = new RemoteCalls(_source);
        using (OperationScope.Enter(acc, budget))
        {
            for (var i = 0; i < 125; i++) remote.Start("query").Dispose();
        }

        // A second partition of the same operation shares the budget.
        using (OperationScope.Enter(acc, budget))
        {
            for (var i = 0; i < 125; i++) remote.Start("query").Dispose();
        }

        Assert.Equal(200, _stopped.Count(a => a.OperationName == "remote.query"));
        var s = acc.Snapshot();
        Assert.Equal(250, s.Requests);
        Assert.Equal(50, s.SpansDropped);
    }

    [Fact]
    public void Outside_an_operation_Start_still_works_and_records_nothing()
    {
        var call = new RemoteCalls(_source).Start("query");
        call.WireBytes(1).Throttled().Cost(1, "ru").Extra("a.b", 1);
        call.Fail(new InvalidOperationException());
        call.Dispose();

        Assert.True(ConnectorStats.Current.GetType().Name == "NoOp");
    }

    [Fact]
    public void Without_a_listener_nothing_is_dropped_or_opened()
    {
        using var unlistened = new ActivitySource("Pz.Connector.test.none." + Guid.NewGuid().ToString("n"));
        var acc = new ConnectorStatsAccumulator();
        using (OperationScope.Enter(acc, new RemoteSpanBudget()))
        {
            for (var i = 0; i < 250; i++) new RemoteCalls(unlistened).Start("query").Dispose();
        }

        var s = acc.Snapshot();
        Assert.Equal(250, s.Requests);
        Assert.Equal(0, s.SpansDropped);
    }
}
