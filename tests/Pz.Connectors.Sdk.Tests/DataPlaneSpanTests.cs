using System.Diagnostics;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

public sealed class DataPlaneSpanTests
{
    [Fact]
    public async Task A_read_stream_span_is_parented_on_the_ticket_context()
    {
        var sourceName = "pz-dp-" + Guid.NewGuid().ToString("N");
        var recorded = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == sourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a => { lock (recorded) { recorded.Add(a); } },
        };
        ActivitySource.AddActivityListener(listener);
        using var source = new ActivitySource(sourceName);

        ActivityContext parent;
        using (var rpc = source.StartActivity("pcp.OpenReadStream")!)
        {
            parent = rpc.Context;
        }

        var partition = new SyncPartition(3, prior: "0+3");
        var ticket = new ReadTicket(PlainSource.RowSchema, partition, BatchOptions.Default, CancellationToken.None, new SyncStateCapture(), parent);
        using var stream = new MemoryStream();

        await DataPlaneListener.ServeReadAsync(stream, ticket, source, CancellationToken.None);

        var span = Assert.Single(recorded, a => a.DisplayName == "pcp.read_stream");
        Assert.Equal(ActivityKind.Server, span.Kind);
        Assert.Equal(parent.TraceId, span.TraceId);
        Assert.Equal(parent.SpanId, span.ParentSpanId);
        Assert.True(stream.Length > 0);
    }
}
