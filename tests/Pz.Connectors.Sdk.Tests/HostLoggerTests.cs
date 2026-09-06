using Grpc.Core;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Protocol.V1;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

public sealed class HostLoggerTests
{
    [Fact]
    public async Task Events_before_attach_are_held_in_order_and_flushed_on_attach()
    {
        var peer = new HostChannelPeer();
        var logger = new HostLoggerProvider(peer).CreateLogger("Kafka");
        logger.LogInformation("first {Topic}", "orders");
        logger.LogWarning("second");

        var writer = new AwaitableStreamWriter(expected: 2);
        peer.Attach(writer);
        await writer.Done.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(["first orders", "second"], writer.Written.Select(m => m.Log.Message).ToArray());
        Assert.Equal((int)LogLevel.Information, writer.Written[0].Log.Level);
        Assert.Equal("orders", writer.Written[0].Log.Fields["Topic"]);
        Assert.Equal("Kafka", writer.Written[0].Log.Fields["category"]);
        Assert.False(writer.Written[0].Log.Fields.ContainsKey("{OriginalFormat}"));
    }

    [Fact]
    public async Task The_backlog_is_bounded_and_keeps_the_newest()
    {
        var peer = new HostChannelPeer();
        var logger = new HostLoggerProvider(peer).CreateLogger("x");
        for (var i = 0; i < 300; i++)
        {
            logger.LogInformation("{N}", i);
        }

        var writer = new AwaitableStreamWriter(expected: 256);
        peer.Attach(writer);
        await writer.Done.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(256, writer.Written.Count);
        Assert.Equal("44", writer.Written[0].Log.Message);
        Assert.Equal("299", writer.Written[^1].Log.Message);
    }

    private sealed class AwaitableStreamWriter(int expected) : IServerStreamWriter<HostChannelUp>
    {
        private readonly TaskCompletionSource _done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public List<HostChannelUp> Written { get; } = [];
        public Task Done => _done.Task;
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(HostChannelUp message)
        {
            lock (Written)
            {
                Written.Add(message);
                if (Written.Count == expected)
                {
                    _done.TrySetResult();
                }
            }

            return Task.CompletedTask;
        }
    }
}
