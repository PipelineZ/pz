using System.Globalization;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Protocol.V1;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

public sealed class HostLoggerTests
{
    /// <summary>A structured property's value must render the same regardless of the connector
    /// process's current culture -- th-TH's default calendar is Buddhist and fi-FI uses '.' as its
    /// decimal separator, either of which would otherwise corrupt a logged double/DateTime en route to
    /// the host.</summary>
    [Fact]
    public async Task Structured_property_values_render_invariant_culture()
    {
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fi-FI");
            var peer = new HostChannelPeer();
            var logger = new HostLoggerProvider(peer).CreateLogger("x");
            logger.LogInformation("amount {Amount}", 1234.5);

            var writer = new AwaitableStreamWriter(expected: 1);
            peer.Attach(writer);
            await writer.Done.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Equal("1234.5", writer.Written[0].Log.Fields["Amount"]);
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    /// <summary>An exception logged alongside a message must carry its own message onto the wire, not
    /// just its type name -- the type name alone gives an operator nothing to act on.</summary>
    [Fact]
    public async Task Exception_message_and_stack_trace_reach_the_wire()
    {
        var peer = new HostChannelPeer();
        var logger = new HostLoggerProvider(peer).CreateLogger("x");
        Exception caught;
        try
        {
            throw new InvalidOperationException("connection refused");
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        logger.LogError(caught, "read failed");

        var writer = new AwaitableStreamWriter(expected: 1);
        peer.Attach(writer);
        await writer.Done.WaitAsync(TimeSpan.FromSeconds(10));

        var fields = writer.Written[0].Log.Fields;
        Assert.Equal("System.InvalidOperationException", fields["exception"]);
        Assert.Equal("connection refused", fields["exceptionMessage"]);
        Assert.Contains("Exception_message_and_stack_trace_reach_the_wire", fields["exceptionStackTrace"]);
    }

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

    [Fact]
    public async Task A_log_produced_while_the_backlog_is_flushing_lands_after_it()
    {
        var peer = new HostChannelPeer();
        var logger = new HostLoggerProvider(peer).CreateLogger("x");
        logger.LogInformation("held one");
        logger.LogInformation("held two");

        var writer = new AwaitableStreamWriter(expected: 3)
        {
            OnFirstWrite = () => logger.LogInformation("live"),
        };
        peer.Attach(writer);
        await writer.Done.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(
            ["held one", "held two", "live"], writer.Written.Select(m => m.Log.Message).ToArray());
    }

    private sealed class AwaitableStreamWriter(int expected) : IServerStreamWriter<HostChannelUp>
    {
        private readonly TaskCompletionSource _done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public List<HostChannelUp> Written { get; } = [];
        public Task Done => _done.Task;
        public WriteOptions? WriteOptions { get; set; }

        /// <summary>Runs once the first message is on the wire -- the window in which a live log
        /// could overtake the backlog being flushed.</summary>
        public Action? OnFirstWrite { get; set; }

        public Task WriteAsync(HostChannelUp message)
        {
            Action? first = null;
            lock (Written)
            {
                Written.Add(message);
                if (Written.Count == 1)
                {
                    first = OnFirstWrite;
                }

                if (Written.Count == expected)
                {
                    _done.TrySetResult();
                }
            }

            first?.Invoke();
            return Task.CompletedTask;
        }
    }
}
