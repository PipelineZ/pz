using Pz.Diagnostics.Events;
using Pz.Engine.Events;

namespace Pz.Cli.Tests;

public sealed class ConnectorLogRelayTests
{
    [Fact]
    public async Task Every_line_becomes_an_event_and_only_warn_and_above_is_said_to_the_person_watching()
    {
        var notes = new List<string>();
        var bus = new RunEventBus();
        var relay = new ConnectorLogRelay(notes.Add) { Events = new RunEventPublisher(bus, "run-1", TimeProvider.System) };

        relay.Log("erp", "info", "connected");
        relay.Log("erp", "warn", "TLS verification is off");
        relay.Log("erp", "error", "poll failed");

        Assert.Equal(3, (await Drain(bus)).Count);
        Assert.Equal(["erp: [warn] TLS verification is off", "erp: [error] poll failed"], notes);
    }

    // A connector process is started once per node, so whatever it says about its connection arrives
    // once per entity read through it. The stream keeps every line; a person reads it once.
    [Fact]
    public async Task A_line_repeated_by_every_node_of_a_connection_is_said_once_but_recorded_every_time()
    {
        var notes = new List<string>();
        var bus = new RunEventBus();
        var relay = new ConnectorLogRelay(notes.Add) { Events = new RunEventPublisher(bus, "run-1", TimeProvider.System) };

        relay.Log("erp", "warn", "TLS verification is off");
        relay.Log("erp", "warn", "TLS verification is off");
        relay.Log("crm", "warn", "TLS verification is off");

        Assert.Equal(3, (await Drain(bus)).Count);
        Assert.Equal(["erp: [warn] TLS verification is off", "crm: [warn] TLS verification is off"], notes);
    }

    [Fact]
    public void A_level_this_build_does_not_know_is_said_rather_than_hidden()
    {
        var notes = new List<string>();
        var relay = new ConnectorLogRelay(notes.Add);

        relay.Log("erp", "unknown", "something new");

        Assert.Equal(["erp: [unknown] something new"], notes);
    }

    private static async Task<List<ConnectorLogEvent>> Drain(RunEventBus bus)
    {
        bus.Complete();
        var logged = new List<ConnectorLogEvent>();
        await foreach (var evt in bus.ReadAllAsync())
        {
            if (evt is ConnectorLogEvent log)
            {
                logged.Add(log);
            }
        }

        return logged;
    }
}
