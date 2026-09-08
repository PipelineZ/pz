using Pz.Connectors.TestKit.Reference;
using Pz.Core.Model;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary>A name must never be silently hijacked — registering a second source (or sink) under a name
/// that is already registered must throw, not last-write-wins overwrite. This is the engine-level half of
/// the guard; <c>Pz.Cli.ConnectorRegistryFactory</c> translates the same exception into a user-facing
/// PZ0305 <c>PzValidationException</c> when a hosted connector's name collides with a builtin's.</summary>
public sealed class ConnectorRegistryTests
{
    [Fact]
    public void AddSource_duplicate_name_throws()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("dup", new InMemoryConnector());

        var ex = Assert.Throws<InvalidOperationException>(() => registry.AddSource("dup", new InMemoryConnector()));
        Assert.Contains("dup", ex.Message);
    }

    [Fact]
    public void AddSink_duplicate_name_throws()
    {
        var registry = new ConnectorRegistry();
        registry.AddSink("dup", new InMemoryConnector());

        var ex = Assert.Throws<InvalidOperationException>(() => registry.AddSink("dup", new InMemoryConnector()));
        Assert.Contains("dup", ex.Message);
    }

    // Builtins register "localfiles" as both a source AND a sink under the same name
    // (BuiltinConnectors.CreateRegistry) — sources and sinks are separate dictionaries, so that must
    // keep working; only a same-dictionary collision (source-vs-source or sink-vs-sink) should throw.
    [Fact]
    public void AddSource_and_AddSink_under_the_same_name_do_not_collide_with_each_other()
    {
        var registry = new ConnectorRegistry();
        var connector = new InMemoryConnector();

        registry.AddSource("dup", connector);
        registry.AddSink("dup", connector); // must not throw

        Assert.True(registry.TryGetSource("dup", out _));
        Assert.True(registry.TryGetSink("dup", out _));
    }

    private static ConnectionDef Connection(string name, string connector) =>
        new(name, connector, new Dictionary<string, object?> { ["root"] = "/data" }, [], "connections.yml");

    // The connection name rides to an out-of-process host under the reserved key, so the host can name
    // the instance (pz.instance on its spans) after the connection rather than by ordinal. A builtin
    // gets the authored values and nothing else: it would receive the key as an unknown option.
    [Fact]
    public void ConfigFor_threads_the_connection_name_in_for_hosted_connectors_only()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("builtin", new InMemoryConnector());
        registry.AddSink("hosted", new InMemoryConnector(), hosted: true);

        var builtin = registry.ConfigFor(Connection("orders", "builtin"));
        Assert.Equal(["root"], builtin.Values.Keys);

        var hosted = registry.ConfigFor(Connection("orders", "hosted"));
        Assert.Equal("orders", hosted.GetString(ConnectorRegistry.InstanceIdKey));
        Assert.Equal("/data", hosted.GetString("root"));
    }

    // The key is host bookkeeping, never an authored option: whatever connections.yml says under it
    // is replaced by the connection's own name, and the authored dictionary itself is left untouched.
    [Fact]
    public void ConfigFor_overrides_an_authored_instance_key_without_mutating_the_definition()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("hosted", new InMemoryConnector(), hosted: true);
        var authored = new Dictionary<string, object?> { [ConnectorRegistry.InstanceIdKey] = "spoofed" };
        var connection = new ConnectionDef("orders", "hosted", authored, [], "connections.yml");

        var config = registry.ConfigFor(connection);

        Assert.Equal("orders", config.GetString(ConnectorRegistry.InstanceIdKey));
        Assert.Equal("spoofed", authored[ConnectorRegistry.InstanceIdKey]);
    }
}
