using Pz.Connectors.TestKit.Reference;
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
}
