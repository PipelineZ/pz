using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Tests.ProcessHosting;

/// <summary>A connector is spawned once per open, and every spawn re-runs the handshake that reports
/// an unrecognized capability -- so the warning channel a lazy connector hands to each handshake has
/// to drop a message it has already delivered, or a run prints it once per node.</summary>
public sealed class OnceOnlyWarningsTests
{
    [Fact]
    public void A_repeated_message_is_delivered_once_and_a_new_one_still_gets_through()
    {
        var delivered = new List<string>();
        var warn = LazyProcessConnector.OnceOnly(delivered.Add)!;

        warn("unknown capability bits (0x8000)");
        warn("unknown capability bits (0x8000)");
        warn("unknown capability names (Teleport)");

        Assert.Equal(["unknown capability bits (0x8000)", "unknown capability names (Teleport)"], delivered);
    }

    [Fact]
    public void No_sink_stays_no_sink()
    {
        Assert.Null(LazyProcessConnector.OnceOnly(null));
    }
}
