using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Tests.ProcessHosting;

/// <summary>A test that is not about the handshake timeout must not race it: the fixture is a .NET process whose cold
/// start, with every other test host running in parallel, can outlast the production 15 s budget. This assembly's
/// module initializer lifts the default budget; the timeout's own tests pass an explicit short one.</summary>
public sealed class HandshakeBudgetTests
{
    [Fact]
    public void The_default_handshake_budget_does_not_race_a_loaded_machine_in_this_assembly()
    {
        Assert.Equal(Timeout.InfiniteTimeSpan, PcpClient.DefaultHandshakeTimeout);
    }
}
