using Pz.Shared;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>Offline, no-docker unit tests for <see cref="MsTransient.IsTransient"/> -- pure and
/// deterministic over synthetic <see cref="Microsoft.Data.SqlClient.SqlException"/> instances built by
/// <see cref="SqlExceptionFactory"/> (see <c>MsTransientDockerTests</c> for the real-driver proof that
/// a genuine deadlock/timeout land on these exact numbers with <c>SqlException.IsTransient == false</c>).</summary>
public sealed class MsTransientTests
{
    [Theory]
    [InlineData(1205)] // deadlock victim
    [InlineData(-2)] // client-side command timeout
    [InlineData(233)] // pre-login transport drop (named pipes)
    [InlineData(64)] // pre-login transport drop (TCP)
    [InlineData(10053)]
    [InlineData(10054)]
    [InlineData(10060)]
    [InlineData(40613)] // Azure SQL: database unavailable
    [InlineData(40197)]
    [InlineData(40501)]
    [InlineData(49918)]
    [InlineData(49919)]
    [InlineData(49920)]
    public void Known_transient_number_is_transient(int number)
    {
        var ex = SqlExceptionFactory.Create(number);
        Assert.False(ex.IsTransient); // the driver's own signal misses every one of these
        Assert.True(MsTransient.IsTransient(ex));
    }

    [Theory]
    [InlineData(208)] // invalid object name
    [InlineData(2627)] // PK violation
    [InlineData(18456)] // login failed
    [InlineData(229)] // permission denied
    public void Permanent_number_is_not_transient(int number)
    {
        var ex = SqlExceptionFactory.Create(number);
        Assert.False(MsTransient.IsTransient(ex));
    }
}
