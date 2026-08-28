using Snowflake.Data.Client;

namespace Pz.Connector.Snowflake.Tests;

public class SfErrorsTests
{
    [Fact]
    public void Timeout_is_transient() =>
        Assert.True(SfErrors.IsTransient(new TimeoutException()));

    [Fact]
    public void Io_is_transient() =>
        Assert.True(SfErrors.IsTransient(new IOException()));

    [Fact]
    public void Http_request_is_transient() =>
        Assert.True(SfErrors.IsTransient(new HttpRequestException()));

    [Fact]
    public void Task_canceled_wrapping_timeout_is_transient() =>
        Assert.True(SfErrors.IsTransient(new TaskCanceledException("timed out", new TimeoutException())));

    [Fact]
    public void Plain_invalid_operation_is_not_transient() =>
        Assert.False(SfErrors.IsTransient(new InvalidOperationException()));

    [Fact]
    public void Invalid_operation_wrapping_io_recurses_to_transient() =>
        Assert.True(SfErrors.IsTransient(new InvalidOperationException("wrapped", new IOException())));

    [Fact]
    public void Snowflake_connection_sqlstate_is_transient()
    {
        var ex = new SnowflakeDbException("08006", 12345, "connection failed", "query-1");
        Assert.True(SfErrors.IsTransient(ex));
    }

    [Fact]
    public void Snowflake_non_connection_sqlstate_is_not_transient()
    {
        var ex = new SnowflakeDbException("22000", 100001, "compilation error", "query-2");
        Assert.False(SfErrors.IsTransient(ex));
    }
}
