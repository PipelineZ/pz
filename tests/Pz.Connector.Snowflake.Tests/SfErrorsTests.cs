using Snowflake.Data.Client;
using Snowflake.Data.Core;

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

    [Fact]
    public void Empty_sqlstate_snowflake_exception_wrapping_io_is_transient()
    {
        // This is the driver's own shape for a client-side network failure: this ctor sets no
        // SqlState at all, so the arm must fall through to the InnerException recursion rather than
        // short-circuit on IsTransientSnowflakeCode alone.
        var ex = new SnowflakeDbException(SFError.INTERNAL_ERROR, "query-3", new IOException("boom"));
        Assert.True(SfErrors.IsTransient(ex));
    }

    [Fact]
    public void Empty_sqlstate_snowflake_exception_with_unlisted_code_and_no_inner_is_not_transient()
    {
        var ex = new SnowflakeDbException(string.Empty, 100001, "some non-network failure", "query-4");
        Assert.False(SfErrors.IsTransient(ex));
    }

    [Theory]
    [InlineData(270007)] // REQUEST_TIMEOUT
    [InlineData(270058)] // IO_ERROR_ON_GETPUT_COMMAND (PUT/GET transfer failure)
    public void Client_side_network_vendor_codes_are_transient_despite_empty_sqlstate(int vendorCode)
    {
        var ex = new SnowflakeDbException(string.Empty, vendorCode, "client-side network failure", "query-5");
        Assert.True(SfErrors.IsTransient(ex));
    }
}
