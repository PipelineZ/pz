using Pz.Cli.Commands;

namespace Pz.Cli.Tests;

public sealed class CdcOutputTests
{
    private static readonly FakeClock Clock = new(new DateTimeOffset(2026, 10, 8, 9, 30, 0, 123, TimeSpan.Zero));

    [Fact]
    public void Json_status_line_has_the_documented_fields_in_order()
    {
        var stdout = new StringWriter();
        var output = CdcOutput.For("json", Clock, stdout, new StringWriter());

        output.Status(new CdcStatusRow("pg.orders", "postgres", true, false, "pz_pg_orders", "0/19DD530", 1_234_567,
            ["slot 'pz_pg_orders' is GONE", "EXEC sys.sp_cdc_disable_table @source_schema = N'dbo'"]));

        Assert.Equal(
            "{\"event\":\"cdc_status\",\"at\":\"2026-10-08T09:30:00.123Z\",\"dataset\":\"pg.orders\"," +
            "\"connector\":\"postgres\",\"adminSupported\":true,\"healthy\":false,\"positionName\":\"pz_pg_orders\"," +
            "\"hasStoredToken\":true,\"retainedBytes\":1234567,\"detail\":[\"slot \\u0027pz_pg_orders\\u0027 is GONE\"," +
            "\"EXEC sys.sp_cdc_disable_table @source_schema = N\\u0027dbo\\u0027\"]}\n",
            stdout.ToString());
    }

    [Fact]
    public void Json_status_for_unsupported_admin_has_null_health_and_keeps_the_token_flag()
    {
        var stdout = new StringWriter();
        CdcOutput.For("json", Clock, stdout, new StringWriter())
            .Status(new CdcStatusRow("files.orders", "localfiles", false, null, null, "tok", null, []));

        Assert.Equal(
            "{\"event\":\"cdc_status\",\"at\":\"2026-10-08T09:30:00.123Z\",\"dataset\":\"files.orders\"," +
            "\"connector\":\"localfiles\",\"adminSupported\":false,\"healthy\":null,\"positionName\":null," +
            "\"hasStoredToken\":true,\"retainedBytes\":null,\"detail\":[]}\n",
            stdout.ToString());
    }

    [Fact]
    public void Json_never_emits_the_stored_token_value()
    {
        var stdout = new StringWriter();
        CdcOutput.For("json", Clock, stdout, new StringWriter())
            .Status(new CdcStatusRow("pg.orders", "postgres", true, true, "s", "SECRET-LSN-VALUE", 0, []));

        Assert.DoesNotContain("SECRET-LSN-VALUE", stdout.ToString());
    }

    [Fact]
    public void Json_dropped_line_for_sqlserver_carries_remediation()
    {
        var stdout = new StringWriter();
        CdcOutput.For("json", Clock, stdout, new StringWriter()).Dropped(new CdcDropSummary(
            "ops.orders", "sqlserver", "dbo_orders", false,
            ["EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'orders', @capture_instance = N'dbo_orders';"],
            LocalState: false));

        Assert.Equal(
            "{\"event\":\"cdc_dropped\",\"at\":\"2026-10-08T09:30:00.123Z\",\"dataset\":\"ops.orders\"," +
            "\"connector\":\"sqlserver\",\"positionName\":\"dbo_orders\",\"serverSideDropped\":false," +
            "\"remediation\":[\"EXEC sys.sp_cdc_disable_table @source_schema = N\\u0027dbo\\u0027, @source_name = " +
            "N\\u0027orders\\u0027, @capture_instance = N\\u0027dbo_orders\\u0027;\"],\"stateCleared\":\"remote\"}\n",
            stdout.ToString());
    }

    [Fact]
    public void Json_notes_go_to_stderr_and_text_notes_to_stdout()
    {
        var jsonOut = new StringWriter();
        var jsonErr = new StringWriter();
        CdcOutput.For("json", Clock, jsonOut, jsonErr).Note("note: state backend: http");
        Assert.Equal("", jsonOut.ToString());
        Assert.Contains("note: state backend: http", jsonErr.ToString());

        var textOut = new StringWriter();
        CdcOutput.For("text", Clock, textOut, new StringWriter()).Note("note: state backend: http");
        Assert.Contains("note: state backend: http", textOut.ToString());
    }

    [Fact]
    public void Text_status_row_is_unchanged_from_the_table_format()
    {
        var stdout = new StringWriter();
        var output = CdcOutput.For("text", Clock, stdout, new StringWriter());
        output.StatusHeader();
        output.Status(new CdcStatusRow("pg.orders", "postgres", true, false, "pz_pg_orders", "0/19DD530", 42, ["fix it"]));

        Assert.Equal(
            $"{"dataset",-28} {"position",-20} {"stored token",-20} {"retained",-12} health\n" +
            $"{"pg.orders",-28} {"pz_pg_orders",-20} {"0/19DD530",-20} {"42",-12} unhealthy\n" +
            "    fix it\n",
            stdout.ToString().Replace("\r\n", "\n"));
    }

    private sealed class FakeClock(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
