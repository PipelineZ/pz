using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary><c>variant:</c> end to end through <c>pz run</c>: a JSON file landed whole as <c>record</c>, and a
/// CSV column of JSON text converted after the land, both read field by field in pipeline SQL; a later file
/// with new fields still lands; text that is not JSON fails PZ0547 counting rows, never quoting them.</summary>
[Collection("console-and-env-serialized")]
public sealed class VariantLandingRunTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-variant-run-tests", Guid.NewGuid().ToString("N"));

    public VariantLandingRunTests()
    {
        Directory.CreateDirectory(Path.Combine(_work, "data"));
        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));
        File.WriteAllText(Path.Combine(_work, "project.yml"),
            "name: variant_demo\nversion: 0.1.0\n\nconnectors:\n  - package: Pz.Connector.LocalFiles\n    version: 0.1.0\n");
        File.WriteAllText(Path.Combine(_work, "connections.yml"), """
            raw:
              connector: localfiles
              entities:
                events:
                  read:
                    path: data/events.ndjson
                    format: json
                    variant: [record]
                calls:
                  read:
                    path: data/calls.csv
                    format: csv
                    variant: [payload]

            lake:
              connector: localfiles
            """);
        File.WriteAllText(Path.Combine(_work, "data", "events.ndjson"),
            "{\"id\":1,\"user\":{\"name\":\"ann\",\"tier\":\"gold\"},\"tags\":[\"a\",\"b\"]}\n" +
            "{\"id\":2,\"user\":{\"name\":\"bob\"},\"amount\":\"7\"}\n");
        File.WriteAllText(Path.Combine(_work, "data", "calls.csv"),
            "id,payload\n1,\"{\"\"status\"\":200,\"\"body\"\":{\"\"ok\"\":true}}\"\n2,\"{\"\"status\"\":500}\"\n");
        File.WriteAllText(Path.Combine(_work, "pipelines", "event_users.sql"), """
            INSERT INTO {{ sink('lake', 'event_users', strategy: 'replace', format: 'csv', path: 'out/events/') }}
            select record.id::BIGINT as id, record.user.name::VARCHAR as name, record.user.tier::VARCHAR as tier,
                   record.tags[1]::VARCHAR as first_tag
            from {{ source('raw', 'events') }}
            order by id
            """);
        File.WriteAllText(Path.Combine(_work, "pipelines", "call_status.sql"), """
            INSERT INTO {{ sink('lake', 'call_status', strategy: 'replace', format: 'csv', path: 'out/calls/') }}
            select id, payload.status::INTEGER as status, payload.body.ok::BOOLEAN as ok
            from {{ source('raw', 'calls') }}
            order by id
            """);
    }

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    /// <summary>Exit code and everything the run printed: config errors go to stderr, node failures to the
    /// run report on stdout.</summary>
    private (int Exit, string Stderr) Run()
    {
        var output = new StringWriter();
        var (originalErr, originalOut) = (Console.Error, Console.Out);
        Console.SetError(output);
        Console.SetOut(output);
        try
        {
            return (CliApp.Build().Parse(["run", "--all", "--project", _work]).Invoke(), output.ToString());
        }
        finally
        {
            Console.SetError(originalErr);
            Console.SetOut(originalOut);
        }
    }

    private string Output(string dir) =>
        File.ReadAllText(Directory.GetFiles(Path.Combine(_work, "out", dir), "*.csv").Single()).ReplaceLineEndings("\n");

    [Fact]
    public void Json_records_and_a_json_text_column_land_as_variant_and_read_by_field()
    {
        var (exit, stderr) = Run();

        Assert.True(exit == ExitCodes.Ok, stderr);
        Assert.Equal("id,name,tier,first_tag\n1,ann,gold,a\n2,bob,,\n", Output("events"));
        Assert.Equal("id,status,ok\n1,200,true\n2,500,\n", Output("calls"));
    }

    [Fact]
    public void A_record_with_new_fields_is_not_drift()
    {
        Assert.Equal(ExitCodes.Ok, Run().Exit);
        File.AppendAllText(Path.Combine(_work, "data", "events.ndjson"),
            "{\"id\":3,\"user\":{\"name\":\"cy\",\"tier\":\"silver\",\"age\":40},\"new_field\":[1]}\n");

        var (exit, stderr) = Run();

        Assert.True(exit == ExitCodes.Ok, stderr);
        Assert.EndsWith("3,cy,silver,\n", Output("events"), StringComparison.Ordinal);
    }

    [Fact]
    public void Text_that_is_not_json_fails_PZ0547_counting_rows_without_quoting_them()
    {
        File.WriteAllText(Path.Combine(_work, "data", "calls.csv"),
            "id,payload\n1,\"{\"\"status\"\":200}\"\n2,SECRET-NOT-JSON\n3,also-not-json\n");

        var (exit, stderr) = Run();

        Assert.Equal(ExitCodes.NodeFailures, exit);
        Assert.Contains("PZ0547", stderr, StringComparison.Ordinal);
        Assert.Contains("2 row(s) of 'payload' hold text that is not JSON", stderr, StringComparison.Ordinal);
        Assert.DoesNotContain("SECRET-NOT-JSON", stderr, StringComparison.Ordinal);
    }

    [Fact]
    public void A_variant_column_the_read_did_not_land_is_PZ0547()
    {
        File.WriteAllText(Path.Combine(_work, "connections.yml"),
            File.ReadAllText(Path.Combine(_work, "connections.yml")).Replace("variant: [payload]", "variant: [body]"));

        var (exit, stderr) = Run();

        Assert.Equal(ExitCodes.NodeFailures, exit);
        Assert.Contains("PZ0547", stderr, StringComparison.Ordinal);
        Assert.Contains("'body', which the read did not land (it landed id, payload)", stderr, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("variant: payload", "takes a list of column names")]
    [InlineData("variant: [payload, PAYLOAD]", "names 'PAYLOAD' twice")]
    public void A_malformed_variant_option_is_PZ0367(string option, string expected)
    {
        File.WriteAllText(Path.Combine(_work, "connections.yml"),
            File.ReadAllText(Path.Combine(_work, "connections.yml")).Replace("variant: [payload]", option));

        var (exit, stderr) = Run();

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0367", stderr, StringComparison.Ordinal);
        Assert.Contains(expected, stderr, StringComparison.Ordinal);
    }

    [Fact]
    public void The_kwarg_surface_declares_it_too()
    {
        // Never both surfaces (PZ0341): the whole read moves to the call site.
        File.WriteAllText(Path.Combine(_work, "connections.yml"),
            File.ReadAllText(Path.Combine(_work, "connections.yml")).Replace(
                "\n    calls:\n      read:\n        path: data/calls.csv\n        format: csv\n        variant: [payload]", ""));
        File.WriteAllText(Path.Combine(_work, "pipelines", "call_status.sql"),
            File.ReadAllText(Path.Combine(_work, "pipelines", "call_status.sql")).Replace("source('raw', 'calls')",
                "source('raw', 'calls', path: 'data/calls.csv', format: 'csv', variant: ['payload'])"));

        var (exit, stderr) = Run();

        Assert.True(exit == ExitCodes.Ok, stderr);
        Assert.Equal("id,status,ok\n1,200,true\n2,500,\n", Output("calls"));
    }

    [Fact]
    public void Naming_the_incremental_cursor_is_PZ0367()
    {
        File.WriteAllText(Path.Combine(_work, "connections.yml"),
            File.ReadAllText(Path.Combine(_work, "connections.yml")).Replace("variant: [payload]",
                "variant: [payload, id]\n        columns:\n          id: bigint\n          payload: varchar\n" +
                "        sync:\n          mode: incremental\n          cursor: id"));

        var (exit, stderr) = Run();

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0367", stderr, StringComparison.Ordinal);
        Assert.Contains("names the incremental cursor 'id'", stderr, StringComparison.Ordinal);
    }

    [Fact]
    public void Record_with_a_columns_contract_is_refused()
    {
        File.WriteAllText(Path.Combine(_work, "connections.yml"),
            File.ReadAllText(Path.Combine(_work, "connections.yml")).Replace("variant: [record]",
                "variant: [record]\n        columns:\n          id: bigint"));

        var (exit, stderr) = Run();

        Assert.NotEqual(ExitCodes.Ok, exit);
        Assert.Contains("variant: [record] lands each object whole, so it takes no columns: contract", stderr,
            StringComparison.Ordinal);
    }
}
