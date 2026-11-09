using System.Text.Json;
using Pz.Mcp;
using Pz.Mcp.Handlers;

namespace Pz.Mcp.Tests;

public class VerifyToolsTests
{
    private static CliServices RealServices() => new()
    {
        CreateRegistryAsync = (project, dir, ct) =>
            Pz.Cli.ConnectorRegistryFactory.CreateAsync(project, dir, noLockCheck: false, ct),
        CreateStateStores = (_, _) => throw new InvalidOperationException("not needed for verify tools"),
        InitProject = (_, _, _) => throw new InvalidOperationException("not needed for verify tools"),
        RunAsync = (_, _) => throw new InvalidOperationException("not needed for verify tools"),
        RetryAsync = (_, _, _) => throw new InvalidOperationException("not needed for verify tools"),
    };

    [Fact]
    public async Task Compile_on_valid_project_returns_ok_with_dag()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await VerifyTools.CompileAsync(p.Dir, RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        var nodes = doc.RootElement.GetProperty("result").GetProperty("nodes");
        Assert.True(nodes.GetArrayLength() >= 3); // source load + 2 pipelines + sink write
    }

    [Fact]
    public async Task Compile_on_broken_project_returns_aggregate_errors()
    {
        using var p = new TempProject();
        // Every PZ0201 form now carries a next_step -- an unknown connection's is a declare-it-in-
        // connections.yml hint or a near miss; an unknown ref()'s is a near miss or a pointer at
        // pipelines/ (see Ref_to_a_missing_pipeline_also_carries_a_next_step below for that form).
        p.WritePipeline("bad", "SELECT * FROM {{ source('no_such_connection', 'x') }}\n");
        var doc = JsonDocument.Parse(await VerifyTools.CompileAsync(p.Dir, RealServices(), CancellationToken.None));
        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        var first = doc.RootElement.GetProperty("errors")[0];
        Assert.StartsWith("PZ", first.GetProperty("code").GetString());
        Assert.False(string.IsNullOrEmpty(first.GetProperty("next_step").GetString()));
    }

    [Fact]
    public async Task Ref_to_a_missing_pipeline_also_carries_a_next_step()
    {
        using var p = new TempProject();
        p.WritePipeline("bad", "SELECT * FROM {{ ref('nope') }}\n");
        var doc = JsonDocument.Parse(await VerifyTools.CompileAsync(p.Dir, RealServices(), CancellationToken.None));
        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        var first = doc.RootElement.GetProperty("errors")[0];
        Assert.Equal("PZ0201", first.GetProperty("code").GetString());
        Assert.False(string.IsNullOrEmpty(first.GetProperty("next_step").GetString()));
    }

    [Fact]
    public async Task Validate_offline_runs_tiers_1_to_4()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(
            await VerifyTools.ValidateAsync(p.Dir, connect: false, RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
    }

    [Fact]
    public async Task Plan_returns_strategy_per_node_and_no_sql_text()
    {
        using var p = new TempProject();
        var json = await VerifyTools.PlanAsync(p.Dir, RealServices(), CancellationToken.None);
        var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.DoesNotContain("SELECT", json); // secret/SQL hygiene: plan results carry no SQL text
    }

    // pz_validate opens connectors the way pz_plan does, anchored to the project: a relative path in the connection
    // resolves against the project, never the server's working directory.
    [Fact]
    public async Task Validate_resolves_relative_connection_paths_against_the_project_like_plan()
    {
        using var p = new TempProject();
        Directory.CreateDirectory(Path.Combine(p.Dir, "certs"));
        using (var key = System.Security.Cryptography.RSA.Create(2048))
        {
            var request = new System.Security.Cryptography.X509Certificates.CertificateRequest("CN=pz-test-ca", key,
                System.Security.Cryptography.HashAlgorithmName.SHA256, System.Security.Cryptography.RSASignaturePadding.Pkcs1);
            using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
            File.WriteAllText(Path.Combine(p.Dir, "certs", "ca.pem"), cert.ExportCertificatePem());
        }

        File.AppendAllText(Path.Combine(p.Dir, "connections.yml"),
            "\napi:\n  connector: http\n  base_url: https://api.example.com\n  tls: { ca_file: certs/ca.pem }\n" +
            "  entities:\n    items:\n      read:\n        path: /items\n");
        p.WritePipeline("items_copy", "select * from {{ source('api', 'items') }}\n");

        var validate = await VerifyTools.ValidateAsync(p.Dir, connect: false, RealServices(), CancellationToken.None);
        var plan = await VerifyTools.PlanAsync(p.Dir, RealServices(), CancellationToken.None);

        Assert.True(JsonDocument.Parse(plan).RootElement.GetProperty("ok").GetBoolean(), plan);
        Assert.True(JsonDocument.Parse(validate).RootElement.GetProperty("ok").GetBoolean(), validate);
    }
}
