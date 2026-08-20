namespace Pz.Mcp.Tests;

/// <summary>A minimal self-contained project (no docker, no network) for exercising the verify tools
/// (pz_compile/pz_validate/pz_plan) end to end. Spellings verified against templates/sample's real
/// connections.yml/pipelines/*.sql:
/// connections.yml has NO top-level `connections:` wrapper and NO nested `connection:` key — a
/// connection's own name is the top-level YAML key, and connector options (e.g. `root`) sit directly
/// under it. `sink()`'s pz-owned disposition kwarg is `strategy`, not `mode`.</summary>
public sealed class TempProject : IDisposable
{
    public string Dir { get; } = Path.Combine(Path.GetTempPath(), "pz-mcp-" + Guid.NewGuid().ToString("N"));

    public TempProject()
    {
        Directory.CreateDirectory(Path.Combine(Dir, "pipelines"));
        Directory.CreateDirectory(Path.Combine(Dir, "data"));
        File.WriteAllText(Path.Combine(Dir, "project.yml"), "name: mcp_test\nversion: \"0.1.0\"\n");
        File.WriteAllText(Path.Combine(Dir, "data", "orders.csv"), "id,amount\n1,10\n2,20\n");
        File.WriteAllText(Path.Combine(Dir, "connections.yml"),
            """
            raw:
              connector: localfiles
              entities:
                orders:
                  read:
                    path: data/orders.csv
                    format: csv

            out:
              connector: localfiles
              root: out
            """ + "\n");
        WritePipeline("stg_orders",
            "select id, amount\nfrom {{ source('raw', 'orders') }}\n");
        WritePipeline("orders_out",
            "INSERT INTO {{ sink('out', 'orders_out', format: 'csv', strategy: 'replace') }}\n" +
            "select * from {{ ref('stg_orders') }}\n");
    }

    public void WritePipeline(string name, string sql) =>
        File.WriteAllText(Path.Combine(Dir, "pipelines", name + ".sql"), sql);

    public void Dispose()
    {
        try { Directory.Delete(Dir, recursive: true); } catch { /* best-effort cleanup */ }
    }
}
