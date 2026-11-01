using System.Text;

namespace Pz.Cli.Tests;

/// <summary>The project the preview tests share: one CSV source whose contract marks <c>email</c>, a table
/// pipeline that renames a copy of it (<c>contact</c>, marked in its sidecar, along with a check), a view,
/// an ephemeral pipeline, and a sink fed by the ephemeral one -- every node kind <c>pz preview</c> has to
/// tell apart.</summary>
internal static class PreviewFixture
{
    internal const string Source = "src_files__customers";

    internal static string Write(string dir, int rows = 1500)
    {
        Directory.CreateDirectory(Path.Combine(dir, "pipelines", "configs"));
        Directory.CreateDirectory(Path.Combine(dir, "data"));

        File.WriteAllText(Path.Combine(dir, "project.yml"), """
            name: preview_basic
            version: 0.1.0
            connectors:
              - package: Pz.Connector.LocalFiles
                version: 0.1.0
            engine:
              threads: 1
            """);
        File.WriteAllText(Path.Combine(dir, "connections.yml"), """
            files:
              connector: localfiles
              entities:
                customers:
                  read:
                    path: data/customers.csv
                    format: csv
                    columns:
                      id: bigint
                      email: { type: varchar, redact: true }
                      note: varchar
            lake:
              connector: localfiles
            """);

        File.WriteAllText(Path.Combine(dir, "pipelines", "stg_customers.sql"),
            "select id, email, lower(email) as contact, note from {{ source('files', 'customers') }}\n");
        File.WriteAllText(Path.Combine(dir, "pipelines", "configs", "stg_customers.yml"), """
            pipeline: stg_customers
            columns:
              - name: contact
                redact: true
            checks:
              - not_null: [id]
            """);
        File.WriteAllText(Path.Combine(dir, "pipelines", "v_customers.sql"),
            "select id, note from {{ ref('stg_customers') }}\n");
        File.WriteAllText(Path.Combine(dir, "pipelines", "configs", "v_customers.yml"),
            "pipeline: v_customers\nmaterialization: view\n");
        File.WriteAllText(Path.Combine(dir, "pipelines", "eph_customers.sql"),
            "select id from {{ ref('stg_customers') }} where id % 2 = 0\n");
        File.WriteAllText(Path.Combine(dir, "pipelines", "configs", "eph_customers.yml"),
            "pipeline: eph_customers\nmaterialization: ephemeral\n");
        File.WriteAllText(Path.Combine(dir, "pipelines", "out_customers.sql"),
            "INSERT INTO {{ sink('lake', 'customers_out', strategy: 'replace', format: 'parquet', path: 'out/') }}\n" +
            "select id from {{ ref('eph_customers') }}\n");

        var csv = new StringBuilder("id,email,note\n");
        for (var i = 1; i <= rows; i++)
        {
            csv.Append(i).Append(",user").Append(i).Append("@example.com,note ").Append(i).Append('\n');
        }

        File.WriteAllText(Path.Combine(dir, "data", "customers.csv"), csv.ToString());
        return dir;
    }

    /// <summary>The newest run directory under <c>.pz/runs</c>.</summary>
    internal static string LatestRunDir(string dir) =>
        Directory.EnumerateDirectories(Path.Combine(dir, ".pz", "runs"))
            .OrderBy(d => Directory.GetCreationTimeUtc(d)).ThenBy(d => d, StringComparer.Ordinal).Last();
}
