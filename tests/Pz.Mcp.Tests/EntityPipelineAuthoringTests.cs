using System.Text.Json;
using Pz.Cli.Commands;
using Pz.Core.Validation;
using Pz.Mcp;
using Pz.Mcp.Handlers;

namespace Pz.Mcp.Tests;

/// <summary>pz_add_entity / pz_set_entity_options / pz_remove_entity, pz_write_pipeline /
/// pz_remove_pipeline, and pz_init_project -- the same guard-&gt;apply-&gt;self-verify
/// mutation pipeline <see cref="ConnectionAuthoringTests"/> exercises for connections.
/// <see cref="RealServices"/> below wires <c>InitProject</c> identically to
/// <c>Pz.Cli.Commands.McpCommand.Execute</c>'s own real construction -- InternalsVisibleTo makes both
/// <c>InitCommand</c> and <c>PzErrorCode.McpInitDirNotEmpty</c> available here, same as
/// <c>ConnectorRegistryFactory</c> already is for the other RealServices() helpers in this
/// project.</summary>
public class EntityPipelineAuthoringTests
{
    private static CliServices RealServices() => new()
    {
        CreateRegistryAsync = (project, dir, ct) =>
            Pz.Cli.ConnectorRegistryFactory.CreateAsync(project, dir, noLockCheck: false, ct),
        CreateStateStores = (_, _) => throw new InvalidOperationException("not needed for entity/pipeline authoring"),
        InitProject = RealInitProject,
        RunAsync = (_, _) => throw new InvalidOperationException("not needed for entity/pipeline authoring"),
        RetryAsync = (_, _, _) => throw new InvalidOperationException("not needed for entity/pipeline authoring"),
    };

    /// <summary>Mirrors <c>McpCommand.InitProject</c> exactly: pre-check "directory is empty" (PZ0603)
    /// before calling <c>InitCommand.Execute</c> with the arguments `pz init &lt;name&gt;` would receive
    /// if run from <paramref name="dir"/>'s parent.</summary>
    private static IReadOnlyList<PzError> RealInitProject(string dir, string name, string templateId)
    {
        if (File.Exists(dir) || (Directory.Exists(dir) && Directory.EnumerateFileSystemEntries(dir).Any()))
        {
            return [new PzError(PzErrorCode.McpInitDirNotEmpty,
                $"target directory '{dir}' already exists and is not empty.", dir, null,
                "call pz_init_project against an empty project directory, or clear it first")];
        }

        var workingDir = Path.GetDirectoryName(dir) ?? dir;
        var exitCode = Pz.Cli.Commands.InitCommand.Execute(name, workingDir, templateId);
        return exitCode == 0
            ? []
            : [new PzError(PzErrorCode.McpInitDirNotEmpty,
                $"pz_init_project failed for '{dir}' (exit code {exitCode})", dir, null,
                "check the directory path and try again")];
    }

    // ----------------------------------------------------------------------------------------------
    // Entities.
    // ----------------------------------------------------------------------------------------------

    [Fact]
    public async Task Add_entity_creates_missing_entities_mapping_and_self_verifies()
    {
        // TempProject's "out" connection has no entities: mapping yet -- this exercises YamlSurgeon's
        // create-missing-mapping path at depth 2 (["out", "entities"]).
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.AddEntityAsync(
            p.Dir, "out", "orders2",
            read: new() { ["path"] = "data/orders.csv", ["format"] = "csv" }, write: null,
            RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.True(doc.RootElement.GetProperty("applied").GetBoolean());
        var text = File.ReadAllText(Path.Combine(p.Dir, "connections.yml"));
        Assert.Contains("entities:", text);
        Assert.Contains("orders2:", text);
    }

    [Fact]
    public async Task Add_entity_into_an_existing_entities_mapping_appends_a_sibling()
    {
        // "raw" already has entities: orders -- this exercises plain insertion into an existing mapping.
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.AddEntityAsync(
            p.Dir, "raw", "customers",
            read: new() { ["path"] = "data/orders.csv", ["format"] = "csv" }, write: null,
            RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("applied").GetBoolean());
        var text = File.ReadAllText(Path.Combine(p.Dir, "connections.yml"));
        Assert.Contains("orders:", text);
        Assert.Contains("customers:", text);
    }

    [Fact]
    public async Task Add_existing_entity_is_pz0602_pointing_at_set_options()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.AddEntityAsync(
            p.Dir, "raw", "orders", read: new() { ["path"] = "x", ["format"] = "csv" }, write: null,
            RealServices(), CancellationToken.None));
        Assert.Equal("PZ0602", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
        Assert.Contains("pz_set_entity_options", doc.RootElement.GetProperty("errors")[0].GetProperty("next_step").GetString());
    }

    [Fact]
    public async Task Add_entity_to_a_nonexistent_connection_is_pz0602_and_writes_nothing()
    {
        // InsertMappingEntry auto-vivifies missing path segments, including the connection itself --
        // without this guard, a typo'd connection name would land as a brand-new, connector-less
        // top-level block instead of being refused up front (validate before writing).
        using var p = new TempProject();
        var before = File.ReadAllText(Path.Combine(p.Dir, "connections.yml"));
        var doc = JsonDocument.Parse(await AuthoringTools.AddEntityAsync(
            p.Dir, "no_such_connection", "orders", read: new() { ["path"] = "x", ["format"] = "csv" },
            write: null, RealServices(), CancellationToken.None));
        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.False(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.Equal("PZ0602", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
        Assert.Contains("pz_add_connection", doc.RootElement.GetProperty("errors")[0].GetProperty("next_step").GetString());
        Assert.Equal(before, File.ReadAllText(Path.Combine(p.Dir, "connections.yml")));
    }

    [Fact]
    public async Task Set_entity_options_replaces_the_block_wholesale()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.SetEntityOptionsAsync(
            p.Dir, "raw", "orders", read: new() { ["path"] = "data/orders.csv", ["format"] = "csv" }, write: null,
            RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.True(doc.RootElement.GetProperty("applied").GetBoolean());
    }

    [Fact]
    public async Task Set_missing_entity_options_is_pz0602_pointing_at_add()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.SetEntityOptionsAsync(
            p.Dir, "raw", "nope", read: new() { ["path"] = "x", ["format"] = "csv" }, write: null,
            RealServices(), CancellationToken.None));
        Assert.Equal("PZ0602", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
        Assert.Contains("pz_add_entity", doc.RootElement.GetProperty("errors")[0].GetProperty("next_step").GetString());
    }

    [Fact]
    public async Task Remove_entity_round_trips_and_self_verifies_ok()
    {
        // source('raw', 'orders') at the call site declares no kwargs of its own -- with the
        // connections.yml entity block gone, it falls back to SourceReadOptions.Default (no compile-time
        // requirement that a source()'d entity be declared in connections.yml at all), and stg_orders.sql
        // was already undeclared/skipped at dry-compile before this removal (no columns: contract) --
        // so removing it is a clean, unremarkable round trip.
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.RemoveEntityAsync(
            p.Dir, "raw", "orders", RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.True(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.DoesNotContain("orders:", File.ReadAllText(Path.Combine(p.Dir, "connections.yml")));
    }

    [Fact]
    public async Task Removing_a_missing_entity_is_pz0602()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.RemoveEntityAsync(
            p.Dir, "raw", "nope", RealServices(), CancellationToken.None));
        Assert.False(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.Equal("PZ0602", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
    }

    // ----------------------------------------------------------------------------------------------
    // Pipelines.
    // ----------------------------------------------------------------------------------------------

    [Fact]
    public async Task Write_pipeline_happy_path_compiles_and_self_verifies()
    {
        // PZ0349 ("a source dataset is read by exactly one pipeline") means we can't add a second
        // pipeline reading raw/orders -- stg_orders.sql already reads it. Declare a fresh, still-unread
        // entity first, then have the new pipeline SELECT from it.
        using var p = new TempProject();
        await AuthoringTools.AddEntityAsync(
            p.Dir, "raw", "orders2", read: new() { ["path"] = "data/orders.csv", ["format"] = "csv" },
            write: null, RealServices(), CancellationToken.None);

        var doc = JsonDocument.Parse(await AuthoringTools.WritePipelineAsync(
            p.Dir, "stg_orders2", "select id, amount\nfrom {{ source('raw', 'orders2') }}\n", null,
            RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.True(doc.RootElement.GetProperty("applied").GetBoolean());
        var sqlPath = Path.Combine(p.Dir, "pipelines", "stg_orders2.sql");
        Assert.True(File.Exists(sqlPath));
        Assert.EndsWith("\n", File.ReadAllText(sqlPath));
    }

    [Fact]
    public async Task Write_pipeline_normalizes_crlf_to_lf_with_trailing_newline()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.WritePipelineAsync(
            p.Dir, "stg_orders3", "select 1 as id\r\nwhere true", null,
            RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        var text = File.ReadAllText(Path.Combine(p.Dir, "pipelines", "stg_orders3.sql"));
        Assert.DoesNotContain("\r", text);
        Assert.EndsWith("\n", text);
    }

    [Fact]
    public async Task Write_pipeline_writes_the_checks_sidecar_verbatim()
    {
        using var p = new TempProject();
        const string checksYaml = "pipeline: stg_orders4\nchecks:\n  - not_null: [id]\n";
        var doc = JsonDocument.Parse(await AuthoringTools.WritePipelineAsync(
            p.Dir, "stg_orders4", "select 1 as id\n", checksYaml,
            RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.True(doc.RootElement.GetProperty("applied").GetBoolean());
        var configPath = Path.Combine(p.Dir, "pipelines", "configs", "stg_orders4.yml");
        Assert.Equal(checksYaml, File.ReadAllText(configPath));
    }

    [Fact]
    public async Task Write_pipeline_with_broken_sql_stays_applied_with_a_dry_compile_error()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.WritePipelineAsync(
            p.Dir, "broken", "select from where;\n", null, RealServices(), CancellationToken.None));
        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.True(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.Equal("PZ0401", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
        // The file was still written -- applied:true means applied.
        Assert.True(File.Exists(Path.Combine(p.Dir, "pipelines", "broken.sql")));
    }

    [Theory]
    [InlineData("../escape")]
    [InlineData("a/b")]
    [InlineData("..")]
    public async Task Write_pipeline_refuses_an_unsafe_name(string name)
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.WritePipelineAsync(
            p.Dir, name, "select 1\n", null, RealServices(), CancellationToken.None));
        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.False(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.Equal("PZ0602", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
    }

    [Fact]
    public async Task Remove_pipeline_deletes_the_sql_and_sidecar_files()
    {
        using var p = new TempProject();
        const string checksYaml = "pipeline: removable\nchecks: []\n";
        await AuthoringTools.WritePipelineAsync(
            p.Dir, "removable", "select 1 as id\n", checksYaml, RealServices(), CancellationToken.None);
        var sqlPath = Path.Combine(p.Dir, "pipelines", "removable.sql");
        var configPath = Path.Combine(p.Dir, "pipelines", "configs", "removable.yml");
        Assert.True(File.Exists(sqlPath));
        Assert.True(File.Exists(configPath));

        var doc = JsonDocument.Parse(await AuthoringTools.RemovePipelineAsync(
            p.Dir, "removable", RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.True(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.False(File.Exists(sqlPath));
        Assert.False(File.Exists(configPath));
    }

    [Fact]
    public async Task Removing_a_refd_pipeline_stays_applied_and_reports_errors()
    {
        // orders_out's SQL does "from {{ ref('stg_orders') }}" -- removing stg_orders breaks compile.
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.RemovePipelineAsync(
            p.Dir, "stg_orders", RealServices(), CancellationToken.None));
        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.True(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.False(File.Exists(Path.Combine(p.Dir, "pipelines", "stg_orders.sql")));
    }

    [Fact]
    public async Task Remove_missing_pipeline_is_pz0602()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.RemovePipelineAsync(
            p.Dir, "nope", RealServices(), CancellationToken.None));
        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.False(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.Equal("PZ0602", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
    }

    // ----------------------------------------------------------------------------------------------
    // Init.
    // ----------------------------------------------------------------------------------------------

    [Fact]
    public void Init_into_an_empty_directory_scaffolds_a_runnable_project()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-mcp-init-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var doc = JsonDocument.Parse(AuthoringTools.InitProject(dir, "sample", RealServices()));
            Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
            Assert.True(doc.RootElement.GetProperty("applied").GetBoolean());
            Assert.True(doc.RootElement.GetProperty("result").GetProperty("created").GetBoolean());
            Assert.True(File.Exists(Path.Combine(dir, "project.yml")));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>The default is the minimal shape. An agent asked for a specific pipeline does not
    /// want the four-pipeline sample: it would have to delete six files first, and until it did,
    /// `pz run --all` would move demo data nobody asked for. The sample stays one flag away, for the
    /// case where a worked example IS the request.</summary>
    [Fact]
    public void Init_defaults_to_the_minimal_project()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-mcp-init-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var doc = JsonDocument.Parse(AuthoringTools.InitProject(dir, TemplateCatalog.DefaultId, RealServices()));
            var result = doc.RootElement.GetProperty("result");

            Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
            Assert.Equal("minimal", result.GetProperty("template").GetString());
            Assert.Equal(
                [".gitignore", "README.md", "connections.yml", "project.yml"],
                result.GetProperty("files").EnumerateArray().Select(f => f.GetString()!).ToArray());
            Assert.False(Directory.Exists(Path.Combine(dir, "data")));
            Assert.False(Directory.Exists(Path.Combine(dir, "pipelines")));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>The manifest is the point of listing files at all -- it saves the round trip that
    /// asking "what did I just get?" used to need.</summary>
    [Fact]
    public void Init_lists_every_file_the_sample_template_wrote()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-mcp-init-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var doc = JsonDocument.Parse(AuthoringTools.InitProject(dir, "sample", RealServices()));
            var result = doc.RootElement.GetProperty("result");
            var files = result.GetProperty("files").EnumerateArray().Select(f => f.GetString()!).ToArray();

            Assert.Equal("sample", result.GetProperty("template").GetString());
            Assert.Contains("pipelines/stg_orders.sql", files, StringComparer.Ordinal);
            Assert.Contains("data/orders.csv", files, StringComparer.Ordinal);
            Assert.Equal(Directory.GetFiles(dir, "*", SearchOption.AllDirectories).Length, files.Length);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Init_into_a_nonempty_directory_is_pz0603_and_writes_nothing()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-mcp-init-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "existing.txt"), "already here");
        try
        {
            var doc = JsonDocument.Parse(AuthoringTools.InitProject(dir, TemplateCatalog.DefaultId, RealServices()));
            Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
            Assert.False(doc.RootElement.GetProperty("applied").GetBoolean());
            Assert.Equal("PZ0603", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
            Assert.False(File.Exists(Path.Combine(dir, "project.yml")));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>Exercises the REAL <c>McpCommand.InitProject</c> wiring (<c>McpCommand.BuildServices()</c>,
    /// the same construction `pz mcp` itself uses) rather than the <see cref="RealInitProject"/> mirror
    /// above -- the unknown-template check under test lives in <c>McpCommand.InitProject</c> itself, so a
    /// test against the mirror would prove nothing about the real wiring. Before the directory-empty
    /// check on purpose: an unknown template is wrong about what the CALLER asked for, independent of
    /// whatever is (or isn't) on disk at <paramref name="dir"/>, and the target must stay untouched.</summary>
    [Fact]
    public void Init_rejects_an_unknown_template_before_touching_the_directory()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-mcp-init-" + Guid.NewGuid().ToString("N"));
        try
        {
            var doc = JsonDocument.Parse(AuthoringTools.InitProject(
                dir, "nope", Pz.Cli.Commands.McpCommand.BuildServices()));

            Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
            Assert.False(doc.RootElement.GetProperty("applied").GetBoolean());
            var error = doc.RootElement.GetProperty("errors")[0];
            Assert.Equal("PZ0131", error.GetProperty("code").GetString());
            Assert.Contains("nope", error.GetProperty("message").GetString(), StringComparison.Ordinal);
            // The hint must name what IS valid -- an error that only says "no" costs a round trip.
            Assert.Contains("minimal", error.GetProperty("next_step").GetString(), StringComparison.Ordinal);
            Assert.False(Directory.Exists(dir), "an unknown template must scaffold nothing");
        }
        finally
        {
            if (Directory.Exists(dir))
            {
                Directory.Delete(dir, recursive: true);
            }
        }
    }

    [Fact]
    public void Init_with_a_trailing_separator_scaffolds_directly_into_that_directory()
    {
        // Path.GetDirectoryName("/foo/bar/") is "/foo/bar", not "/foo" -- a trailing separator on
        // projectDir must be trimmed up front, or InitCommand.Execute resolves one level too deep
        // (targetDir "/foo/bar/bar" instead of "/foo/bar").
        var dir = Path.Combine(Path.GetTempPath(), "pz-mcp-init-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var withTrailingSeparator = dir + Path.DirectorySeparatorChar;
        try
        {
            var doc = JsonDocument.Parse(AuthoringTools.InitProject(withTrailingSeparator, TemplateCatalog.DefaultId, RealServices()));
            Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
            Assert.True(doc.RootElement.GetProperty("applied").GetBoolean());
            Assert.Equal(dir, doc.RootElement.GetProperty("result").GetProperty("dir").GetString());
            Assert.True(File.Exists(Path.Combine(dir, "project.yml")));
            Assert.False(Directory.Exists(Path.Combine(dir, Path.GetFileName(dir))));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
