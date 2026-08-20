using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Tests.Loading;

/// <summary>The unified dataset `sync:` block (`sync: { mode: incremental | cdc | auto }`) is the only
/// read-mode surface: there is no `incremental:` block and no opaque `sync:` marker. Carries its own
/// TempProject helper, since that helper is private to ProjectLoaderTests.</summary>
public class SyncModeLoaderTests
{
    private static readonly IReadOnlyDictionary<string, string> Env = new Dictionary<string, string>();

    private static string TempProject(string sourceYaml)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "project.yml"), "name: t\nversion: \"1\"\n");
        File.WriteAllText(Path.Combine(dir, "connections.yml"), sourceYaml);
        return dir;
    }

    [Fact]
    public void Sync_mode_incremental_with_cursor_parses()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    sync:
                      mode: incremental
                      cursor: id
                      max_window: "100"
                      initial: "0"
            """;
        var project = ProjectLoader.Load(TempProject(yaml), Env);
        var dataset = Assert.Single(Assert.Single(project.Connections).Datasets);

        Assert.Equal(SyncMode.Incremental, dataset.SyncMode!.Mode);
        Assert.Equal("id", dataset.SyncMode.Incremental!.Cursor);
        Assert.Equal("100", dataset.SyncMode.Incremental.MaxWindow);
        Assert.Equal("0", dataset.SyncMode.Incremental.Initial);
        Assert.Null(dataset.SyncMode.Incremental.Until);
    }

    [Fact]
    public void No_sync_block_means_null_sync_mode()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
            """;
        var project = ProjectLoader.Load(TempProject(yaml), Env);
        var dataset = Assert.Single(Assert.Single(project.Connections).Datasets);

        Assert.Null(dataset.SyncMode);
    }

    [Fact]
    public void Sync_mode_auto_parses_and_rejects_extra_keys()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    sync:
                      mode: auto
            """;
        var project = ProjectLoader.Load(TempProject(yaml), Env);
        var dataset = Assert.Single(Assert.Single(project.Connections).Datasets);
        Assert.Equal(new SyncModeDef(SyncMode.Auto, null), dataset.SyncMode);

        var yamlBad = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    sync:
                      mode: auto
                      cursor: id
            """;
        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(TempProject(yamlBad), Env));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SyncModeInvalid);
        Assert.Contains("mode 'auto' accepts no other keys", error.Message);
    }

    [Fact]
    public void Sync_mode_incremental_requires_cursor()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    sync:
                      mode: incremental
            """;
        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(TempProject(yaml), Env));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SyncModeInvalid);
        Assert.Contains("'cursor'", error.Message);
    }

    [Fact]
    public void Sync_mode_cdc_parses()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    sync:
                      mode: cdc
            """;
        var project = ProjectLoader.Load(TempProject(yaml), Env);
        var dataset = Assert.Single(Assert.Single(project.Connections).Datasets);

        Assert.Equal(SyncMode.Cdc, dataset.SyncMode!.Mode);
        Assert.Null(dataset.SyncMode.Incremental);
        Assert.Null(dataset.SyncMode.Slot);
    }

    [Fact]
    public void Sync_mode_cdc_with_slot_parses()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    sync:
                      mode: cdc
                      slot: my_slot
            """;
        var project = ProjectLoader.Load(TempProject(yaml), Env);
        var dataset = Assert.Single(Assert.Single(project.Connections).Datasets);

        Assert.Equal("my_slot", dataset.SyncMode!.Slot);
    }

    [Fact]
    public void Sync_mode_cdc_unknown_key_is_refused()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    sync:
                      mode: cdc
                      cursor: id
            """;
        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(TempProject(yaml), Env));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SyncModeInvalid);
        Assert.Contains("unknown 'sync' key 'cursor'", error.Message);
    }

    [Fact]
    public void Sync_mode_cdc_slot_must_be_scalar()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    sync:
                      mode: cdc
                      slot: [a]
            """;
        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(TempProject(yaml), Env));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SyncModeInvalid);
        Assert.Contains("'sync.slot' must be a scalar value", error.Message);
    }

    [Fact]
    public void Sync_mode_unknown_lists_accepted_modes()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    sync:
                      mode: bogus
            """;
        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(TempProject(yaml), Env));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SyncModeInvalid);
        Assert.Contains("incremental, cdc, auto", error.Message);
    }

    [Fact]
    public void Sync_scalar_is_shape_error_with_hint()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    sync: "scalar"
            """;
        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(TempProject(yaml), Env));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SyncModeInvalid);
        Assert.Contains("sync:\n  mode: incremental\n  cursor: <column>", error.Hint);
    }

    [Fact]
    public void Old_incremental_key_is_refused_with_exact_rewrite()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    incremental:
                      cursor: id
            """;
        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(TempProject(yaml), Env));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.RetiredReadSurface);
        Assert.Contains("sync:\n          mode: incremental\n          cursor: id", error.Hint);
    }

    [Fact]
    public void Sync_mode_incremental_unknown_sub_key_is_refused()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    sync:
                      mode: incremental
                      cursor: id
                      slot: foo
            """;
        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(TempProject(yaml), Env));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SyncModeInvalid);
        Assert.Contains("unknown 'sync' key 'slot'", error.Message);
    }

    [Fact]
    public void Errors_aggregate_across_datasets()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    incremental:
                      cursor: id
                customers:
                  read:
                    sync:
                      mode: bogus
            """;
        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(TempProject(yaml), Env));
        Assert.Single(ex.Errors, e => e.Code == PzErrorCode.RetiredReadSurface);
        Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SyncModeInvalid);
    }
}
