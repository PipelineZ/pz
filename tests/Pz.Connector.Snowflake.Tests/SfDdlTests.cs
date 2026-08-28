using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Snowflake.Data.Client;

namespace Pz.Connector.Snowflake.Tests;

public class SfDdlTests
{
    private static Schema TwoColSchema() => new([
        new Field("id", Int64Type.Default, nullable: true),
        new Field("name", StringType.Default, nullable: true)], null);

    [Fact]
    public void Quote_doubles_embedded_quotes() =>
        Assert.Equal("\"we\"\"ird\"", SfDdl.Quote("we\"ird"));

    [Fact]
    public void SplitEntity_splits_on_first_dot() =>
        Assert.Equal(("RAW", "ORDERS.V2"), SfDdl.SplitEntity("RAW.ORDERS.V2"));

    [Fact]
    public void SplitEntity_defaults_schema_to_public() =>
        Assert.Equal(("PUBLIC", "ORDERS"), SfDdl.SplitEntity("ORDERS"));

    [Fact]
    public void SplitEntity_rejects_empty_parts()
    {
        var ex = Assert.Throws<PzConnectorException>(() => SfDdl.SplitEntity(".ORDERS"));
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void CreateTable_maps_the_v0_matrix() =>
        Assert.Equal(
            "create table if not exists \"S\".\"T\" (\"id\" BIGINT, \"name\" VARCHAR)",
            SfDdl.BuildCreateTableSql("S", "T", TwoColSchema()));

    [Fact]
    public void InsertOverwrite_lists_columns_explicitly() =>
        Assert.Equal(
            "insert overwrite into \"S\".\"T\" (\"id\", \"name\") select \"id\", \"name\" from pz_load_x",
            SfDdl.BuildInsertOverwriteSql("S", "T", "pz_load_x", TwoColSchema()));

    [Fact]
    public void Merge_dedups_last_writer_wins_and_excludes_keys_from_update()
    {
        var sql = SfDdl.BuildMergeSql("S", "T", "pz_load_x", TwoColSchema(), ["id"]);
        Assert.Contains("qualify row_number() over (partition by \"id\" order by \"_pz_seq\" desc) = 1", sql);
        Assert.Contains("when matched then update set \"name\" = s.\"name\"", sql);
        Assert.DoesNotContain("update set \"id\"", sql);
        Assert.Contains("when not matched then insert (\"id\", \"name\") values (s.\"id\", s.\"name\")", sql);
        Assert.Contains("on t.\"id\" = s.\"id\"", sql);
    }

    [Fact]
    public void CreateStage_uses_the_schema_qualified_identifier_verbatim() =>
        Assert.Equal("create temporary stage \"S\".\"pz_stage_ab12cd34\"",
            SfDdl.BuildCreateStageSql("\"S\".\"pz_stage_ab12cd34\""));

    [Fact]
    public void Put_single_quotes_the_uri_and_forward_slashes_a_windows_path() =>
        Assert.Equal(
            "put 'file://C:/Users/pz svc/AppData/Local/Temp/pz-snowflake/x/part_00000.csv.gz' " +
            "@\"S\".\"pz_stage_ab12cd34\" auto_compress = false source_compression = gzip",
            SfDdl.BuildPutSql(
                "C:\\Users\\pz svc\\AppData\\Local\\Temp\\pz-snowflake\\x\\part_00000.csv.gz",
                "\"S\".\"pz_stage_ab12cd34\""));

    [Fact]
    public void CreateStagingTable_omits_sequence_column_outside_merge() =>
        Assert.Equal(
            "create temporary table \"S\".\"pz_load_ab12cd34\" (\"id\" BIGINT, \"name\" VARCHAR)",
            SfDdl.BuildCreateStagingTableSql("\"S\".\"pz_load_ab12cd34\"", TwoColSchema(), includeSequenceColumn: false));

    [Fact]
    public void CreateStagingTable_appends_a_plain_bigint_sequence_column_for_merge() =>
        Assert.Equal(
            "create temporary table \"S\".\"pz_load_ab12cd34\" (\"id\" BIGINT, \"name\" VARCHAR, \"_pz_seq\" bigint)",
            SfDdl.BuildCreateStagingTableSql("\"S\".\"pz_load_ab12cd34\"", TwoColSchema(), includeSequenceColumn: true));

    [Fact]
    public void CopyIntoStaging_uses_the_transformation_form_with_a_target_column_list()
    {
        // A target column list is invalid on COPY's standard `from @stage` form -- only the
        // transformation form (`from (select $1, ... from @stage)`) accepts one.
        var sql = SfDdl.BuildCopyIntoStagingSql(
            "\"S\".\"pz_load_ab12cd34\"", "\"S\".\"pz_stage_ab12cd34\"", TwoColSchema(), includeSequenceColumn: false);
        Assert.Equal(
            "copy into \"S\".\"pz_load_ab12cd34\" (\"id\", \"name\") from " +
            "(select $1, $2 from @\"S\".\"pz_stage_ab12cd34\") " +
            "file_format = (type = csv field_optionally_enclosed_by = '\"' null_if = ('\\\\N') " +
            "escape_unenclosed_field = none) on_error = abort_statement",
            sql);
    }

    [Fact]
    public void CopyIntoStaging_lists_and_positions_the_trailing_sequence_column_for_merge()
    {
        var sql = SfDdl.BuildCopyIntoStagingSql(
            "\"S\".\"pz_load_ab12cd34\"", "\"S\".\"pz_stage_ab12cd34\"", TwoColSchema(), includeSequenceColumn: true);
        Assert.Contains("(\"id\", \"name\", \"_pz_seq\")", sql);
        Assert.Contains("(select $1, $2, $3 from @\"S\".\"pz_stage_ab12cd34\")", sql);
    }

    [Fact]
    public async Task EnsureTarget_rejects_evolve_without_touching_the_connection()
    {
        // The evolve rejection is the very first thing EnsureTargetAsync does, before any query --
        // an unopened connection is enough to prove no network round-trip happens on this path.
        await using var connection = new SnowflakeDbConnection { ConnectionString = "account=a" };
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await SfDdl.EnsureTargetAsync(connection, "evolve", "S", "T", TwoColSchema(), "sf.S.T", CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("evolve", ex.Message);
        Assert.Contains("fail_on_change", ex.Message);
    }

    [Fact]
    public async Task EnsureTarget_rejects_evolve_case_insensitively()
    {
        await using var connection = new SnowflakeDbConnection { ConnectionString = "account=a" };
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await SfDdl.EnsureTargetAsync(connection, "EVOLVE", "S", "T", TwoColSchema(), "sf.S.T", CancellationToken.None));
        Assert.False(ex.IsTransient);
    }
}
