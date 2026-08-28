using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

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
}
