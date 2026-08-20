using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connector.SqlServer.Tests;

public class MsSinkSqlGenTests
{
    private static readonly Schema S = new(
    [
        new Field("id", Int64Type.Default, nullable: false),
        new Field("name", StringType.Default, nullable: true),
        new Field("amount", new Decimal128Type(38, 9), nullable: true),
        new Field("flag", BooleanType.Default, nullable: true),
        new Field("d", Date32Type.Default, nullable: true),
        new Field("ts", new TimestampType(TimeUnit.Microsecond, "+00:00"), nullable: true),
        new Field("n", Int32Type.Default, nullable: true),
        new Field("x", DoubleType.Default, nullable: true),
    ], null);

    private static IReadOnlyDictionary<string, string> Types(Schema schema) =>
        schema.FieldsList.ToDictionary(f => f.Name, f => f.DataType.TypeId == ArrowTypeId.String
            ? "nvarchar(4000)" : MsDdl.DdlType(f));

    [Fact]
    public void CreateTable_for_append_is_a_heap_with_canonical_types()
    {
        var sql = MsDdl.BuildCreateTableSql("dbo", "t", S, "append", [], Types(S));
        Assert.Equal(
            "create table [dbo].[t] ([id] bigint, [name] nvarchar(4000), [amount] decimal(38,9), " +
            "[flag] bit, [d] date, [ts] datetime2(6), [n] int, [x] float)",
            sql);
    }

    [Fact]
    public void CreateTable_for_merge_adds_not_null_keys_and_clustered_pk()
    {
        var sql = MsDdl.BuildCreateTableSql("dbo", "t", S, "merge", ["id"], Types(S));
        Assert.Contains("[id] bigint not null", sql, StringComparison.Ordinal);
        Assert.Contains("constraint [pk_t] primary key clustered ([id])", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Create_table_renders_the_effective_types()
    {
        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, nullable: true),
            new Field("note", StringType.Default, nullable: true),
        ], null);
        var types = new Dictionary<string, string> { ["id"] = "bigint", ["note"] = "nvarchar(32)" };
        var sql = MsDdl.BuildCreateTableSql("dbo", "t", schema, "append", [], types);
        Assert.Contains("[note] nvarchar(32)", sql);
        Assert.DoesNotContain("nvarchar(max)", sql);
    }

    [Fact]
    public void Column_list_renders_the_effective_types()
    {
        var schema = new Schema([new Field("note", StringType.Default, nullable: true)], null);
        var sql = MsDdl.BuildColumnListSql(schema, new Dictionary<string, string> { ["note"] = "nvarchar(64)" });
        Assert.Equal("[note] nvarchar(64)", sql);
    }

    // The USING source is the staging table key-deduped last-writer-wins over the __pz_seq arrival
    // ordinal (MsDdl.StagingSequenceColumn) -- a raw MERGE over duplicated staged keys would fail
    // loudly instead of honoring the TestKit merge contract (MergeRows.Absorb).
    [Fact]
    public void Merge_updates_non_keys_and_inserts_all_columns()
    {
        var sql = MsDdl.BuildMergeSql("[dbo].[t]", "[#tmp]", S, ["id"]);
        Assert.Equal(
            "merge [dbo].[t] with (holdlock) as t using " +
            "(select [id], [name], [amount], [flag], [d], [ts], [n], [x] from " +
            "(select [id], [name], [amount], [flag], [d], [ts], [n], [x], row_number() over " +
            "(partition by [id] order by [__pz_seq] desc) as [__pz_rn] from [#tmp]) as d " +
            "where [__pz_rn] = 1) as s on t.[id] = s.[id] " +
            "when matched then update set t.[name] = s.[name], t.[amount] = s.[amount], " +
            "t.[flag] = s.[flag], t.[d] = s.[d], t.[ts] = s.[ts], t.[n] = s.[n], t.[x] = s.[x] " +
            "when not matched then insert ([id], [name], [amount], [flag], [d], [ts], [n], [x]) " +
            "values (s.[id], s.[name], s.[amount], s.[flag], s.[d], s.[ts], s.[n], s.[x]);",
            sql);
    }

    [Fact]
    public void Merge_with_only_key_columns_omits_the_update_clause()
    {
        var keyOnly = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);
        var sql = MsDdl.BuildMergeSql("[dbo].[t]", "[#tmp]", keyOnly, ["id"]);
        Assert.DoesNotContain("when matched", sql, StringComparison.Ordinal);
        Assert.Contains("when not matched then insert ([id]) values (s.[id]);", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Merge_dedups_staging_rows_by_key_last_writer_wins()
    {
        var sql = MsDdl.BuildMergeSql("[dbo].[t]", "[#tmp]", S, ["id", "n"]);
        Assert.Contains(
            "row_number() over (partition by [id], [n] order by [__pz_seq] desc) as [__pz_rn]",
            sql, StringComparison.Ordinal);
        Assert.Contains("where [__pz_rn] = 1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Staging_types_mirror_the_existing_targets_string_columns()
    {
        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, nullable: true),
            new Field("note", StringType.Default, nullable: true),
        ], null);
        var resolved = new MsResolvedTypes(
            new Dictionary<string, string> { ["id"] = "bigint", ["note"] = "nvarchar(4000)" },
            new HashSet<string>());
        var existing = new Dictionary<string, string> { ["id"] = "bigint", ["note"] = "nvarchar(50)" };
        var types = SqlServerSink.StagingTypes(schema, resolved, existing);
        Assert.Equal("nvarchar(50)", types["note"]); // target governs what the bulk load pays
        Assert.Equal("bigint", types["id"]);
    }

    [Fact]
    public void Staging_types_use_resolved_types_when_the_target_was_just_created()
    {
        var schema = new Schema([new Field("note", StringType.Default, nullable: true)], null);
        var resolved = new MsResolvedTypes(
            new Dictionary<string, string> { ["note"] = "nvarchar(32)" }, new HashSet<string>());
        Assert.Equal("nvarchar(32)", SqlServerSink.StagingTypes(schema, resolved, existingColumns: null)["note"]);
    }

    [Fact]
    public void Declared_and_existing_agree_in_staging_when_fail_on_change_passed()
    {
        var schema = new Schema([new Field("note", StringType.Default, nullable: true)], null);
        var resolved = new MsResolvedTypes(
            new Dictionary<string, string> { ["note"] = "nvarchar(20)" },
            new HashSet<string> { "note" });
        var existing = new Dictionary<string, string> { ["note"] = "nvarchar(20)" }; // fail_on_change proved equality
        Assert.Equal("nvarchar(20)", SqlServerSink.StagingTypes(schema, resolved, existing)["note"]);
    }

    [Theory]
    [InlineData(2628)]
    [InlineData(8152)]
    public void Truncation_errors_carry_the_remediation_hint(int number)
    {
        var message = SqlServerSink.BuildBulkWriteMessage(number, "String or binary data would be truncated", "dbo.t");
        Assert.Contains("alter table", message);
        Assert.Contains("columns:", message);
    }

    [Fact]
    public void Other_sql_errors_keep_the_plain_message()
    {
        var message = SqlServerSink.BuildBulkWriteMessage(547, "constraint violation", "dbo.t");
        Assert.DoesNotContain("columns:", message);
        Assert.Contains("constraint violation", message);
    }
}
