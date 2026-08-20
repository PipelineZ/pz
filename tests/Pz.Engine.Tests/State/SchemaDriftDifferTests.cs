using Pz.Connectors.Abstractions;
using Pz.Engine.State;

namespace Pz.Engine.Tests.State;

public sealed class SchemaDriftDifferTests
{
    [Fact]
    public void No_change_yields_empty_diff()
    {
        var baseline = new List<SchemaColumn> { new("id", "BIGINT"), new("name", "VARCHAR") };
        var observed = new List<SchemaColumn> { new("id", "BIGINT"), new("name", "VARCHAR") };

        var changes = SchemaDriftDiffer.Diff(baseline, observed);

        Assert.Empty(changes);
    }

    [Fact]
    public void One_of_each_kind_is_reported()
    {
        var baseline = new List<SchemaColumn>
        {
            new("id", "BIGINT"),
            new("removed_col", "VARCHAR"),
            new("retyped_col", "VARCHAR"),
        };
        var observed = new List<SchemaColumn>
        {
            new("id", "BIGINT"),
            new("retyped_col", "BIGINT"),
            new("added_col", "VARCHAR"),
        };

        var changes = SchemaDriftDiffer.Diff(baseline, observed);

        Assert.Equal(3, changes.Count);
        Assert.Contains(changes, c => c.Kind == "added" && c.Column == "added_col" && c.From == null && c.To == "VARCHAR");
        Assert.Contains(changes, c => c.Kind == "removed" && c.Column == "removed_col" && c.From == "VARCHAR" && c.To == null);
        Assert.Contains(changes, c => c.Kind == "retyped" && c.Column == "retyped_col" && c.From == "VARCHAR" && c.To == "BIGINT");
    }

    [Fact]
    public void Type_change_is_exact_string_comparison()
    {
        var baseline = new List<SchemaColumn> { new("amount", "VARCHAR") };
        var observed = new List<SchemaColumn> { new("amount", "BIGINT") };

        var changes = SchemaDriftDiffer.Diff(baseline, observed);

        var change = Assert.Single(changes);
        Assert.Equal("retyped", change.Kind);
        Assert.Equal("amount", change.Column);
        Assert.Equal("VARCHAR", change.From);
        Assert.Equal("BIGINT", change.To);
    }

    [Fact]
    public void Diff_is_order_deterministic_across_calls()
    {
        var baseline = new List<SchemaColumn>
        {
            new("a", "BIGINT"),
            new("b", "VARCHAR"),
            new("c", "VARCHAR"),
        };
        var observed = new List<SchemaColumn>
        {
            new("a", "BIGINT"),
            new("c", "BIGINT"),
            new("d", "VARCHAR"),
        };

        var first = SchemaDriftDiffer.Diff(baseline, observed);
        var second = SchemaDriftDiffer.Diff(baseline, observed);

        Assert.Equal(first, second);
    }

    [Fact]
    public void Extra_observed_column_is_added_never_tolerated_silently()
    {
        var baseline = new List<SchemaColumn> { new("id", "BIGINT") };
        var observed = new List<SchemaColumn> { new("id", "BIGINT"), new("extra", "VARCHAR") };

        var changes = SchemaDriftDiffer.Diff(baseline, observed);

        var change = Assert.Single(changes);
        Assert.Equal("added", change.Kind);
        Assert.Equal("extra", change.Column);
    }

    [Fact]
    public void HashHints_is_stable_and_differs_from_hint_with_one_column()
    {
        var none1 = SchemaDriftDiffer.HashHints(ReadHints.None);
        var none2 = SchemaDriftDiffer.HashHints(ReadHints.None);
        var withColumn = SchemaDriftDiffer.HashHints(new ReadHints(Columns: ["id"]));

        Assert.Equal(none1, none2);
        Assert.NotEqual(none1, withColumn);
    }

    [Fact]
    public void HashHints_predicate_only_difference_changes_hash()
    {
        var withoutPredicate = SchemaDriftDiffer.HashHints(new ReadHints(Columns: ["id"]));
        var withPredicate = SchemaDriftDiffer.HashHints(new ReadHints(Columns: ["id"], PredicateSql: "id > 1"));

        Assert.NotEqual(withoutPredicate, withPredicate);
    }
}
