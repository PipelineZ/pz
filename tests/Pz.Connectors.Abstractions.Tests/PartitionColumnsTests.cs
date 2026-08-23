using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Abstractions.Tests;

/// <summary>One place parses <c>partition_by:</c>, so no connector invents its own spelling for it and
/// none has to guess what a YAML sequence deserialized into.</summary>
public sealed class PartitionColumnsTests
{
    private static Dictionary<string, object?> With(object? value) =>
        new() { ["partition_by"] = value };

    [Fact]
    public void Absent_or_null_names_no_columns()
    {
        Assert.Empty(PartitionColumns.Read(new Dictionary<string, object?>()));
        Assert.Empty(PartitionColumns.Read(With(null)));
        Assert.True(PartitionColumns.TryRead(new Dictionary<string, object?>(), out _, out var problem));
        Assert.Null(problem);
    }

    [Fact]
    public void A_scalar_is_one_column()
    {
        Assert.Equal(["ts"], PartitionColumns.Read(With("ts")));
        Assert.Equal(["ts"], PartitionColumns.Read(With("  ts  ")));
    }

    /// <summary>The trap this type closes: <c>ToString()</c> on a deserialized list yields its CLR type
    /// name, which is a non-empty string — so a presence check passes and everything downstream reads a
    /// column named <c>System.Collections.Generic.List`1[System.Object]</c>.</summary>
    [Fact]
    public void A_list_is_its_columns_never_a_clr_type_name()
    {
        var columns = PartitionColumns.Read(With(new List<object?> { "year", "month" }));

        Assert.Equal(["year", "month"], columns);
        Assert.DoesNotContain(columns, c => c.Contains("System.Collections", StringComparison.Ordinal));
    }

    [Fact]
    public void A_single_element_list_is_the_same_as_the_scalar() =>
        Assert.Equal(
            PartitionColumns.Read(With("ts")),
            PartitionColumns.Read(With(new List<object?> { "ts" })));

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void A_blank_scalar_is_refused(string value)
    {
        Assert.False(PartitionColumns.TryRead(With(value), out _, out var problem));
        Assert.NotNull(problem);
    }

    [Fact]
    public void An_empty_list_is_refused()
    {
        Assert.False(PartitionColumns.TryRead(With(new List<object?>()), out _, out var problem));
        Assert.Contains("empty list", problem!);
    }

    [Fact]
    public void A_list_entry_that_is_not_a_column_name_is_refused()
    {
        Assert.False(PartitionColumns.TryRead(With(new List<object?> { "ts", 7 }), out _, out var problem));
        Assert.Contains("7", problem!);
    }

    /// <summary>A repeated column is a typo, not a partition scheme — and it would produce a different
    /// layout on every destination that dedupes and every one that does not.</summary>
    [Fact]
    public void A_repeated_column_is_refused()
    {
        Assert.False(PartitionColumns.TryRead(With(new List<object?> { "ts", "ts" }), out _, out var problem));
        Assert.Contains("twice", problem!);
    }

    [Fact]
    public void A_value_that_is_neither_is_refused()
    {
        Assert.False(PartitionColumns.TryRead(With(42), out _, out var problem));
        Assert.Contains("42", problem!);
    }

    /// <summary>Read never throws: a malformed declaration is DagCompiler's to refuse with a coded error
    /// before a connector ever sees the spec, so a connector reading it gets "nothing declared" rather
    /// than an exception from a code path it cannot diagnose.</summary>
    [Fact]
    public void Read_never_throws_on_a_malformed_value() =>
        Assert.Empty(PartitionColumns.Read(With(42)));
}
