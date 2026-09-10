using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

public sealed class ReadSchemaProjectionTests
{
    private static readonly Schema Declared = new Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("name", StringType.Default, true))
        .Field(new Field("amount", DoubleType.Default, true))
        .Field(new Field("flag", BooleanType.Default, true))
        .Build();

    [Fact]
    public void Non_prefix_reordered_hint_yields_the_hinted_fields_in_hint_order()
    {
        var projected = ReadSchemaProjection.Apply(
            Declared, new ReadHints(Columns: ["flag", "name"]), ConnectorCapabilities.ColumnPruning);

        Assert.Equal(["flag", "name"], projected.FieldsList.Select(f => f.Name));
        Assert.Equal(ArrowTypeId.Boolean, projected.FieldsList[0].DataType.TypeId);
        Assert.Equal(ArrowTypeId.String, projected.FieldsList[1].DataType.TypeId);
        Assert.True(projected.FieldsList[1].IsNullable);
    }

    [Fact]
    public void Hint_names_match_case_insensitively_and_keep_the_declared_spelling()
    {
        var projected = ReadSchemaProjection.Apply(
            Declared, new ReadHints(Columns: ["NAME", "Id"]), ConnectorCapabilities.ColumnPruning);

        Assert.Equal(["name", "id"], projected.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public void No_hint_or_empty_hint_returns_the_declared_schema()
    {
        Assert.Same(Declared, ReadSchemaProjection.Apply(Declared, ReadHints.None, ConnectorCapabilities.ColumnPruning));
        Assert.Same(Declared, ReadSchemaProjection.Apply(Declared, new ReadHints(Columns: []), ConnectorCapabilities.ColumnPruning));
    }

    [Fact]
    public void Connector_without_ColumnPruning_returns_the_declared_schema()
    {
        Assert.Same(Declared, ReadSchemaProjection.Apply(
            Declared, new ReadHints(Columns: ["flag", "name"]), ConnectorCapabilities.PredicatePushdown));
    }

    [Fact]
    public void Hint_naming_an_unknown_column_returns_the_declared_schema()
    {
        Assert.Same(Declared, ReadSchemaProjection.Apply(
            Declared, new ReadHints(Columns: ["flag", "missing"]), ConnectorCapabilities.ColumnPruning));
    }

    [Fact]
    public void Schema_metadata_is_carried_onto_the_projection()
    {
        var withMetadata = new Schema(Declared.FieldsList, new Dictionary<string, string> { ["k"] = "v" });

        var projected = ReadSchemaProjection.Apply(
            withMetadata, new ReadHints(Columns: ["amount"]), ConnectorCapabilities.ColumnPruning);

        Assert.Equal("v", projected.Metadata["k"]);
    }
}
