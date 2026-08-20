using System.Data;
using Apache.Arrow.Types;
using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>Offline unit tests for the `procedure:` dataset mode. All of
/// <see cref="ProcedureDataset.BuildCommand"/>'s validation and parameter-binding logic runs before any
/// I/O, so an unopened <see cref="SqlConnection"/> is enough to exercise it -- no docker required.</summary>
public class ProcedureDatasetTests
{
    private static DatasetSpec Spec(params (string Key, object? Value)[] options) =>
        new("ms", "ds", options.ToDictionary(o => o.Key, o => o.Value));

    private static Dictionary<string, object?> Params(params (string Key, object? Value)[] entries) =>
        entries.ToDictionary(e => e.Key, e => e.Value);

    [Fact]
    public void BuildCommand_uses_stored_procedure_command_type_and_verbatim_name()
    {
        using var command = ProcedureDataset.BuildCommand(new SqlConnection(), Spec(("procedure", "dbo.orders_page")));
        Assert.Equal(CommandType.StoredProcedure, command.CommandType);
        Assert.Equal("dbo.orders_page", command.CommandText);
        Assert.Empty(command.Parameters);
    }

    [Fact]
    public void Literal_parameters_bind_typed_values_under_at_prefixed_names()
    {
        var spec = Spec(("procedure", "dbo.p"),
            ("parameters", Params(("min_id", 5), ("name", "hi"), ("active", true))));
        using var command = ProcedureDataset.BuildCommand(new SqlConnection(), spec);
        Assert.Equal(5, command.Parameters["@min_id"].Value);
        Assert.Equal("hi", command.Parameters["@name"].Value);
        Assert.Equal(true, command.Parameters["@active"].Value);
    }

    [Fact]
    public void Explicit_null_literal_parameter_binds_DBNull()
    {
        var spec = Spec(("procedure", "dbo.p"), ("parameters", Params(("x", null))));
        using var command = ProcedureDataset.BuildCommand(new SqlConnection(), spec);
        Assert.Equal(DBNull.Value, command.Parameters["@x"].Value);
    }

    [Fact]
    public void Watermark_sentinel_binds_watermark_value_when_present()
    {
        var spec = Spec(("procedure", "dbo.p"), ("parameters", Params(("min_id", "$watermark"))))
            with
        { WatermarkCursor = "id", WatermarkValue = "99" };
        using var command = ProcedureDataset.BuildCommand(new SqlConnection(), spec);
        Assert.Equal("99", command.Parameters["@min_id"].Value);
    }

    [Fact]
    public void Watermark_sentinel_binds_DBNull_when_watermark_is_null()
    {
        // Planning-time probes carry no watermark (WatermarkValue null) -- procs must treat this as
        // unbounded, and DBNull is the binding that lets `@min_id is null or ...`-style proc bodies do that.
        var spec = Spec(("procedure", "dbo.p"), ("parameters", Params(("min_id", "$watermark"))));
        using var command = ProcedureDataset.BuildCommand(new SqlConnection(), spec);
        Assert.Equal(DBNull.Value, command.Parameters["@min_id"].Value);
    }

    [Fact]
    public void Watermark_upper_sentinel_binds_upper_bound_when_present()
    {
        var spec = Spec(("procedure", "dbo.p"), ("parameters", Params(("max_id", "$watermark_upper"))))
            with
        { WatermarkCursor = "id", WatermarkValue = "99", WatermarkUpperBound = "119" };
        using var command = ProcedureDataset.BuildCommand(new SqlConnection(), spec);
        Assert.Equal("119", command.Parameters["@max_id"].Value);
    }

    [Fact]
    public void Watermark_upper_sentinel_binds_DBNull_when_upper_bound_is_null()
    {
        var spec = Spec(("procedure", "dbo.p"), ("parameters", Params(("max_id", "$watermark_upper"))))
            with
        { WatermarkCursor = "id", WatermarkValue = "99" };
        using var command = ProcedureDataset.BuildCommand(new SqlConnection(), spec);
        Assert.Equal(DBNull.Value, command.Parameters["@max_id"].Value);
    }

    [Fact]
    public void Malformed_parameters_shape_throws_non_transient_naming_dataset()
    {
        var spec = Spec(("procedure", "dbo.p"), ("parameters", "oops"));
        var ex = Assert.Throws<PzConnectorException>(
            () => ProcedureDataset.BuildCommand(new SqlConnection(), spec));
        Assert.False(ex.IsTransient);
        Assert.Contains("parameters", ex.Message, StringComparison.Ordinal);
        Assert.Contains("dataset 'ds'", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("x; drop table y")]
    [InlineData("dbo.p; select 1")]
    [InlineData("dbo.'p'")]
    public void Invalid_procedure_name_throws_non_transient_naming_dataset(string name)
    {
        var ex = Assert.Throws<PzConnectorException>(
            () => ProcedureDataset.BuildCommand(new SqlConnection(), Spec(("procedure", name))));
        Assert.False(ex.IsTransient);
        Assert.Contains("invalid procedure name", ex.Message, StringComparison.Ordinal);
        Assert.Contains("dataset 'ds'", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("dbo.orders_page")]
    [InlineData("[dbo].[orders_page]")]
    [InlineData("orders_page")]
    public void Valid_procedure_names_are_accepted_verbatim(string name)
    {
        using var command = ProcedureDataset.BuildCommand(new SqlConnection(), Spec(("procedure", name)));
        Assert.Equal(name, command.CommandText);
    }

    [Fact]
    public void Contract_schema_maps_all_eight_declared_types_to_the_exact_arrow_types()
    {
        var spec = Spec(("procedure", "dbo.p"), ("columns", Params(
            ("c_int", "int"), ("c_bigint", "bigint"), ("c_double", "double"), ("c_decimal", "decimal"),
            ("c_varchar", "varchar"), ("c_boolean", "boolean"), ("c_date", "date"), ("c_timestamp", "timestamp"))));

        var schema = ProcedureDataset.BuildContractSchema(spec);
        Assert.NotNull(schema);
        Assert.Equal(8, schema!.FieldsList.Count);
        Assert.True(schema.FieldsList.All(f => f.IsNullable));

        Assert.Same(Int32Type.Default, schema.GetFieldByName("c_int").DataType);
        Assert.Same(Int64Type.Default, schema.GetFieldByName("c_bigint").DataType);
        Assert.Same(DoubleType.Default, schema.GetFieldByName("c_double").DataType);
        Assert.Same(StringType.Default, schema.GetFieldByName("c_varchar").DataType);
        Assert.Same(BooleanType.Default, schema.GetFieldByName("c_boolean").DataType);
        Assert.Same(Date32Type.Default, schema.GetFieldByName("c_date").DataType);

        var decimalType = Assert.IsType<Decimal128Type>(schema.GetFieldByName("c_decimal").DataType);
        Assert.Equal(38, decimalType.Precision);
        Assert.Equal(9, decimalType.Scale);

        var timestampType = Assert.IsType<TimestampType>(schema.GetFieldByName("c_timestamp").DataType);
        Assert.Equal(TimeUnit.Microsecond, timestampType.Unit);
        Assert.Equal("+00:00", timestampType.Timezone);
    }

    [Fact]
    public void Contract_schema_builder_returns_null_when_no_columns_contract_is_declared()
    {
        Assert.Null(ProcedureDataset.BuildContractSchema(Spec(("procedure", "dbo.p"))));
    }

    [Fact]
    public void Contract_schema_builder_rejects_unknown_type_name_naming_column_and_valid_set()
    {
        var spec = Spec(("procedure", "dbo.p"), ("columns", Params(("id", "uuid"))));
        var ex = Assert.Throws<PzConnectorException>(() => ProcedureDataset.BuildContractSchema(spec));
        Assert.False(ex.IsTransient);
        Assert.Contains("column 'id'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("'uuid'", ex.Message, StringComparison.Ordinal);
        foreach (var validType in new[] { "int", "bigint", "double", "decimal", "varchar", "boolean", "date", "timestamp" })
        {
            Assert.Contains(validType, ex.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task PlanReadAsync_rejects_partition_column_before_opening_a_connection()
    {
        // host is unreachable and never gets dialed: the guard must fire before any connection attempt,
        // proven by this call never hanging/timing out.
        var source = new SqlServerSource("Server=unreachable.invalid;Database=x;Connect Timeout=1;TrustServerCertificate=true");
        var spec = Spec(("procedure", "dbo.p"), ("partition_column", "id"));
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains(
            "dataset 'ds': partitioned reads are not supported for procedure datasets -- hint: expose the " +
            "underlying query via 'table' or 'query', or remove 'partition_column'",
            ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PlanReadAsync_rejects_partitions_count_before_opening_a_connection()
    {
        var source = new SqlServerSource("Server=unreachable.invalid;Database=x;Connect Timeout=1;TrustServerCertificate=true");
        var spec = Spec(("procedure", "dbo.p"), ("partitions", 4));
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("partitioned reads are not supported for procedure datasets", ex.Message, StringComparison.Ordinal);
    }

    // Two-way mutual exclusion now that `table:` is retired: only query and procedure can still
    // conflict. Covered here next to the rest of this file's procedure-mode coverage;
    // SqlServerSqlGenTests covers the same guard.
    [Fact]
    public void BuildSelect_rejects_procedure_combined_with_query()
    {
        var ex = Assert.Throws<PzConnectorException>(
            () => SqlServerSource.BuildSelect(Spec(("query", "select 1"), ("procedure", "dbo.p")), ReadHints.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("mutually exclusive", ex.Message, StringComparison.Ordinal);
    }
}
