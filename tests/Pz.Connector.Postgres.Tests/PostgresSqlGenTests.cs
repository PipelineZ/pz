using Npgsql;
using Pz.Connector.Postgres;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Postgres.Tests;

/// <summary>Unit tests for <see cref="PostgresSource.BuildSelect"/>, the SQL-generation heart of the
/// Postgres source: entity-mode column pruning + predicate pushdown, query-mode passthrough, entity
/// name resolution (the dataset name IS the object name), and
/// identifier quoting (SQL-injection surface).</summary>
public class PostgresSqlGenTests
{
    private static DatasetSpec TableSpec(IReadOnlyDictionary<string, object?> extra) =>
        new("pg", "users", extra);

    private static DatasetSpec EntitySpec(string entity) => new("pg", entity, new Dictionary<string, object?>());

    [Fact]
    public void Table_mode_prunes_columns_and_pushes_predicate()
    {
        var spec = EntitySpec("users");
        var hints = new ReadHints(Columns: ["id", "email"], PredicateSql: "id > 10");

        var sql = PostgresSource.BuildSelect(spec, hints);

        Assert.Equal("select \"id\", \"email\" from \"public\".\"users\" where (id > 10)", sql);
    }

    [Fact]
    public void Query_mode_ignores_hints()
    {
        var spec = TableSpec(new Dictionary<string, object?> { ["query"] = "select * from users" });
        var hints = new ReadHints(Columns: ["id"], PredicateSql: "id > 10");

        var sql = PostgresSource.BuildSelect(spec, hints);

        Assert.Equal("select * from users", sql);
    }

    [Fact]
    public void An_unqualified_entity_reads_from_the_default_schema()
    {
        Assert.Equal("select * from \"public\".\"orders\"",
            PostgresSource.BuildSelect(EntitySpec("orders"), ReadHints.None));
    }

    [Fact]
    public void A_dotted_entity_reads_from_its_own_schema()
    {
        Assert.Equal("select * from \"raw\".\"orders\"",
            PostgresSource.BuildSelect(EntitySpec("raw.orders"), ReadHints.None));
    }

    // A cross-database name would otherwise be quoted as ONE identifier literally called
    // "db.raw"."orders" -- a silent wrong read. Refused instead.
    [Fact]
    public void A_three_part_entity_is_refused_rather_than_quoted_wrong()
    {
        var ex = Assert.Throws<PzConnectorException>(
            () => PostgresSource.BuildSelect(EntitySpec("db.raw.orders"), ReadHints.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("db.raw.orders", ex.Message, StringComparison.Ordinal);
    }

    // query: wins over the entity name -- the name is then just a label for the node.
    [Fact]
    public void Query_mode_ignores_the_entity_name()
    {
        var spec = new DatasetSpec("pg", "db.raw.orders", new Dictionary<string, object?>
        {
            ["query"] = "select 1",
        });

        Assert.Equal("select 1", PostgresSource.BuildSelect(spec, ReadHints.None));
    }

    // An offset-less watermark literal (e.g. "2026-05-01T12:00:00.000000", the canonical UTC
    // form pz captures) is coerced against a `timestamptz` cursor column in the SESSION time zone. If that
    // zone is not UTC, `cursor > 'wm'` shifts the boundary by the offset -- silently skipping (loss) or
    // re-reading (duplication) a band of rows, then advancing the watermark past the skipped ones forever.
    // Pinning the Npgsql session to UTC makes the coercion exact for the UTC-by-convention values pz stores.
    [Fact]
    public void Connection_pins_session_timezone_to_utc()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["host"] = "localhost",
            ["database"] = "pz",
        });

        var parsed = new NpgsqlConnectionStringBuilder(PostgresConnector.BuildConnectionString(config));

        Assert.Equal("UTC", parsed.Timezone);
    }

    [Fact]
    public void Identifier_quotes_are_doubled()
    {
        var spec = EntitySpec("users");
        var hints = new ReadHints(Columns: ["we\"ird"]);

        var sql = PostgresSource.BuildSelect(spec, hints);

        Assert.Equal("select \"we\"\"ird\" from \"public\".\"users\"", sql);
    }



    // A non-integer-shaped "partitions" value must surface as a named
    // PzConnectorException, not a raw FormatException off Convert.ToInt32. ParsePartitionCount runs
    // synchronously before PlanReadAsync ever opens a connection (the partition_column-gated probe only
    // happens after a valid partition count is parsed), so this is a pure unit test -- no Testcontainers,
    // no real connection string needed.
    [Fact]
    public async Task PlanReadAsync_non_integer_partitions_value_is_a_named_error()
    {
        ISource source = new PostgresSource("Host=unused;Database=unused");
        var spec = TableSpec(new Dictionary<string, object?>
        {
            ["partition_column"] = "id",
            ["partitions"] = "four",
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("'partitions' must be an integer", ex.Message, StringComparison.Ordinal);
        Assert.Contains("'four'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Published_schemas_are_valid_json_schema()
    {
        var c = new PostgresConnector();
        foreach (var s in new[] { c.ConnectionConfigSchema, c.DatasetConfigSchema })
        {
            var schema = Json.Schema.JsonSchema.FromText(s); // throws on malformed
            Assert.NotNull(schema);
        }
    }

    // The bounded-window upper bound joins the SAME AND-chain as the lower-bound watermark
    // predicate above -- see PostgresSource.BuildSelect's watermark block.
    [Fact]
    public void Window_upper_bound_joins_the_predicate_chain()
    {
        var spec = new DatasetSpec("pg", "orders", new Dictionary<string, object?>())
        {
            WatermarkCursor = "updated_at",
            WatermarkValue = "2020-01-01T00:00:00.000000",
            WatermarkUpperBound = "2020-01-02T00:00:00.000000",
        };
        var select = PostgresSource.BuildSelect(spec, ReadHints.None);
        Assert.Contains("\"updated_at\" > '2020-01-01T00:00:00.000000'", select);
        Assert.Contains("\"updated_at\" <= '2020-01-02T00:00:00.000000'", select);
    }

    [Fact]
    public void Upper_bound_literal_is_quote_doubled()
    {
        var spec = new DatasetSpec("pg", "orders", new Dictionary<string, object?>())
        {
            WatermarkCursor = "c",
            WatermarkValue = "1",
            WatermarkUpperBound = "o''brien", // engine never produces this; defense-in-depth like the lower bound
        };
        var select = PostgresSource.BuildSelect(spec, ReadHints.None);
        Assert.Contains("'o''''brien'", select);
    }

    [Fact]
    public void Connector_declares_bounded_window()
    {
        Assert.True(new PostgresConnector().Capabilities.HasFlag(ConnectorCapabilities.BoundedWindow));
    }

    [Fact]
    public void Watermark_lower_inclusive_true_uses_greater_equal()
    {
        var spec = new DatasetSpec("pg", "orders", new Dictionary<string, object?>())
        {
            WatermarkCursor = "updated_at",
            WatermarkValue = "2020-01-01T00:00:00.000000",
            WatermarkLowerInclusive = true,
        };
        var select = PostgresSource.BuildSelect(spec, ReadHints.None);
        Assert.Contains("\"updated_at\" >= '2020-01-01T00:00:00.000000'", select);
    }

    [Fact]
    public void Watermark_lower_inclusive_false_uses_greater()
    {
        var spec = new DatasetSpec("pg", "orders", new Dictionary<string, object?>())
        {
            WatermarkCursor = "updated_at",
            WatermarkValue = "2020-01-01T00:00:00.000000",
            WatermarkLowerInclusive = false,
        };
        var select = PostgresSource.BuildSelect(spec, ReadHints.None);
        Assert.Contains("\"updated_at\" > '2020-01-01T00:00:00.000000'", select);
    }

    [Fact]
    public void Connector_declares_inclusive_watermark_bound()
    {
        Assert.True(new PostgresConnector().Capabilities.HasFlag(ConnectorCapabilities.InclusiveWatermarkBound));
    }

    // A cursor-set/value-null spec is the first-run shape SpecBuilder stamps for every incremental
    // dataset (DatasetSpec.WatermarkCursor's doc comment: the predicate applies only "when set
    // (alongside WatermarkValue)"). The gate must key
    // off WatermarkValue, not WatermarkCursor alone, or this throws NullReferenceException instead of
    // producing the same unbounded SELECT as a watermark-free spec.
    [Fact]
    public void Cursor_set_value_null_produces_same_select_as_no_watermark()
    {
        var options = new Dictionary<string, object?>();
        var withCursorOnly = new DatasetSpec("pg", "orders", options) { WatermarkCursor = "updated_at" };
        var withoutWatermark = new DatasetSpec("pg", "orders", options);

        var cursorOnlySql = PostgresSource.BuildSelect(withCursorOnly, ReadHints.None);
        var noWatermarkSql = PostgresSource.BuildSelect(withoutWatermark, ReadHints.None);

        Assert.Equal(noWatermarkSql, cursorOnlySql);
        Assert.DoesNotContain("updated_at", cursorOnlySql, StringComparison.Ordinal);
    }
}
