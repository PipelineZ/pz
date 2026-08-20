using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

public class HttpDatasetConfigTests
{
    private static DatasetSpec Spec(Dictionary<string, object?> options) => new("s", "d", options);

    [Fact]
    public void Parses_full_raw_incremental_dataset()
    {
        var config = HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/issues",
            ["query"] = new Dictionary<string, object?> { ["state"] = "all" },
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "link_header" },
            ["cursor"] = "updated_at",
            ["cursor_type"] = "timestamp",
            ["max_pages"] = 10L,
        }));

        Assert.Equal("/issues", config.Path);
        Assert.Equal("all", config.Query["state"]);
        Assert.NotNull(config.PageStrategyFactory);
        Assert.False(config.IsContractMode);
        Assert.Equal("/updated_at", config.CursorPointer);
        Assert.Equal(10, config.MaxPages);
    }

    [Fact]
    public void Aggregates_option_errors_into_one_permanent_exception()
    {
        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(Spec(new()
        {
            // missing path; cursor without cursor_type; bad max_pages; unknown strategy
            ["cursor"] = "updated_at",
            ["max_pages"] = 0L,
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "spiral" },
        })));

        Assert.False(ex.IsTransient);
        Assert.Contains("http dataset 's.d'", ex.Message);
        Assert.Contains("path", ex.Message);
        Assert.Contains("cursor_type", ex.Message);
        Assert.Contains("max_pages", ex.Message);
        Assert.Contains("spiral", ex.Message);
    }

    [Fact]
    public void Contract_mode_rejects_cursor_type_and_requires_declared_cursor()
    {
        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/x",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
            ["cursor"] = "updated_at",       // not in columns
            ["cursor_type"] = "timestamp",   // forbidden in contract mode
        })));

        Assert.Contains("cursor_type", ex.Message);
        Assert.Contains("updated_at", ex.Message);
    }

    [Fact]
    public void Raw_mode_rejects_cursor_name_payload_collision()
    {
        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/items",
            ["cursor"] = "payload",
            ["cursor_type"] = "int",
        })));

        Assert.Contains("payload", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Malformed_query_binding_template_is_rejected_offline()
    {
        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/items",
            ["query"] = new Dictionary<string, object?> { ["since"] = "{{ watermark" },
        })));

        Assert.False(ex.IsTransient);
        Assert.Contains("since", ex.Message);
        Assert.Contains("malformed", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Unknown_query_binding_name_is_rejected_offline()
    {
        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/items",
            ["query"] = new Dictionary<string, object?> { ["x"] = "{{ nope }}" },
        })));

        Assert.False(ex.IsTransient);
        Assert.Contains("x", ex.Message);
        Assert.Contains("nope", ex.Message);
        Assert.Contains("unknown binding", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Pagination_rejects_non_numeric_start()
    {
        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/items",
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "page", ["start"] = "abc" },
        })));

        Assert.False(ex.IsTransient);
        Assert.Contains("'start'", ex.Message);
        Assert.Contains("abc", ex.Message);
    }

    [Fact]
    public void Pagination_rejects_negative_size()
    {
        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/items",
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "page", ["size"] = -5L },
        })));

        Assert.False(ex.IsTransient);
        Assert.Contains("'size'", ex.Message);
        Assert.Contains("-5", ex.Message);
    }

    [Fact]
    public void Parses_cursor_order_on_a_raw_cursor_dataset()
    {
        var config = HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/items",
            ["cursor"] = "updated_at",
            ["cursor_type"] = "timestamp",
            ["cursor_order"] = "desc",
        }));
        Assert.Equal("desc", config.CursorOrder);
    }

    [Fact]
    public void Parses_cursor_order_on_a_contract_dataset()
    {
        // Contract mode: the cursor is an engine-level concern (incremental.cursor names a
        // column), invisible to Parse -- cursor_order is accepted on the contract's word.
        var config = HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/items",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
            ["cursor_order"] = "asc",
        }));
        Assert.Equal("asc", config.CursorOrder);
    }

    [Fact]
    public void Rejects_invalid_cursor_order_value()
    {
        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/items",
            ["cursor"] = "updated_at",
            ["cursor_type"] = "timestamp",
            ["cursor_order"] = "newest-first",
        })));
        Assert.False(ex.IsTransient);
        Assert.Contains("cursor_order", ex.Message);
        Assert.Contains("asc", ex.Message);
    }

    [Fact]
    public void Rejects_cursor_order_without_any_cursor()
    {
        // No raw cursor option and no columns contract: ordering describes nothing.
        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/items",
            ["cursor_order"] = "asc",
        })));
        Assert.Contains("cursor_order", ex.Message);
        Assert.Contains("requires a cursor", ex.Message);
    }

    [Fact]
    public void NaturalReadShape_with_delta_pointer_returns_feed()
    {
        var spec = Spec(new()
        {
            ["path"] = "/items",
            ["delta_pointer"] = "/deltaLink",
        });
        var config = new HttpConnectionConfig(new Uri("http://localhost/"), null, new Dictionary<string, string>(), null,
            HttpConnectionConfig.DefaultTimeout, HttpConnectionConfig.DefaultMaxResponseBytes, []);
        var source = new HttpSource(config);
        Assert.Equal(NaturalReadShape.Feed, ((INaturalReadShapeSource)source).GetNaturalReadShape(spec));
    }

    [Fact]
    public void NaturalReadShape_without_delta_pointer_returns_full()
    {
        var spec = Spec(new()
        {
            ["path"] = "/items",
        });
        var config = new HttpConnectionConfig(new Uri("http://localhost/"), null, new Dictionary<string, string>(), null,
            HttpConnectionConfig.DefaultTimeout, HttpConnectionConfig.DefaultMaxResponseBytes, []);
        var source = new HttpSource(config);
        Assert.Equal(NaturalReadShape.Full, ((INaturalReadShapeSource)source).GetNaturalReadShape(spec));
    }
}
