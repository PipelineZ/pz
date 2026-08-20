using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Bindings;

namespace Pz.Connectors.Toolkit.Tests.Bindings;

public class BindingExpanderTests
{
    private static string Raw(string name, BindingValue v) => v.Value!;

    [Fact]
    public void Substitutes_known_binding()
    {
        var bindings = new Dictionary<string, BindingValue> { ["watermark"] = new("2026-07-01", "date") };
        Assert.True(BindingExpander.TryExpand("since={{ watermark }}", bindings, Raw, out var result, out _));
        Assert.Equal("since=2026-07-01", result);
    }

    [Fact]
    public void Null_binding_yields_null_result_for_param_omission()
    {
        var bindings = new Dictionary<string, BindingValue> { ["watermark"] = new(null, null) };
        Assert.True(BindingExpander.TryExpand("{{ watermark }}", bindings, Raw, out var result, out _));
        Assert.Null(result);
    }

    [Fact]
    public void Unknown_binding_is_an_error_not_passthrough()
    {
        var bindings = new Dictionary<string, BindingValue> { ["watermark"] = new("1", "int") };
        Assert.False(BindingExpander.TryExpand("{{ run_id }}", bindings, Raw, out _, out var error));
        Assert.Contains("run_id", error);
    }

    [Fact]
    public void Template_without_references_passes_through()
    {
        Assert.True(BindingExpander.TryExpand("all", new Dictionary<string, BindingValue>(), Raw,
            out var result, out _));
        Assert.Equal("all", result);
    }

    [Fact]
    public void Referenced_bindings_lists_names_and_rejects_malformed()
    {
        Assert.Equal(["watermark"], BindingExpander.ReferencedBindings("x={{ watermark }}&y={{watermark}}"));
        Assert.Throws<FormatException>(() => BindingExpander.ReferencedBindings("bad {{ watermark"));
    }

    [Fact]
    public void Formatter_controls_rendering()
    {
        var bindings = new Dictionary<string, BindingValue> { ["watermark"] = new("a b", "varchar") };
        Assert.True(BindingExpander.TryExpand("{{ watermark }}", bindings, (_, v) => Uri.EscapeDataString(v.Value!),
            out var result, out _));
        Assert.Equal("a%20b", result);
    }

    [Fact]
    public void FromSpec_exposes_watermark_and_window_upper()
    {
        var spec = new DatasetSpec("s", "d", new Dictionary<string, object?>()) { WatermarkValue = "42" };
        var bindings = BindingExpander.FromSpec(spec);
        Assert.Equal(["watermark", "window_upper"], bindings.Keys.Order().ToArray());
        Assert.Equal("42", bindings["watermark"].Value);
    }

    [Fact]
    public void FromSpec_exposes_window_upper_from_watermark_upper_bound()
    {
        var spec = new DatasetSpec("s", "d", new Dictionary<string, object?>())
        {
            WatermarkCursor = "updated_at",
            WatermarkValue = "2024-01-01T00:00:00Z",
            WatermarkUpperBound = "2024-01-08T00:00:00Z",
        };

        var bindings = BindingExpander.FromSpec(spec);

        Assert.Equal("2024-01-01T00:00:00Z", bindings["watermark"].Value);
        Assert.Equal("2024-01-08T00:00:00Z", bindings["window_upper"].Value);
    }

    [Fact]
    public void FromSpec_window_upper_is_null_when_not_windowed()
    {
        var spec = new DatasetSpec("s", "d", new Dictionary<string, object?>())
        {
            WatermarkCursor = "updated_at",
            WatermarkValue = "2024-01-01T00:00:00Z",
        };

        var bindings = BindingExpander.FromSpec(spec);

        Assert.True(bindings.ContainsKey("window_upper"));
        Assert.True(bindings["window_upper"].IsNull);
    }

    [Fact]
    public void Unknown_binding_reported_even_when_another_binding_is_null()
    {
        var bindings = new Dictionary<string, BindingValue> { ["watermark"] = new(null, null) };
        Assert.False(BindingExpander.TryExpand("{{ watermark }}&x={{ unknown_name }}", bindings, Raw, out _, out var error));
        Assert.Contains("unknown_name", error);
    }

    [Fact]
    public void Repeated_placeholder_substitutes_all_occurrences()
    {
        var bindings = new Dictionary<string, BindingValue> { ["watermark"] = new("2026-07-01", "date") };
        Assert.True(BindingExpander.TryExpand("a={{ watermark }}&b={{ watermark }}", bindings, Raw, out var result, out _));
        Assert.Equal("a=2026-07-01&b=2026-07-01", result);
    }
}
