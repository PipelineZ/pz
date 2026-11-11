using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Sql;

namespace Pz.Connectors.Toolkit.Tests.Sql;

public sealed class NativeScanWindowTests
{
    private const string Fragment = "read_parquet('data/*.parquet')";

    private static DatasetSpec Spec() => new("c", "e", new Dictionary<string, object?>());

    [Fact]
    public void An_unwindowed_spec_leaves_the_fragment_untouched()
    {
        Assert.Equal(Fragment, NativeScanWindow.Wrap(Fragment, Spec()));
        Assert.Equal(Fragment, NativeScanWindow.Wrap(Fragment, Spec() with { WatermarkCursor = "ts", WatermarkValue = "1" }));
    }

    [Fact]
    public void A_window_wraps_the_fragment_in_quoted_escaped_bounds()
    {
        var spec = Spec() with { WatermarkCursor = "up\"dated", WatermarkValue = "a'1", WatermarkUpperBound = "b'2" };

        Assert.Equal(
            $"(select * from {Fragment} where \"up\"\"dated\" > 'a''1' and \"up\"\"dated\" <= 'b''2')",
            NativeScanWindow.Wrap(Fragment, spec));
    }

    [Fact]
    public void An_upper_bound_without_a_lower_bound_is_refused()
    {
        var spec = Spec() with { WatermarkCursor = "ts", WatermarkUpperBound = "9" };

        Assert.Throws<InvalidOperationException>(() => NativeScanWindow.Wrap(Fragment, spec));
    }
}
