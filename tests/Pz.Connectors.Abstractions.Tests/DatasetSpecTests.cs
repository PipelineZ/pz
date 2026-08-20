using Pz.Connectors.Abstractions;

public class DatasetSpecTests
{
    [Fact]
    public void WatermarkLowerInclusive_defaults_false()
    {
        var spec = new DatasetSpec("source", "dataset", new Dictionary<string, object?>());
        Assert.False(spec.WatermarkLowerInclusive);
    }
}
