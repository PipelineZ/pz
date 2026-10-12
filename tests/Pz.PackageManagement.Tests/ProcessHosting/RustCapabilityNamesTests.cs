using System.Text.RegularExpressions;
using Pz.Connectors.Abstractions;

namespace Pz.PackageManagement.Tests.ProcessHosting;

public sealed partial class RustCapabilityNamesTests
{
    [Fact]
    public void Rust_capability_table_names_every_connector_capability_exactly()
    {
        var root = AppContext.BaseDirectory;
        while (!File.Exists(Path.Combine(root, "Pz.slnx")))
        {
            root = Path.GetDirectoryName(root) ?? throw new InvalidOperationException("Pz.slnx not found.");
        }

        var source = File.ReadAllText(Path.Combine(root, "rust", "pz-connector", "src", "server.rs"));
        var start = source.IndexOf("const CAPABILITY_NAMES", StringComparison.Ordinal);
        Assert.True(start >= 0, "CAPABILITY_NAMES not found in server.rs.");
        var end = source.IndexOf("];", start, StringComparison.Ordinal);

        var rust = EntryPattern().Matches(source[start..end])
            .ToDictionary(m => long.Parse(m.Groups[1].Value.Replace("_", "")), m => m.Groups[2].Value);

        var expected = Enum.GetValues<ConnectorCapabilities>()
            .Where(c => c != ConnectorCapabilities.None)
            .ToDictionary(c => (long)c, c => c.ToString());

        Assert.Equal(expected.OrderBy(e => e.Key), rust.OrderBy(e => e.Key));
    }

    [GeneratedRegex(@"\(\s*([0-9_]+)\s*,\s*""(\w+)""\s*\)")]
    private static partial Regex EntryPattern();
}
