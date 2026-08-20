using Pz.Connectors.Abstractions;

namespace Pz.Connector.AzureBlob;

/// <summary>Scheme/container/path parsing and blob-URL rendering for the azure connector. Accepts
/// az://, azure:// (normalized to az) and abfss://. Rendering and escaping are injection-safe: every
/// literal that reaches SQL goes through <see cref="Escape"/>.</summary>
internal readonly record struct AzureLocation(string Scheme, string Container, string Key);

internal static class AzureUrl
{
    /// <summary>Parses a source dataset location: container + path both required.</summary>
    public static AzureLocation ParseDataset(IReadOnlyDictionary<string, object?> options, string subject)
    {
        var scheme = ValidateScheme(Str(options, "scheme"), subject);
        var container = Require(options, "container", subject);
        var key = Require(options, "path", subject).Trim('/');
        return new AzureLocation(scheme, container, key);
    }

    /// <summary>Parses a sink output location: container required, path is an optional prefix, the object
    /// name (already computed by the caller from mode/format) is appended.</summary>
    public static AzureLocation ParseSink(IReadOnlyDictionary<string, object?> options, string subject, string objectName)
    {
        var scheme = ValidateScheme(Str(options, "scheme"), subject);
        var container = Require(options, "container", subject);
        var prefix = (Str(options, "path") ?? "").Trim('/');
        var key = prefix.Length > 0 ? $"{prefix}/{objectName}" : objectName;
        return new AzureLocation(scheme, container, key);
    }

    public static string Render(AzureLocation loc) => $"{loc.Scheme}://{loc.Container}/{loc.Key}";

    public static string ValidateScheme(string? raw, string subject)
    {
        var s = raw?.ToLowerInvariant();
        return s switch
        {
            null or "az" or "azure" => "az",
            "abfss" => "abfss",
            _ => throw new PzConnectorException(
                $"{subject}: azure 'scheme' must be one of 'az', 'azure', 'abfss' (got '{raw}')", isTransient: false),
        };
    }

    public static string Escape(string value) => value.Replace("'", "''");

    private static string? Str(IReadOnlyDictionary<string, object?> options, string key) =>
        options.TryGetValue(key, out var v) ? v?.ToString() : null;

    private static string Require(IReadOnlyDictionary<string, object?> options, string key, string subject) =>
        Str(options, key) is { Length: > 0 } s
            ? s
            : throw new PzConnectorException($"{subject}: azure requires '{key}'", isTransient: false);
}
