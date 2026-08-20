using System.Text.Json.Nodes;

namespace Pz.Engine.Validation;

/// <summary>Converts a loader-produced YAML value tree into a <see cref="JsonNode"/> so it can be
/// evaluated against a connector-published JSON Schema. The loader (<c>Pz.Core.Loading.YamlMapper</c> +
/// <c>EnvInterpolator</c>) only ever produces scalars of type <c>long</c>/<c>double</c>/<c>bool</c>/
/// <c>string</c>/<c>null</c>, and containers of type <c>Dictionary&lt;string, object?&gt;</c>/
/// <c>List&lt;object?&gt;</c> -- this is a closed conversion over exactly that shape, not a general
/// object-to-JSON serializer.
///
/// Connector options may also be written as <c>source()</c>/<c>sink()</c> keyword arguments, where
/// Scriban produces <c>int</c> in place of the YAML loader's <c>long</c>. Both integer shapes are
/// accepted; the set is otherwise still closed.</summary>
internal static class YamlToJson
{
    public static JsonNode? Convert(object? yamlValue) => yamlValue switch
    {
        null => null,
        long l => JsonValue.Create(l),
        int i => JsonValue.Create(i),
        double d => JsonValue.Create(d),
        bool b => JsonValue.Create(b),
        string s => JsonValue.Create(s),
        Dictionary<string, object?> dict => ConvertObject(dict),
        List<object?> list => ConvertArray(list),
        _ => throw new ArgumentException(
            $"YamlToJson.Convert: unsupported value type '{yamlValue.GetType()}' -- only loader-shaped " +
            "values (long/double/bool/string/null, Dictionary<string,object?>, List<object?>) are supported.",
            nameof(yamlValue)),
    };

    private static JsonObject ConvertObject(Dictionary<string, object?> dict)
    {
        var obj = new JsonObject();
        foreach (var (key, value) in dict)
        {
            obj[key] = Convert(value);
        }

        return obj;
    }

    private static JsonArray ConvertArray(List<object?> list)
    {
        var array = new JsonArray();
        foreach (var item in list)
        {
            array.Add(Convert(item));
        }

        return array;
    }
}
