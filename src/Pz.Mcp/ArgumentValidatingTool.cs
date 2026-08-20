using System.Text.Json;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Pz.Mcp;

/// <summary>The SDK's own argument binder turns a binding failure into a generic tool result whose
/// whole text is "An error occurred
/// invoking '&lt;tool&gt;'." — no argument name, no expected type, nothing an agent can self-correct
/// from. This decorator pre-validates every call's arguments against the wrapped tool's own
/// published input schema — unknown names, missing required arguments, and JSON-kind mismatches —
/// and answers a real invalid-params (-32602) error naming the argument, so the binder only ever
/// sees arguments its schema already admitted. Validation is deliberately shallow (top-level names
/// and JSON kinds against the schema the server itself publishes): the goal is a self-correctable
/// message, not a JSON Schema implementation — anything deeper still binds, and a handler-level
/// failure still rides the envelope.</summary>
internal sealed class ArgumentValidatingTool(McpServerTool inner) : McpServerTool
{
    public override Tool ProtocolTool => inner.ProtocolTool;

    public override IReadOnlyList<object> Metadata => inner.Metadata;

    public override ValueTask<CallToolResult> InvokeAsync(
        RequestContext<CallToolRequestParams> request, CancellationToken cancellationToken = default)
    {
        Validate(request.Params?.Arguments);
        return inner.InvokeAsync(request, cancellationToken);
    }

    private void Validate(IDictionary<string, JsonElement>? arguments)
    {
        var name = ProtocolTool.Name;
        var schema = ProtocolTool.InputSchema;
        var properties = schema.TryGetProperty("properties", out var props) && props.ValueKind == JsonValueKind.Object
            ? props
            : default;

        if (schema.TryGetProperty("required", out var required) && required.ValueKind == JsonValueKind.Array)
        {
            foreach (var entry in required.EnumerateArray())
            {
                var requiredName = entry.GetString();
                if (requiredName is not null && (arguments is null || !arguments.ContainsKey(requiredName)))
                {
                    throw new McpProtocolException(
                        $"invalid params for '{name}': required argument '{requiredName}' is missing",
                        McpErrorCode.InvalidParams);
                }
            }
        }

        if (arguments is null || properties.ValueKind != JsonValueKind.Object)
        {
            return;
        }

        foreach (var (argumentName, value) in arguments)
        {
            if (!properties.TryGetProperty(argumentName, out var propertySchema))
            {
                var known = string.Join(", ", properties.EnumerateObject().Select(p => p.Name));
                throw new McpProtocolException(
                    $"invalid params for '{name}': unknown argument '{argumentName}' (accepted: {known})",
                    McpErrorCode.InvalidParams);
            }

            if (value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            {
                continue; // explicit null falls through to the binder's own optional/default handling
            }

            if (propertySchema.TryGetProperty("type", out var type) && type.ValueKind == JsonValueKind.String &&
                type.GetString() is { } expected && !Matches(expected, value.ValueKind))
            {
                throw new McpProtocolException(
                    $"invalid params for '{name}': argument '{argumentName}' expects {expected}, " +
                    $"got {Describe(value.ValueKind)}",
                    McpErrorCode.InvalidParams);
            }
        }
    }

    private static bool Matches(string schemaType, JsonValueKind kind) => schemaType switch
    {
        "string" => kind == JsonValueKind.String,
        "boolean" => kind is JsonValueKind.True or JsonValueKind.False,
        "integer" or "number" => kind == JsonValueKind.Number,
        "array" => kind == JsonValueKind.Array,
        "object" => kind == JsonValueKind.Object,
        _ => true, // unions and unrecognized types fall through to the binder
    };

    private static string Describe(JsonValueKind kind) => kind switch
    {
        JsonValueKind.True or JsonValueKind.False => "boolean",
        JsonValueKind.String => "string",
        JsonValueKind.Number => "number",
        JsonValueKind.Array => "array",
        JsonValueKind.Object => "object",
        _ => kind.ToString().ToLowerInvariant(),
    };
}
