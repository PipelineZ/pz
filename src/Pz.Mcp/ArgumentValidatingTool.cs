using System.Text.Json;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Pz.Core.Validation;

namespace Pz.Mcp;

/// <summary>The SDK's own argument binder turns a binding failure into a generic tool result whose
/// whole text is "An error occurred
/// invoking '&lt;tool&gt;'." — no argument name, no expected type, nothing an agent can self-correct
/// from. This decorator pre-validates every call's arguments against the wrapped tool's own
/// published input schema — unknown names, missing required arguments, and JSON-kind mismatches —
/// and answers a real invalid-params (-32602) error naming the argument, so the binder only ever
/// sees arguments its schema already admitted. Validation is deliberately shallow (top-level names
/// and JSON kinds against the schema the server itself publishes): the goal is a self-correctable
/// message, not a JSON Schema implementation — anything deeper still binds.
///
/// It is also the last catch: a handler exception no typed catch classified would otherwise hit the
/// SDK's own catch and become that same generic text with the exception discarded, so one is
/// translated into a PZ0609 envelope here. Between the two, "An error occurred invoking
/// '&lt;tool&gt;'." is a string no pz MCP client can receive — every failure names something.</summary>
internal sealed class ArgumentValidatingTool(McpServerTool inner) : McpServerTool
{
    public override Tool ProtocolTool => inner.ProtocolTool;

    public override IReadOnlyList<object> Metadata => inner.Metadata;

    public override async ValueTask<CallToolResult> InvokeAsync(
        RequestContext<CallToolRequestParams> request, CancellationToken cancellationToken = default)
    {
        Validate(request.Params?.Arguments);
        try
        {
            return await inner.InvokeAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException and not McpProtocolException)
        {
            // The backstop, not a diagnosis: the SDK's own catch would discard `ex` behind "An error
            // occurred invoking '<tool>'." and log nowhere (`pz mcp` wires no ILoggerFactory), so an
            // agent would get a dead end and the operator no trace. Carrying the exception text out
            // keeps PZ0609 self-diagnosing -- it names the handler that needs a typed catch.
            return new CallToolResult
            {
                IsError = true,
                Content = [new TextContentBlock { Text = ToolEnvelope.Errors([Failure(ex)], applied: false) }],
            };
        }
    }

    private PzError Failure(Exception ex) => new(
        PzErrorCode.McpToolFailed,
        $"'{ProtocolTool.Name}' failed: {ex.GetType().Name}: {ex.Message}",
        null, null,
        "this is a pz defect, not a bad argument -- re-check the arguments against the tool's input " +
        "schema, and report it at https://github.com/PipelineZ/pz/issues with this message");

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

            // A property schema is not necessarily an object: JSON Schema's `true`/`false` are whole
            // schemas ("anything" / "nothing"), and that is exactly what the SDK emits for a
            // JsonElement-typed parameter. TryGetProperty THROWS on a non-object element, so the kind
            // check is load-bearing -- without it this decorator becomes the very thing it exists to
            // prevent, turning every pz_add_connection call into "An error occurred invoking ...".
            if (propertySchema.ValueKind != JsonValueKind.Object)
            {
                continue; // `true` admits any kind; anything else is the binder's call, not ours
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
