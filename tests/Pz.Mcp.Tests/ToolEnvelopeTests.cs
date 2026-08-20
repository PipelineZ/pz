using Pz.Core.Validation;
using Pz.Mcp;

public class ToolEnvelopeTests
{
    [Fact]
    public void Error_envelope_serializes_all_pzerror_fields_in_fixed_order()
    {
        var errors = new List<PzError>
        {
            new(PzErrorCode.YamlShape, "Malformed YAML: bad indent", "connections.yml", 12, "fix the YAML syntax near this location"),
        };
        var json = ToolEnvelope.Errors(errors);
        Assert.Equal(
            "{\"ok\":false,\"errors\":[{\"code\":\"PZ0101\",\"message\":\"Malformed YAML: bad indent\"," +
            "\"file\":\"connections.yml\",\"line\":12,\"next_step\":\"fix the YAML syntax near this location\"}]}",
            json);
    }

    [Fact]
    public void Ok_envelope_with_result_and_applied()
    {
        var json = ToolEnvelope.Ok(w => { w.WriteStartObject("result"); w.WriteNumber("nodes", 3); w.WriteEndObject(); }, applied: true);
        Assert.Equal("{\"ok\":true,\"applied\":true,\"result\":{\"nodes\":3}}", json);
    }

    [Fact]
    public void Null_line_and_file_are_omitted_not_nulled()
    {
        var errors = new List<PzError> { new(PzErrorCode.VarsInvalid, "boom", null, null, "fix it") };
        var json = ToolEnvelope.Errors(errors);
        Assert.DoesNotContain("null", json);
        Assert.Contains("\"next_step\":\"fix it\"", json);
    }
}
