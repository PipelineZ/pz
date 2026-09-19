using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit.Reference;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.Validation;

namespace Pz.Engine.Tests.Validation;

/// <summary>Tier 3 (`ConnectorConfigValidator`): schema validation of connection/dataset blocks plus
/// each connector's own cross-field <c>ValidateAsync</c>, aggregated across the whole project. Builds
/// tiny in-memory <see cref="PzProject"/>s the way <c>NodeExecutorTests</c> does; a
/// <see cref="StubConnector"/> stands in for a real connector wherever the test needs a specific,
/// restrictive schema.</summary>
public sealed class ConnectorConfigValidatorTests
{
    // Mirrors LocalFilesConnector's real schemas -- copied here rather than depending on the
    // concrete connectors/Pz.Connector.LocalFiles project from Pz.Engine.Tests.
    private const string LocalFilesConnectionSchema =
        """{ "type": "object", "properties": { "root": { "type": "string" } }, "additionalProperties": false }""";
    private const string LocalFilesDatasetSchema =
        """{ "type": "object", "required": ["path"], "properties": { "path": { "type": "string" }, "format": { "enum": ["csv"] }, "columns": { "type": "object", "minProperties": 1, "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } } }, "additionalProperties": false }""";
    private const string PostgresConnectionSchema =
        """{ "type": "object", "required": ["host","database"], "properties": { "host": { "type": "string" }, "port": { "type": "integer" }, "database": { "type": "string" } }, "additionalProperties": false }""";

    private static PzProject Project(IReadOnlyList<ConnectionDef>? sources = null, IReadOnlyList<ConnectionDef>? sinks = null) =>
        new("proj", "0.1.0", new EngineConfig(), new Dictionary<string, object?>(), [],
            [.. sources ?? [], .. sinks ?? []], []);

    [Fact]
    public async Task Valid_config_produces_no_errors()
    {
        var mem = new InMemoryConnector();
        var registry = new ConnectorRegistry();
        registry.AddSource("inmemory", mem);
        registry.AddSink("inmemory", mem);

        var source = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", new Dictionary<string, object?> { ["rows"] = 100L }, null)], "sources/mem.yml");
        var sink = new ConnectionDef("out", "inmemory", new Dictionary<string, object?>(), [], "sinks/out.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project([source], [sink]), registry, default);

        Assert.Empty(errors);
    }

    // A connector's own ValidateAsync may report a non-blocking warning (ValidationResult.Warnings)
    // alongside zero errors -- the optional `warnings` out-parameter collects it as a PZ0364 without
    // failing validation; a caller that does not pass the parameter (every pre-existing call site)
    // still sees nothing but errors, unchanged.
    [Fact]
    public async Task A_connector_warning_is_collected_without_failing_validation()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new StubConnector
        {
            ValidateFunc = _ => ValidationResult.Success with { Warnings = ["no host key is pinned"] },
        });

        var source = new ConnectionDef("db", "stub", new Dictionary<string, object?>(), [], "sources/db.yml");
        var warnings = new List<PzWarning>();

        var errors = await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default, warnings);

        Assert.Empty(errors);
        var warning = Assert.Single(warnings);
        Assert.Equal(PzErrorCode.ConnectorConfigWarning, warning.Code);
        Assert.Contains("db", warning.Message, StringComparison.Ordinal);
        Assert.Contains("no host key is pinned", warning.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Omitting_the_warnings_parameter_still_reports_errors_normally()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new StubConnector
        {
            ValidateFunc = _ => ValidationResult.Failed("bad config") with { Warnings = ["also a warning"] },
        });

        var source = new ConnectionDef("db", "stub", new Dictionary<string, object?>(), [], "sources/db.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default);

        var error = Assert.Single(errors);
        Assert.Contains("bad config", error.Message, StringComparison.Ordinal);
    }

    // An option written as a source() keyword argument reaches this tier as an int (Scriban), not the
    // long a YAML scalar produces; the schema converter's type set must accept both or the whole
    // command dies with an unhandled exception.
    [Fact]
    public async Task An_integer_option_written_at_a_call_site_validates()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("postgres", new StubConnector { ConnectionConfigSchema = PostgresConnectionSchema });

        var source = new ConnectionDef("crm", "postgres",
            new Dictionary<string, object?> { ["host"] = "db", ["database"] = "crm", ["port"] = 5432 },
            [], "connections.yml");

        Assert.Empty(await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default));
    }

    [Fact]
    public async Task Unknown_connection_key_is_PZ0301_naming_file_and_path()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("localfiles", new StubConnector { ConnectionConfigSchema = LocalFilesConnectionSchema });

        var source = new ConnectionDef("crm", "localfiles",
            new Dictionary<string, object?> { ["root"] = "/tmp", ["bogus"] = "nope" },
            [], "connections.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default);

        var error = Assert.Single(errors);
        Assert.Equal(PzErrorCode.ConnectorConfigInvalid, error.Code);
        Assert.Equal("connection 'crm': unknown option 'bogus'", error.Message);
        Assert.Equal("connections.yml", error.File);
        // The accepted set is the whole next step -- a misremembered option name is fixed by seeing
        // the real ones, and the raw schema message ("All values fail against the false schema")
        // named neither the offending key nor the alternatives.
        Assert.Equal("remove or rename it -- accepted options: root", error.Hint);
    }

    /// <summary>`additionalProperties` is equally legal as a SUBSCHEMA -- localfiles' <c>columns</c>
    /// maps every column name to a type enum that way. A violation under one of those means "this
    /// value is wrong", not "this key is unknown", and must keep its own message.</summary>
    [Fact]
    public async Task A_subschema_additionalProperties_violation_is_not_called_an_unknown_option()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("localfiles", new StubConnector { DatasetConfigSchema = LocalFilesDatasetSchema });

        var source = new ConnectionDef("crm", "localfiles", new Dictionary<string, object?>(),
            [new DatasetDef("customers", new Dictionary<string, object?> { ["path"] = "c.csv" },
                new Dictionary<string, string> { ["id"] = "notatype" })],
            "connections.yml");

        var error = Assert.Single(await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default));

        Assert.DoesNotContain("unknown option", error.Message, StringComparison.Ordinal);
        Assert.Contains("/columns/id", error.Message, StringComparison.Ordinal);
    }

    /// <summary>A nested unknown key names the object it sits in; a top-level one does not, or the
    /// message stutters ("/bogus: unknown option 'bogus'").</summary>
    [Fact]
    public async Task A_nested_unknown_option_names_the_block_it_sits_in()
    {
        const string nestedSchema =
            """{ "type": "object", "properties": { "tls": { "type": "object", "properties": { "verify": { "type": "boolean" } }, "additionalProperties": false } }, "additionalProperties": false }""";
        var registry = new ConnectorRegistry();
        registry.AddSource("thing", new StubConnector { ConnectionConfigSchema = nestedSchema });

        var source = new ConnectionDef("t", "thing",
            new Dictionary<string, object?>
            {
                ["tls"] = new Dictionary<string, object?> { ["verfiy"] = true },
            },
            [], "connections.yml");

        var error = Assert.Single(await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default));

        Assert.Equal("connection 't': /tls: unknown option 'verfiy'", error.Message);
        Assert.Equal("remove or rename it -- accepted options: verify", error.Hint);
    }

    [Fact]
    public async Task Wrong_type_dataset_option_is_PZ0301()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("localfiles", new StubConnector { DatasetConfigSchema = LocalFilesDatasetSchema });

        var source = new ConnectionDef("crm", "localfiles", new Dictionary<string, object?>(),
            [new DatasetDef("customers", new Dictionary<string, object?> { ["path"] = "c.csv" },
                new Dictionary<string, string> { ["id"] = "notatype" })],
            "connections.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default);

        var error = Assert.Single(errors);
        Assert.Equal(PzErrorCode.ConnectorConfigInvalid, error.Code);
        Assert.Contains("id", error.Message, StringComparison.Ordinal);
        Assert.Equal("connections.yml", error.File);
    }

    // YamlMapper only types a plain, lowercase true/false (or a plain integer/decimal); "True", "yes",
    // "null", and "~" all stay strings by design (see CHANGELOG's "Quoted YAML scalars are strings").
    // A boolean/integer option that receives one of them must not get the JSON Schema library's raw
    // "Value is \"string\" but should be \"boolean\"" -- it must name the rule.
    [Theory]
    [InlineData("True", "write true/false in lower case, unquoted")]
    [InlineData("yes", "write true/false in lower case, unquoted")]
    [InlineData("null", "leave the option out instead of writing null")]
    [InlineData("~", "leave the option out instead of writing null")]
    public async Task A_yaml_boolean_lookalike_string_names_the_typing_rule(string literal, string hint)
    {
        const string schema =
            """{ "type": "object", "properties": { "verify": { "type": "boolean" } }, "additionalProperties": false }""";
        var registry = new ConnectorRegistry();
        registry.AddSource("thing", new StubConnector { ConnectionConfigSchema = schema });

        var source = new ConnectionDef("t", "thing",
            new Dictionary<string, object?> { ["verify"] = literal }, [], "connections.yml");

        var error = Assert.Single(await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default));

        Assert.Equal(PzErrorCode.ConnectorConfigInvalid, error.Code);
        Assert.Contains($"'{literal}' was read as text, not a boolean or number here", error.Message,
            StringComparison.Ordinal);
        Assert.Equal(hint, error.Hint);
    }

    // The mirror case: an unquoted all-digit password, or one that reached the file through a bare
    // ${VAR}, is a number by YAML's rules. The error must not leak the value (it is as likely a secret
    // as anything) and must say how to keep it text.
    [Fact]
    public async Task A_number_where_text_is_expected_says_to_quote_it_and_does_not_echo_the_value()
    {
        const string schema =
            """{ "type": "object", "properties": { "password": { "type": "string" } }, "additionalProperties": false }""";
        var registry = new ConnectorRegistry();
        registry.AddSource("thing", new StubConnector { ConnectionConfigSchema = schema });

        var source = new ConnectionDef("t", "thing",
            new Dictionary<string, object?> { ["password"] = 12345678L }, [], "connections.yml");

        var error = Assert.Single(await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default));

        Assert.Equal(PzErrorCode.ConnectorConfigInvalid, error.Code);
        Assert.Contains("password", error.Message, StringComparison.Ordinal);
        Assert.Contains("read as a number", error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("12345678", error.Message, StringComparison.Ordinal);
        Assert.Contains("\"${VAR}\"", error.Hint, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_yaml_boolean_lookalike_string_against_an_integer_option_also_names_the_rule()
    {
        const string schema =
            """{ "type": "object", "properties": { "port": { "type": "integer" } }, "additionalProperties": false }""";
        var registry = new ConnectorRegistry();
        registry.AddSource("thing", new StubConnector { ConnectionConfigSchema = schema });

        var source = new ConnectionDef("t", "thing",
            new Dictionary<string, object?> { ["port"] = "null" }, [], "connections.yml");

        var error = Assert.Single(await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default));

        Assert.Equal(PzErrorCode.ConnectorConfigInvalid, error.Code);
        Assert.Equal("leave the option out instead of writing null", error.Hint);
    }

    // A string that merely LOOKS like a number (e.g. an intentionally quoted "5432") is a different
    // condition -- this rule fires only for the boolean/null lookalikes, not for every string that
    // fails a boolean/integer schema check.
    [Fact]
    public async Task An_unrelated_string_against_an_integer_option_keeps_the_generic_message()
    {
        const string schema =
            """{ "type": "object", "properties": { "port": { "type": "integer" } }, "additionalProperties": false }""";
        var registry = new ConnectorRegistry();
        registry.AddSource("thing", new StubConnector { ConnectionConfigSchema = schema });

        var source = new ConnectionDef("t", "thing",
            new Dictionary<string, object?> { ["port"] = "5432" }, [], "connections.yml");

        var error = Assert.Single(await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default));

        Assert.DoesNotContain("write true/false", error.Hint, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Missing_required_key_is_PZ0301()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("postgres", new StubConnector { ConnectionConfigSchema = PostgresConnectionSchema });

        var source = new ConnectionDef("db", "postgres",
            new Dictionary<string, object?> { ["database"] = "mydb" }, [], "sources/db.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default);

        var error = Assert.Single(errors);
        Assert.Equal(PzErrorCode.ConnectorConfigInvalid, error.Code);
        Assert.Contains("host", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ValidateAsync_cross_field_errors_are_aggregated()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("flaky", new StubConnector
        {
            ValidateFunc = _ => ValidationResult.Failed("cross field problem detected"),
        });
        registry.AddSource("localfiles", new StubConnector { ConnectionConfigSchema = LocalFilesConnectionSchema });

        var flakySource = new ConnectionDef("a", "flaky", new Dictionary<string, object?>(), [], "sources/a.yml");
        var badSchemaSource = new ConnectionDef("b", "localfiles",
            new Dictionary<string, object?> { ["bogus"] = "nope" }, [], "sources/b.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(
            Project([flakySource, badSchemaSource]), registry, default);

        Assert.Equal(2, errors.Count);
        Assert.All(errors, e => Assert.Equal(PzErrorCode.ConnectorConfigInvalid, e.Code));
        Assert.Contains(errors, e => e.Message.Contains("cross field problem detected", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Message.Contains("bogus", StringComparison.Ordinal));
    }

    // An s3-shaped connection missing BOTH required credential keys raises three raw errors -- one
    // combined "required" schema violation plus two per-key cross-field ValidateAsync messages. The
    // dedup must collapse this to exactly two lines, one per key (see ConnectorConfigValidator's
    // flaggedKeys/ReferencesFlaggedKey).
    private const string S3ConnectionSchema =
        """{ "type": "object", "required": ["access_key","secret_key"], "properties": { "access_key": { "type": "string" }, "secret_key": { "type": "string" } }, "additionalProperties": false }""";

    [Fact]
    public async Task Missing_both_credential_keys_is_exactly_two_errors_not_three()
    {
        var registry = new ConnectorRegistry();
        registry.AddSink("s3", new StubConnector
        {
            ConnectionConfigSchema = S3ConnectionSchema,
            ValidateFunc = config =>
            {
                var errors = new List<string>();
                if (string.IsNullOrEmpty(config.GetString("access_key")))
                {
                    errors.Add("s3 connection requires 'access_key'");
                }

                if (string.IsNullOrEmpty(config.GetString("secret_key")))
                {
                    errors.Add("s3 connection requires 'secret_key'");
                }

                return errors.Count == 0 ? ValidationResult.Success : ValidationResult.Failed([.. errors]);
            },
        });

        var sink = new ConnectionDef("store", "s3", new Dictionary<string, object?>(), [], "sinks/store.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project(sinks: [sink]), registry, default);

        Assert.Equal(2, errors.Count);
        Assert.Contains(errors, e => e.Message.Contains("access_key", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Message.Contains("secret_key", StringComparison.Ordinal));
    }

    // The dedup must NOT suppress a cross-field error that merely mentions an
    // already-flagged key's name for a DIFFERENT reason -- only a cross-field message using our
    // connectors' own "requires '<key>'" phrasing about a key the schema flagged as MISSING is a genuine
    // duplicate. A message about the same key but a different problem (e.g. malformed value) must survive.
    [Fact]
    public async Task Distinct_cross_field_error_about_flagged_key_survives_dedup()
    {
        var registry = new ConnectorRegistry();
        registry.AddSink("s3", new StubConnector
        {
            ConnectionConfigSchema = S3ConnectionSchema,
            ValidateFunc = _ => ValidationResult.Failed(
                "s3 connection: value for 'access_key' looks malformed (expected base64)"),
        });

        // Only secret_key is supplied -- the schema flags 'access_key' as missing/required. The
        // connector's cross-field message ALSO names 'access_key', but for an unrelated reason (a value
        // shape complaint, not "requires") and never uses the "requires '<key>'" phrasing -- it must not
        // be suppressed by the missing-key dedup.
        var sink = new ConnectionDef("store", "s3",
            new Dictionary<string, object?> { ["secret_key"] = "shh" }, [], "sinks/store.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project(sinks: [sink]), registry, default);

        Assert.Equal(2, errors.Count);
        Assert.Contains(errors, e => e.Message.Contains("'access_key' is required", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Message.Contains("looks malformed", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Root_level_schema_violation_message_has_no_double_colon()
    {
        var registry = new ConnectorRegistry();
        registry.AddSink("s3", new StubConnector { ConnectionConfigSchema = S3ConnectionSchema });

        var sink = new ConnectionDef("store", "s3", new Dictionary<string, object?>(), [], "sinks/store.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project(sinks: [sink]), registry, default);

        Assert.NotEmpty(errors);
        Assert.All(errors, e => Assert.DoesNotContain(": :", e.Message, StringComparison.Ordinal));
        // The kind IS "connection", so the block label is dropped for the connection block itself
        // rather than stuttering "connection 'store' connection:".
        Assert.Contains(errors, e => e.Message.Contains("connection 'store': 'access_key' is required",
            StringComparison.Ordinal));
    }

    /// <summary>An unknown connector name must be reported here rather than skipped: nothing upstream
    /// checks a connection's connector name against the registry, so a silent skip would let a typo'd
    /// `connector:` sail through `pz validate` and fail only at run time.</summary>
    [Fact]
    public async Task Unknown_connector_name_is_PZ0305_not_silently_skipped()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("localfiles", new StubConnector());

        var source = new ConnectionDef("weird", "doesnotexist",
            new Dictionary<string, object?> { ["whatever"] = "junk" }, [], "connections.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default);

        var error = Assert.Single(errors);
        Assert.Equal(PzErrorCode.ConnectorNotInstalled, error.Code);
        Assert.Contains("doesnotexist", error.Message, StringComparison.Ordinal);
        Assert.Contains("weird", error.Message, StringComparison.Ordinal);
        Assert.Contains("localfiles", error.Hint!, StringComparison.Ordinal);
    }

    /// <summary>pz owns connector/entities/max_concurrency/
    /// rate_limit/retry/allow_unsigned_extensions at connection level, so a connector declaring one
    /// could never receive it.</summary>
    [Theory]
    [InlineData("retry")]
    [InlineData("entities")]
    [InlineData("max_concurrency")]
    [InlineData("rate_limit")]
    [InlineData("connector")]
    [InlineData("allow_unsigned_extensions")]
    public async Task A_connector_declaring_a_reserved_property_is_PZ0345(string reserved)
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("greedy", new StubConnector
        {
            ConnectionConfigSchema =
                $$"""{ "type": "object", "properties": { "host": { "type": "string" }, "{{reserved}}": { "type": "string" } } }""",
        });
        var connection = new ConnectionDef("db", "greedy",
            new Dictionary<string, object?> { ["host"] = "h" }, [], "connections.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project([connection]), registry, default);

        var error = Assert.Single(errors, e => e.Code == PzErrorCode.ReservedConnectionKey);
        Assert.Contains("greedy", error.Message, StringComparison.Ordinal);
        Assert.Contains($"'{reserved}'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task No_first_party_connector_claims_a_reserved_property()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("plain", new StubConnector { ConnectionConfigSchema = PostgresConnectionSchema });
        var connection = new ConnectionDef("db", "plain",
            new Dictionary<string, object?> { ["host"] = "h", ["database"] = "d" }, [], "connections.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project([connection]), registry, default);

        Assert.DoesNotContain(errors, e => e.Code == PzErrorCode.ReservedConnectionKey);
    }

    // DuckDB accepts `set motherduck_token` only before the first attach in a session, so a RUN that
    // touches two motherduck connections with different tokens fails (PZ0311). The project is still
    // legitimate when each run selects only one of them, so validate says it without refusing: a warning
    // under the same code, naming both connections and never either token.
    private const string MotherDuckConnectionSchema =
        """{ "type": "object", "properties": { "database": { "type": "string" }, "token": { "type": "string" } } }""";

    [Fact]
    public async Task Two_motherduck_connections_with_different_tokens_draw_a_PZ0311_warning_at_validate_time()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("motherduck", new StubConnector { ConnectionConfigSchema = MotherDuckConnectionSchema });
        registry.AddSink("motherduck", new StubConnector { ConnectionConfigSchema = MotherDuckConnectionSchema });

        var first = new ConnectionDef("md1", "motherduck",
            new Dictionary<string, object?> { ["database"] = "d1", ["token"] = "secret-token-a" }, [], "connections.yml");
        var second = new ConnectionDef("md2", "motherduck",
            new Dictionary<string, object?> { ["database"] = "d2", ["token"] = "secret-token-b" }, [], "connections.yml");

        var warnings = new List<PzWarning>();
        var errors = await ConnectorConfigValidator.ValidateAsync(
            Project([first], [second]), registry, default, warnings);

        Assert.Empty(errors);
        var warning = Assert.Single(warnings);
        Assert.Equal(PzErrorCode.NativeSetupFailed, warning.Code);
        Assert.Contains("md1", warning.Message, StringComparison.Ordinal);
        Assert.Contains("md2", warning.Message, StringComparison.Ordinal);
        Assert.Contains("separate runs", warning.Hint, StringComparison.Ordinal);
        Assert.DoesNotContain("secret-token-a", warning.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("secret-token-b", warning.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Two_motherduck_connections_with_the_same_token_are_valid()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("motherduck", new StubConnector { ConnectionConfigSchema = MotherDuckConnectionSchema });
        registry.AddSink("motherduck", new StubConnector { ConnectionConfigSchema = MotherDuckConnectionSchema });

        var first = new ConnectionDef("md1", "motherduck",
            new Dictionary<string, object?> { ["database"] = "d1", ["token"] = "same-token" }, [], "connections.yml");
        var second = new ConnectionDef("md2", "motherduck",
            new Dictionary<string, object?> { ["database"] = "d2", ["token"] = "same-token" }, [], "connections.yml");

        var warnings = new List<PzWarning>();
        var errors = await ConnectorConfigValidator.ValidateAsync(Project([first], [second]), registry, default, warnings);

        Assert.DoesNotContain(errors, e => e.Code == PzErrorCode.NativeSetupFailed);
        Assert.Empty(warnings);
    }

    // A connection missing 'token' entirely is the connector's own ValidateAsync's problem ("requires
    // 'token'") -- the cross-connection comparison must not also throw or double-report on it.
    [Fact]
    public async Task A_motherduck_connection_missing_a_token_does_not_break_the_mismatch_check()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("motherduck", new StubConnector { ConnectionConfigSchema = MotherDuckConnectionSchema });
        registry.AddSink("motherduck", new StubConnector { ConnectionConfigSchema = MotherDuckConnectionSchema });

        var noToken = new ConnectionDef("md1", "motherduck",
            new Dictionary<string, object?> { ["database"] = "d1" }, [], "connections.yml");
        var withToken = new ConnectionDef("md2", "motherduck",
            new Dictionary<string, object?> { ["database"] = "d2", ["token"] = "tok" }, [], "connections.yml");

        var warnings = new List<PzWarning>();
        var errors = await ConnectorConfigValidator.ValidateAsync(Project([noToken], [withToken]), registry, default, warnings);

        Assert.DoesNotContain(errors, e => e.Code == PzErrorCode.NativeSetupFailed);
        Assert.Empty(warnings);
    }
}
