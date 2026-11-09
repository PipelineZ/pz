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

    // A `5432m` kwarg literal evaluates to decimal and one too big for long to BigInteger; the compiler
    // hashes both into node identity, so the schema converter must take them too.
    public static TheoryData<object> WideNumericKwargValues => new() { 5432m, new System.Numerics.BigInteger(5432) };

    [Theory]
    [MemberData(nameof(WideNumericKwargValues))]
    public async Task A_decimal_or_big_integer_option_written_at_a_call_site_validates(object port)
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("postgres", new StubConnector { ConnectionConfigSchema = PostgresConnectionSchema });

        var source = new ConnectionDef("crm", "postgres",
            new Dictionary<string, object?> { ["host"] = "db", ["database"] = "crm", ["port"] = port },
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

    private static ConnectionDef WithPointers(string connector) =>
        new("crm", connector, new Dictionary<string, object?>(),
            [new DatasetDef("customers", new Dictionary<string, object?>
                {
                    ["path"] = "c.csv",
                    ["column_pointers"] = new Dictionary<string, object?> { ["status"] = "/a/b", ["email"] = "/c" },
                },
                new Dictionary<string, string> { ["status"] = "varchar", ["email"] = "varchar" })],
            "connections.yml");

    /// <summary>A column `pointer` reaches the connector as `column_pointers`; a connector whose dataset
    /// schema does not list it cannot honour it, and the refusal names the columns, not the internal key.</summary>
    [Fact]
    public async Task Column_pointers_on_a_connector_that_does_not_list_them_name_the_columns()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("localfiles", new StubConnector { DatasetConfigSchema = LocalFilesDatasetSchema });

        var error = Assert.Single(await ConnectorConfigValidator.ValidateAsync(Project([WithPointers("localfiles")]),
            registry, default));

        Assert.Equal(PzErrorCode.ConnectorConfigInvalid, error.Code);
        Assert.Contains("dataset 'customers'", error.Message, StringComparison.Ordinal);
        Assert.Contains("'email', 'status'", error.Message, StringComparison.Ordinal);
        Assert.Contains("connector 'localfiles'", error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("unknown option", error.Message, StringComparison.Ordinal);
        Assert.Contains("pointer", error.Hint, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Column_pointers_pass_on_a_connector_that_lists_them()
    {
        var schema = LocalFilesDatasetSchema.Replace("\"path\": { \"type\": \"string\" }",
            "\"path\": { \"type\": \"string\" }, \"column_pointers\": { \"type\": \"object\" }");
        var registry = new ConnectorRegistry();
        registry.AddSource("http", new StubConnector { DatasetConfigSchema = schema });

        Assert.Empty(await ConnectorConfigValidator.ValidateAsync(Project([WithPointers("http")]), registry, default));
    }

    /// <summary>An empty schema (an external connector not spawned yet, or one that sends none) cannot
    /// show that the connector reads `column_pointers`, so a pointer is refused rather than let through.</summary>
    [Fact]
    public async Task Column_pointers_on_a_connector_with_an_empty_schema_are_refused()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("acme", new StubConnector { DatasetConfigSchema = "" });

        var error = Assert.Single(await ConnectorConfigValidator.ValidateAsync(Project([WithPointers("acme")]),
            registry, default));

        Assert.Equal(PzErrorCode.ConnectorConfigInvalid, error.Code);
        Assert.Contains("connector 'acme'", error.Message, StringComparison.Ordinal);
    }

    /// <summary>An external connector's schema arrives with its first spawn, which the cross-field check
    /// triggers; the pointer check runs after it, so its outcome cannot depend on connection order.</summary>
    [Fact]
    public async Task Column_pointers_are_checked_against_the_schema_known_after_the_cross_field_check()
    {
        var listing = LocalFilesDatasetSchema.Replace("\"path\": { \"type\": \"string\" }",
            "\"path\": { \"type\": \"string\" }, \"column_pointers\": { \"type\": \"object\" }");
        var stub = new SpawningStub(listing);
        var registry = new ConnectorRegistry();
        registry.AddSource("acme", stub);

        Assert.Empty(await ConnectorConfigValidator.ValidateAsync(Project([WithPointers("acme")]), registry, default));
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

    // A clash between a connection and one dataset is invisible to the connection's ValidateAsync and to the
    // dataset schema; a connector offering IDatasetConfigValidator reports it per dataset, every one at once.
    [Fact]
    public async Task Dataset_cross_field_errors_are_reported_per_dataset_and_aggregated()
    {
        var registry = new ConnectorRegistry();
        var connector = new StubDatasetValidatorConnector
        {
            ValidateDatasetFunc = (connection, dataset) => dataset.Dataset == "ok"
                ? ValidationResult.Success
                : ValidationResult.Failed($"clash one with {connection.GetString("host")}", "clash two"),
        };
        registry.AddSource("stub-ds", connector);

        var source = new ConnectionDef("api", "stub-ds", new Dictionary<string, object?> { ["host"] = "h" },
            [
                new DatasetDef("ok", new Dictionary<string, object?> { ["path"] = "/a" }, null),
                new DatasetDef("orders", new Dictionary<string, object?> { ["path"] = "/o" },
                    new Dictionary<string, string> { ["id"] = "bigint" }),
            ], "connections.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default);

        Assert.Equal(["connection 'api' dataset 'orders': clash one with h", "connection 'api' dataset 'orders': clash two"],
            errors.Select(e => e.Message));
        Assert.All(errors, e => Assert.Equal(PzErrorCode.ConnectorConfigInvalid, e.Code));
        Assert.All(errors, e => Assert.Equal("connections.yml", e.File));
        var orders = Assert.Single(connector.Seen, d => d.Dataset == "orders");
        Assert.Equal("api", orders.Source);
        Assert.Equal("/o", orders.Options["path"]);
        Assert.NotNull(orders.Options["columns"]);
    }

    // Validation reports every problem at once, so the connector's dataset check sees a schema-rejected dataset
    // too; a finding naming an option the schema already flagged is the same problem said twice and is dropped. A
    // connector that refuses by throwing is reported like one that returns its findings.
    [Fact]
    public async Task Dataset_check_sees_schema_rejected_datasets_without_repeating_schema_findings()
    {
        var registry = new ConnectorRegistry();
        var connector = new StubDatasetValidatorConnector
        {
            DatasetConfigSchema = """{"type":"object","properties":{"path":{"type":"string"},"mode":{"type":"string"}},"additionalProperties":false}""",
            ValidateDatasetFunc = (_, dataset) => dataset.Dataset == "orders"
                ? throw new PzConnectorException("'/o' clashes", isTransient: false)
                : ValidationResult.Failed("option 'pathh' is not known", "pathh.x cannot be set", "'mode' needs 'path'"),
        };
        registry.AddSource("stub-ds", connector);

        var source = new ConnectionDef("api", "stub-ds", new Dictionary<string, object?>(),
            [
                new DatasetDef("typo", new Dictionary<string, object?> { ["mode"] = "x", ["pathh"] = "/b" }, null),
                new DatasetDef("orders", new Dictionary<string, object?> { ["path"] = "/o" }, null),
            ], "connections.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default);

        Assert.Equal(
            [
                "connection 'api' dataset 'typo': unknown option 'pathh'",
                "connection 'api' dataset 'typo': 'mode' needs 'path'",
                "connection 'api' dataset 'orders': '/o' clashes",
            ],
            errors.Select(e => e.Message));
        Assert.All(errors, e => Assert.Equal(PzErrorCode.ConnectorConfigInvalid, e.Code));
        Assert.Equal(["typo", "orders"], connector.Seen.Select(d => d.Dataset));
    }

    // Tier 3 runs the plan's own offline probes on a builtin connection, so `pz validate` refuses what `pz plan`
    // would: a connection the connector refuses to open (once, however many datasets use it), and a dataset whose
    // read shape it refuses. A dataset that already has a finding is not probed again.
    [Fact]
    public async Task A_connection_refused_at_open_is_reported_once_at_validate()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("stub-ds", new StubDatasetValidatorConnector
        {
            OpenThrows = () => new PzConnectorException("gcs sources require 'auth: hmac'", isTransient: false,
                code: PzConnectorErrorCodes.Config),
        });
        var source = new ConnectionDef("lake", "stub-ds", new Dictionary<string, object?>(),
            [
                new DatasetDef("a", new Dictionary<string, object?>(), null),
                new DatasetDef("b", new Dictionary<string, object?>(), null),
            ], "connections.yml");

        var error = Assert.Single(await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default));

        Assert.Equal(PzErrorCode.ConnectorConfigInvalid, error.Code);
        Assert.Equal("connection 'lake': gcs sources require 'auth: hmac'", error.Message);
        Assert.Equal("fix the options the connector names", error.Hint);
    }

    [Fact]
    public async Task A_dataset_refused_by_the_read_shape_probe_is_reported_at_validate()
    {
        var registry = new ConnectorRegistry();
        var connector = new StubDatasetValidatorConnector
        {
            ValidateDatasetFunc = (_, d) => d.Dataset == "flagged" ? ValidationResult.Failed("'x' is wrong") : ValidationResult.Success,
            ShapeThrows = spec => new PzConnectorException($"'{spec.Dataset}' refused", isTransient: false,
                code: PzConnectorErrorCodes.Config),
        };
        registry.AddSource("stub-ds", connector);
        var source = new ConnectionDef("api", "stub-ds", new Dictionary<string, object?>(),
            [
                new DatasetDef("flagged", new Dictionary<string, object?>(), null),
                new DatasetDef("probed", new Dictionary<string, object?>(), null),
            ], "connections.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default);

        Assert.Equal(
            ["connection 'api' dataset 'flagged': 'x' is wrong", "connection 'api' dataset 'probed': 'probed' refused"],
            errors.Select(e => e.Message));
        Assert.Equal(["probed"], connector.ShapeProbed);
    }

    [Fact]
    public async Task A_hosted_connector_or_a_connection_with_findings_is_not_probed()
    {
        var hosted = new StubDatasetValidatorConnector { OpenThrows = () => new InvalidOperationException("opened") };
        var builtin = new StubDatasetValidatorConnector
        {
            ConnectionConfigSchema = """{"type":"object","required":["host"]}""",
            OpenThrows = () => new InvalidOperationException("opened"),
        };
        var registry = new ConnectorRegistry();
        registry.AddSource("remote", hosted, hosted: true);
        registry.AddSource("local", builtin);
        var dataset = new DatasetDef("d", new Dictionary<string, object?>(), null);

        var errors = await ConnectorConfigValidator.ValidateAsync(Project(
            [
                new ConnectionDef("r", "remote", new Dictionary<string, object?>(), [dataset], "connections.yml"),
                new ConnectionDef("l", "local", new Dictionary<string, object?>(), [dataset], "connections.yml"),
            ]), registry, default);

        Assert.Equal(["connection 'l': 'host' is required"], errors.Select(e => e.Message));
    }

    // A native-path refusal coded config is reported at validate as the plan reports it (PZ0353); an uncoded one
    // depends on what is on disk now and stays the plan's; force_universal skips the probe, as in the plan.
    [Theory]
    [InlineData(true, false, true)]
    [InlineData(false, false, false)]
    [InlineData(true, true, false)]
    public async Task A_native_scan_refusal_is_reported_at_validate_only_when_coded_config(bool coded, bool forceUniversal,
        bool reported)
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("stub-ds", new StubDatasetValidatorConnector
        {
            NativeScanThrows = spec => new PzConnectorException($"dataset '{spec.Dataset}': cannot be scanned",
                isTransient: false, code: coded ? PzConnectorErrorCodes.Config : null),
        });
        var source = new ConnectionDef("db", "stub-ds", new Dictionary<string, object?>(),
            [new DatasetDef("events", new Dictionary<string, object?>(), null)], "connections.yml");
        var project = Project([source]) with { Engine = new EngineConfig { ForceUniversal = forceUniversal } };

        var errors = await ConnectorConfigValidator.ValidateAsync(project, registry, default);

        if (reported)
        {
            var error = Assert.Single(errors);
            Assert.Equal(PzErrorCode.NativePathContractMismatch, error.Code);
            Assert.Equal("dataset 'events': cannot be scanned", error.Message);
        }
        else
        {
            Assert.Empty(errors);
        }
    }

    // The plan never probes the native path of a dataset that resumes from a sync token (feed or cdc): it takes
    // the arrow path. Validate skips the probe for those shapes too, so it is never stricter than the plan.
    [Theory]
    [InlineData("feed")]
    [InlineData("cdc")]
    public async Task A_token_resumed_dataset_is_not_native_probed_at_validate(string shape)
    {
        var registry = new ConnectorRegistry();
        var connector = new StubDatasetValidatorConnector
        {
            Shape = shape == "feed" ? NaturalReadShape.Feed : NaturalReadShape.Full,
            NativeScanThrows = _ => new PzConnectorException("refused", isTransient: false, code: PzConnectorErrorCodes.Config),
        };
        registry.AddSource("stub-ds", connector);
        var dataset = new DatasetDef("events", new Dictionary<string, object?>(), null,
            shape == "cdc" ? new SyncModeDef(SyncMode.Cdc, null) : null);
        var source = new ConnectionDef("db", "stub-ds", new Dictionary<string, object?>(), [dataset], "connections.yml");

        Assert.Empty(await ConnectorConfigValidator.ValidateAsync(Project([source]), registry, default));
    }

    // An output on a connection whose connector also reads is schema-checked too, not only a write-only one's.
    [Fact]
    public async Task Outputs_of_a_read_write_connection_are_schema_validated()
    {
        var registry = new ConnectorRegistry();
        var connector = new StubOutputSchemaConnector
        {
            OutputConfigSchema = """{"type":"object","properties":{"tablock":{"type":"boolean"}},"additionalProperties":false}""",
        };
        registry.AddSource("rw", connector);
        registry.AddSink("rw", connector);
        var connection = new ConnectionDef("db", "rw", new Dictionary<string, object?>(), [], "connections.yml")
        {
            Outputs = [new OutputDef("orders", "", "append", "fail_on_change",
                new Dictionary<string, object?> { ["tablok"] = true }, [])],
        };

        var error = Assert.Single(await ConnectorConfigValidator.ValidateAsync(Project([connection]), registry, default));

        Assert.Equal("connection 'db' output 'orders': unknown option 'tablok'", error.Message);
    }

    // A sink's write: options were never schema-validated at all before IOutputConfigSchema: a typo'd
    // option reached the connector unchecked and was silently ignored. These pin both halves of the
    // fix -- a connector that offers the capability gets its outputs checked, one that does not is
    // untouched -- using sink.Outputs exactly as DagCompiler hands it to ValidateAsync (the EFFECTIVE,
    // already-merged output list; see ValidateCommand's `project with { Connections = dag.Connections }`).
    [Fact]
    public async Task Valid_output_options_against_an_offered_schema_produce_no_errors()
    {
        var registry = new ConnectorRegistry();
        registry.AddSink("stub-output", new StubOutputSchemaConnector
        {
            OutputConfigSchema = """{"type":"object","properties":{"tablock":{"type":"boolean"}},"additionalProperties":false}""",
        });

        var sink = new ConnectionDef("out", "stub-output", new Dictionary<string, object?>(), [], "sinks/out.yml")
        {
            Outputs = [new OutputDef("orders", "", "append", "fail_on_change",
                new Dictionary<string, object?> { ["tablock"] = true })],
        };

        var errors = await ConnectorConfigValidator.ValidateAsync(Project(sinks: [sink]), registry, default);

        Assert.Empty(errors);
    }

    [Fact]
    public async Task Unknown_output_option_is_refused_and_names_the_output()
    {
        var registry = new ConnectorRegistry();
        registry.AddSink("stub-output", new StubOutputSchemaConnector
        {
            OutputConfigSchema = """{"type":"object","properties":{"tablock":{"type":"boolean"}},"additionalProperties":false}""",
        });

        var sink = new ConnectionDef("out", "stub-output", new Dictionary<string, object?>(), [], "sinks/out.yml")
        {
            Outputs = [new OutputDef("orders", "", "append", "fail_on_change",
                new Dictionary<string, object?> { ["tablok"] = true })],
        };

        var errors = await ConnectorConfigValidator.ValidateAsync(Project(sinks: [sink]), registry, default);

        var error = Assert.Single(errors);
        Assert.Equal(PzErrorCode.ConnectorConfigInvalid, error.Code);
        Assert.Equal("connection 'out' output 'orders': unknown option 'tablok'", error.Message);
        // "tablok" is one deletion away from "tablock" -- proves the near-miss hint fires for an
        // output-schema violation, not just the accepted-options list.
        Assert.Contains("did you mean 'tablock'", error.Hint, StringComparison.Ordinal);
    }

    // partition_by stays among an output's options because partitioning sinks read it, but whether a
    // connector can honour it is the planner's question, answered from its capabilities with a refusal
    // that says which capability is missing. A sink's schema calling it an unknown option would
    // replace that answer with a worse one.
    [Fact]
    public async Task Partition_by_is_left_to_the_planners_capability_gate()
    {
        var registry = new ConnectorRegistry();
        registry.AddSink("stub-output", new StubOutputSchemaConnector());

        var sink = new ConnectionDef("out", "stub-output", new Dictionary<string, object?>(), [], "sinks/out.yml")
        {
            Outputs = [new OutputDef("orders", "", "append", "fail_on_change",
                new Dictionary<string, object?> { ["partition_by"] = "order_date" })],
        };

        var errors = await ConnectorConfigValidator.ValidateAsync(Project(sinks: [sink]), registry, default);

        Assert.Empty(errors);
    }

    [Fact]
    public async Task A_sink_that_does_not_offer_IOutputConfigSchema_leaves_output_options_unchecked()
    {
        var registry = new ConnectorRegistry();
        registry.AddSink("stub", new StubConnector());

        var sink = new ConnectionDef("out", "stub", new Dictionary<string, object?>(), [], "sinks/out.yml")
        {
            Outputs = [new OutputDef("orders", "", "append", "fail_on_change",
                new Dictionary<string, object?> { ["whatever_typo"] = "x" })],
        };

        var errors = await ConnectorConfigValidator.ValidateAsync(Project(sinks: [sink]), registry, default);

        Assert.Empty(errors);
    }

    [Fact]
    public async Task Multiple_outputs_on_one_sink_are_each_validated_independently()
    {
        var registry = new ConnectorRegistry();
        registry.AddSink("stub-output", new StubOutputSchemaConnector
        {
            OutputConfigSchema = """{"type":"object","properties":{"tablock":{"type":"boolean"}},"additionalProperties":false}""",
        });

        var sink = new ConnectionDef("out", "stub-output", new Dictionary<string, object?>(), [], "sinks/out.yml")
        {
            Outputs =
            [
                new OutputDef("orders", "", "append", "fail_on_change", new Dictionary<string, object?> { ["tablock"] = true }),
                new OutputDef("customers", "", "append", "fail_on_change", new Dictionary<string, object?> { ["bogus"] = 1 }),
            ],
        };

        var errors = await ConnectorConfigValidator.ValidateAsync(Project(sinks: [sink]), registry, default);

        var error = Assert.Single(errors);
        Assert.Equal("connection 'out' output 'customers': unknown option 'bogus'", error.Message);
    }

    /// <summary>A connector whose dataset schema is empty until its ValidateAsync runs, like an external
    /// connector's process host before its first spawn.</summary>
    private sealed class SpawningStub(string schema) : ISourceConnector
    {
        private string _schema = "";

        public ConnectorInfo Info { get; } = new("acme", "0.1.0", ProtocolVersion.Major);

        public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;

        public string ConnectionConfigSchema => "";

        public string DatasetConfigSchema => _schema;

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
        {
            _schema = schema;
            return ValueTask.FromResult(ValidationResult.Success);
        }

        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            ValueTask.FromResult(new ConnectionCheck(true));

        public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(new NoopSource());
    }
}
