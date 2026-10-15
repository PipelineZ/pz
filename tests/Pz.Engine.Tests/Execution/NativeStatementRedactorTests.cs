using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

public sealed class NativeStatementRedactorTests
{
    [Fact]
    public void Describe_two_keywords()
    {
        Assert.Equal("CREATE SECRET …", NativeStatementRedactor.Describe("create secret pz_test (type s3)"));
    }

    [Fact]
    public void Describe_single_token()
    {
        Assert.Equal("VACUUM …", NativeStatementRedactor.Describe("vacuum"));
    }

    [Fact]
    public void Describe_create_secret_hides_body()
    {
        var described = NativeStatementRedactor.Describe(
            "create secret pz_test (type s3, secret 'SECRET_VALUE')");

        Assert.Equal("CREATE SECRET …", described);
        Assert.DoesNotContain("SECRET_VALUE", described);
    }

    /// <summary>Captured verbatim from a real DuckDB CLI (v1.x) run of
    /// <c>create secret pz_x (type s3 secret 'SECRET_VALUE')</c> — a malformed CREATE SECRET whose
    /// Parser Error echoes the whole offending statement (including the secret literal) in its
    /// "LINE 1: ..." context block. Reproduced with: `duckdb -c "create secret pz_x (type s3 secret
    /// 'SECRET_VALUE')"`.</summary>
    private const string CapturedParserErrorMessage =
        "Parser Error: syntax error at or near \"secret\"\n\n" +
        "LINE 1: create secret pz_x (type s3 secret 'SECRET_VALUE')\n" +
        "                                    ^";

    [Fact]
    public void SanitizeEngineMessage_drops_line_context_block()
    {
        var sanitized = NativeStatementRedactor.SanitizeEngineMessage(CapturedParserErrorMessage);

        Assert.Equal("Parser Error: syntax error at or near \"secret\"", sanitized);
        Assert.DoesNotContain("SECRET_VALUE", sanitized);
        Assert.DoesNotContain("LINE", sanitized);
    }

    [Fact]
    public void SanitizeEngineMessage_masks_single_quoted_literals_outside_line_block()
    {
        var sanitized = NativeStatementRedactor.SanitizeEngineMessage(
            "Binder Error: column 'ssn_1234567890' not found in FROM clause");

        Assert.Equal("Binder Error: column '***' not found in FROM clause", sanitized);
        Assert.DoesNotContain("ssn_1234567890", sanitized);
    }

    [Fact]
    public void SanitizeEngineMessage_passes_through_message_with_no_line_block_or_quotes()
    {
        var sanitized = NativeStatementRedactor.SanitizeEngineMessage("Invalid Input Error: Secret type not found");

        Assert.Equal("Invalid Input Error: Secret type not found", sanitized);
    }

    /// <summary>The azure extension's native-copy failure from a Linux host whose default transport could not
    /// find the CA bundle: the file URL and the nested transport error are what explain it.</summary>
    [Fact]
    public void SanitizeEngineMessage_keeps_the_file_url_and_the_nested_original_error()
    {
        var sanitized = NativeStatementRedactor.SanitizeEngineMessage(
            "IO Error: AzureStorageFileSystem could not open file: 'az://lake/raw/quakes/data_0.parquet', unknown error " +
            "occurred, this could mean the credentials used were wrong. Original error message: 'Problem with the SSL CA " +
            "cert (path? access rights?)'");

        Assert.Equal(
            "IO Error: AzureStorageFileSystem could not open file: 'az://lake/raw/quakes/data_0.parquet', unknown error " +
            "occurred, this could mean the credentials used were wrong. Original error message: 'Problem with the SSL CA " +
            "cert (path? access rights?)'",
            sanitized);
    }

    [Fact]
    public void SanitizeEngineMessage_masks_a_sas_query_string_and_userinfo_in_a_url_literal()
    {
        var sanitized = NativeStatementRedactor.SanitizeEngineMessage(
            "IO Error: could not open 'https://acct.blob.core.windows.net/lake/x.parquet?sv=2024&sig=SASSIG%3D' and " +
            "'postgresql://admin:PGPASS@db.internal:5432/app'");

        Assert.Equal(
            "IO Error: could not open 'https://acct.blob.core.windows.net/lake/x.parquet?***' and " +
            "'postgresql://***@db.internal:5432/app'",
            sanitized);
        Assert.DoesNotContain("SASSIG", sanitized);
        Assert.DoesNotContain("PGPASS", sanitized);
    }

    [Fact]
    public void SanitizeEngineMessage_scrubs_credentials_inside_a_nested_original_error()
    {
        var sanitized = NativeStatementRedactor.SanitizeEngineMessage(
            "IO Error: request failed. Original error message: 'GET https://acct.blob.core.windows.net/c/x?sig=SASSIG " +
            "rejected for AccountName=acct;AccountKey=AZKEY=='");

        Assert.Equal(
            "IO Error: request failed. Original error message: 'GET https://acct.blob.core.windows.net/c/x?*** " +
            "rejected for AccountName=acct;AccountKey=***'",
            sanitized);
    }

    [Fact]
    public void SanitizeEngineMessage_still_masks_a_secret_literal_that_is_not_a_url()
    {
        var sanitized = NativeStatementRedactor.SanitizeEngineMessage(
            "Invalid Input Error: could not parse connection string 'DefaultEndpointsProtocol=https;AccountName=a;AccountKey=AZKEY'");

        Assert.Equal("Invalid Input Error: could not parse connection string '***'", sanitized);
    }

    [Fact]
    public void SanitizeEngineMessage_masks_a_password_containing_a_slash_in_url_userinfo()
    {
        var sanitized = NativeStatementRedactor.SanitizeEngineMessage(
            "IO Error: could not connect to 'postgresql://admin:PG/PASS@db.internal/app'");

        Assert.Equal("IO Error: could not connect to 'postgresql://***@db.internal/app'", sanitized);
    }

    [Fact]
    public void SanitizeEngineMessage_scrubs_past_a_quote_inside_the_nested_original_error()
    {
        var sanitized = NativeStatementRedactor.SanitizeEngineMessage(
            "IO Error: failed. Original error message: 'secret 'INNERSECRET' rejected, Authorization: Bearer EYJTOKEN, " +
            "{\"password\": \"JSONPW\"}, client_secret: COLONPW'");

        Assert.DoesNotContain("INNERSECRET", sanitized);
        Assert.DoesNotContain("EYJTOKEN", sanitized);
        Assert.DoesNotContain("JSONPW", sanitized);
        Assert.DoesNotContain("COLONPW", sanitized);
        Assert.Contains("Original error message: 'secret '***' rejected", sanitized);
    }

    [Fact]
    public void SanitizeEngineMessage_masks_credential_pairs_in_a_kept_url_path()
    {
        var sanitized = NativeStatementRedactor.SanitizeEngineMessage(
            "IO Error: could not open 'https://acct.blob.core.windows.net/c;AccountKey=AZKEY/x.parquet'");

        Assert.DoesNotContain("AZKEY", sanitized);
    }
}
