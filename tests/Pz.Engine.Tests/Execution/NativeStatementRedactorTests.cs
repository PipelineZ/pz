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
}
