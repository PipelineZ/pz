using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary><see cref="MessageRedaction"/> is a thin named seam over
/// <see cref="NativeStatementRedactor.SanitizeEngineMessage"/> -- this test mirrors
/// <c>NativeStatementRedactorTests</c>' own cases to prove the seam delegates faithfully rather than
/// reimplementing (or drifting from) the sanitizer's conventions.</summary>
public sealed class MessageRedactionTests
{
    private const string CapturedParserErrorMessage =
        "Parser Error: syntax error at or near \"secret\"\n\n" +
        "LINE 1: create secret pz_x (type s3 secret 'SECRET_VALUE')\n" +
        "                                    ^";

    [Fact]
    public void MessageRedaction_strips_quoted_literals_and_sql_echo()
    {
        var redactedEcho = MessageRedaction.Redact(CapturedParserErrorMessage);
        Assert.Equal("Parser Error: syntax error at or near \"secret\"", redactedEcho);
        Assert.DoesNotContain("SECRET_VALUE", redactedEcho);
        Assert.DoesNotContain("LINE", redactedEcho);

        var redactedLiteral = MessageRedaction.Redact(
            "Binder Error: column 'ssn_1234567890' not found in FROM clause");
        Assert.Equal("Binder Error: column '***' not found in FROM clause", redactedLiteral);
        Assert.DoesNotContain("ssn_1234567890", redactedLiteral);
    }
}
