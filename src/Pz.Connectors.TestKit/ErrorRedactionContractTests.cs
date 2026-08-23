using Xunit;

namespace Pz.Connectors.TestKit;

/// <summary>Acceptance contract for a connector that wraps a third-party client's error text.
///
/// <para>A <c>PzConnectorException</c>'s message is written VERBATIM into <c>run_results.json</c>, onto
/// the NDJSON event stream, and into a <c>retry_scheduled</c> event's reason. The engine does not redact
/// it, deliberately: it cannot parse what a connector wrapped, and only the connector knows which
/// substrings of its own message are secrets. The trust is also total — nothing downstream catches a
/// credential that reaches that message. A connector's own redactor is the only thing standing between
/// a wrapped client error and a credential in a durable artifact that routinely leaves the machine.</para>
///
/// <para>That redactor is easy to get wrong in a way its own tests cannot see: a local emulator answers
/// in a different shape than the real service. Real object-store and cloud-storage rejections carry the
/// access key id and the signing payload inside XML ELEMENTS, not as <c>name=value</c> pairs, and an
/// emulator's rejection body may carry neither — so a suite built against the emulator passes with the
/// redactor removed entirely. This class feeds the shapes the real services answer in.</para>
///
/// <para>Subclass it once per connector, alongside the source/sink acceptance suites.</para></summary>
public abstract class ErrorRedactionContractTests
{
    /// <summary>The connector's own redaction of third-party error text — whatever it runs before
    /// wrapping that text in a <c>PzConnectorException</c>.</summary>
    protected abstract string RedactErrorText(string thirdPartyMessage);

    /// <summary>Invoked first by every fact below, matching the source/sink suites' hook.</summary>
    protected virtual void GateFact()
    {
    }

    /// <summary>The synthetic secret every payload below embeds. Not a real credential, and none should
    /// ever be added here — the point is that the redactor removes THIS string.</summary>
    private const string Secret = "pz-testkit-secret-value";

    public static TheoryData<string, string, string> CredentialShapes() => new()
    {
        {
            "s3 signature rejection",
            $"""
             <?xml version="1.0" encoding="UTF-8"?>
             <Error><Code>SignatureDoesNotMatch</Code>
             <Message>The request signature we calculated does not match the signature you provided.</Message>
             <AWSAccessKeyId>{Secret}</AWSAccessKeyId>
             <StringToSign>{Secret}</StringToSign></Error>
             """,
            "SignatureDoesNotMatch"
        },
        {
            "azure authorization failure",
            $"""
             <?xml version="1.0" encoding="utf-8"?>
             <Error><Code>AuthorizationFailure</Code>
             <Message>This request is not authorized to perform this operation. Signature={Secret}</Message>
             </Error>
             """,
            "AuthorizationFailure"
        },
        {
            "connection string",
            $"Login failed for 'Server=db.example;User Id=app;Password={Secret};'",
            "Login failed"
        },
        {
            "name=value pair",
            $"request rejected: access_key={Secret}",
            "request rejected"
        },
    };

    [SkippableTheory]
    [MemberData(nameof(CredentialShapes))]
    public void Redaction_removes_the_credential_and_keeps_the_diagnosis(
        string shape, string payload, string diagnosis)
    {
        GateFact();

        // `shape` names the case in the test output and is not otherwise asserted on.
        _ = shape;

        var redacted = RedactErrorText(payload);

        Assert.DoesNotContain(Secret, redacted, StringComparison.Ordinal);

        // A redactor that answers "" or a fixed string satisfies the check above while destroying the
        // only thing the message is for. The diagnosis has to survive, or the artifact is useless.
        Assert.Contains(diagnosis, redacted, StringComparison.Ordinal);
    }
}
