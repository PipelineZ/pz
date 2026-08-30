using System.Net.Sockets;
using Renci.SshNet.Common;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

public class SftpErrorsTests
{
    [Fact]
    public void SshConnectionException_is_transient() =>
        Assert.True(SftpErrors.IsTransient(new SshConnectionException("dropped")));

    [Fact]
    public void SocketException_is_transient() =>
        Assert.True(SftpErrors.IsTransient(new SocketException()));

    [Fact]
    public void IOException_is_transient() =>
        Assert.True(SftpErrors.IsTransient(new IOException("disk full")));

    [Fact]
    public void SshOperationTimeoutException_is_transient() =>
        Assert.True(SftpErrors.IsTransient(new SshOperationTimeoutException("timed out")));

    [Fact]
    public void SshAuthenticationException_is_permanent() =>
        Assert.False(SftpErrors.IsTransient(new SshAuthenticationException("bad credentials")));

    [Fact]
    public void SftpPermissionDeniedException_is_permanent() =>
        Assert.False(SftpErrors.IsTransient(new SftpPermissionDeniedException("denied")));

    [Fact]
    public void SftpPathNotFoundException_is_permanent() =>
        Assert.False(SftpErrors.IsTransient(new SftpPathNotFoundException("missing")));

    [Fact]
    public void Inner_exception_classification_walks_the_chain() =>
        // SshException itself isn't classified either way, but its wrapped SocketException is.
        Assert.True(SftpErrors.IsTransient(new SshException("wrapped", new SocketException())));

    [Fact]
    public void Unrecognized_exception_is_permanent() =>
        Assert.False(SftpErrors.IsTransient(new InvalidOperationException("huh")));

    [Fact]
    public void Map_prefixes_context_carries_transience_and_wraps_the_original()
    {
        var original = new SshAuthenticationException("bad credentials");

        var mapped = SftpErrors.Map(original, "sftp host 'h': connect failed");

        Assert.False(mapped.IsTransient);
        Assert.Equal("sftp host 'h': connect failed: bad credentials", mapped.Message);
        Assert.Same(original, mapped.InnerException);
    }

    // Payloads copied verbatim (same literal secret and shapes) from CredentialShapes() in
    // src/Pz.Connectors.TestKit/ErrorRedactionContractTests.cs — that class documents why each
    // shape matters; SftpErrors.Redact must survive all four the same way.
    private const string Secret = "pz-testkit-secret-value";

    public static TheoryData<string, string> CredentialShapes() => new()
    {
        {
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
            $"""
             <?xml version="1.0" encoding="utf-8"?>
             <Error><Code>AuthorizationFailure</Code>
             <Message>This request is not authorized to perform this operation. Signature={Secret}</Message>
             </Error>
             """,
            "AuthorizationFailure"
        },
        {
            $"Login failed for 'Server=db.example;User Id=app;Password={Secret};'",
            "Login failed"
        },
        {
            $"request rejected: access_key={Secret}",
            "request rejected"
        },
    };

    [Theory]
    [MemberData(nameof(CredentialShapes))]
    public void Redact_removes_the_credential_and_keeps_the_diagnosis(string payload, string diagnosis)
    {
        var redacted = SftpErrors.Redact(payload);

        Assert.DoesNotContain(Secret, redacted, StringComparison.Ordinal);
        Assert.Contains(diagnosis, redacted, StringComparison.Ordinal);
    }
}
