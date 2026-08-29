namespace Pz.Connector.Sftp.Tests;

/// <summary>Runs the TestKit's error-redaction acceptance contract against the real
/// <see cref="SftpErrors.Redact"/> -- pure string transformation, so unlike the source/sink acceptance
/// suites this needs no docker container and no <see cref="GateFact"/> override.</summary>
public sealed class SftpRedactionAcceptance : Pz.Connectors.TestKit.ErrorRedactionContractTests
{
    protected override string RedactErrorText(string thirdPartyMessage) => SftpErrors.Redact(thirdPartyMessage);
}
