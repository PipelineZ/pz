using Pz.Core.Validation;

namespace Pz.PackageManagement.Tests.Hosting;

public class HostErrorCodeTests
{
    [Fact]
    public void Host_error_literals_match_pz_error_codes()
    {
        Assert.Equal("PZ0304", PzErrorCode.ConnectorPackageMissing);
        Assert.Equal("PZ0305", PzErrorCode.ConnectorNotInstalled);
        Assert.Equal("PZ0306", PzErrorCode.ProtocolMismatch);
        Assert.Equal("PZ0307", PzErrorCode.NoConnectorEntryPoint);
    }

    // Restore/DriftChecker/ConnectorRegistryFactory raise RestoreException/PzValidationException
    // with these exact literals (RestoreException itself can't reference Pz.Core.Validation — see its
    // doc comment — so the pinning lives here, same cross-assembly pattern as the PZ03xx host codes above).
    [Fact]
    public void Restore_error_literals_match_pz_error_codes()
    {
        Assert.Equal("PZ0320", PzErrorCode.RestoreFailed);
        Assert.Equal("PZ0321", PzErrorCode.LockDrift);
        Assert.Equal("PZ0322", PzErrorCode.LockMissing);
    }
}
