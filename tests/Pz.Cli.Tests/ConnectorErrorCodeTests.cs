using Pz.Connectors.Toolkit.Auth;
using Pz.Core.Validation;

namespace Pz.Cli.Tests;

/// <summary>PZ codes that connector-side code repeats as literals (it cannot reference Pz.Core) must name the
/// catalog entry they mean, not merely some catalog entry.</summary>
public class ConnectorErrorCodeTests
{
    [Fact]
    public void The_refresh_token_rotation_code_matches_the_catalog()
        => Assert.Equal(PzErrorCode.OAuth2RefreshTokenRotated, OAuth2RefreshTokenAuthenticator.RotatedCode);
}
