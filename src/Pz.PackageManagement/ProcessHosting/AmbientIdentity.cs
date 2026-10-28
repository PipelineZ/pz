using Pz.Connectors.Abstractions;

namespace Pz.PackageManagement.ProcessHosting;

/// <summary>The environment variables behind each <see cref="AmbientIdentityGroups"/> name: what
/// <see cref="ConnectorProcess.Spawn"/> copies into a connector child, on top of its allowlist,
/// for every group the package's manifest declares.
///
/// <para>Variables a platform sets to tell a cloud SDK where its identity is (a host-local token
/// endpoint, a federated token file, which client/tenant/role to ask for), plus the region and project
/// settings those SDKs need alongside them. The azure group also carries a service principal
/// configured through the environment (the variables Azure.Identity's <c>EnvironmentCredential</c>
/// reads: <c>AZURE_CLIENT_SECRET</c>, or a certificate path and its password), so a host that
/// authenticates as a service principal works the same for a declaring connector as for a builtin
/// one; forwarding <c>AZURE_CLIENT_ID</c> without its secret would instead send
/// <c>DefaultAzureCredential</c> to managed identity with the service principal's id. Otherwise no
/// static key or secret held in a variable (<c>AWS_ACCESS_KEY_ID</c>, <c>AWS_SECRET_ACCESS_KEY</c>,
/// <c>AWS_SESSION_TOKEN</c>, <c>AZURE_STORAGE_KEY</c>, <c>AZURE_USERNAME</c>/<c>AZURE_PASSWORD</c>,
/// ...): those belong in <c>connections.yml</c> as <c>${VAR}</c>. One exception by nature: <c>GOOGLE_APPLICATION_CREDENTIALS</c> is how GCP
/// federation is found, but on a host that points it at a downloaded service-account key, a package
/// declaring <c>gcp</c> can use that key. <c>ProgramData</c> is in the azure group because the Azure
/// Arc source checks its token-file path against it on Windows. <c>IDENTITY_HEADER</c>, <c>MSI_SECRET</c> and
/// <c>AWS_CONTAINER_AUTHORIZATION_TOKEN</c> are secrets, but each only unlocks a host-local endpoint
/// for the host's own identity. <c>AZURE_CLIENT_SECRET</c> and the certificate are the service
/// principal itself and work from anywhere. Either way a package that declared the group can act as
/// the host's identity, which is exactly what it needs and what builtin connectors already have.</para></summary>
public static class AmbientIdentity
{
    private static readonly Dictionary<string, string[]> VariablesByGroup = new(StringComparer.Ordinal)
    {
        [AmbientIdentityGroups.Azure] =
        [
            "IDENTITY_ENDPOINT", "IDENTITY_HEADER", "IMDS_ENDPOINT", "MSI_ENDPOINT", "MSI_SECRET",
            "IDENTITY_SERVER_THUMBPRINT", "AZURE_FEDERATED_TOKEN_FILE", "AZURE_CLIENT_ID", "AZURE_TENANT_ID",
            "AZURE_AUTHORITY_HOST", "AZURE_POD_IDENTITY_AUTHORITY_HOST", "DEFAULT_IDENTITY_CLIENT_ID",
            "ProgramData",
            // A service principal (EnvironmentCredential) and DefaultAzureCredential's own selection.
            "AZURE_CLIENT_SECRET", "AZURE_CLIENT_CERTIFICATE_PATH", "AZURE_CLIENT_CERTIFICATE_PASSWORD",
            "AZURE_CLIENT_SEND_CERTIFICATE_CHAIN", "AZURE_ADDITIONALLY_ALLOWED_TENANTS", "AZURE_TOKEN_CREDENTIALS",
        ],
        [AmbientIdentityGroups.Aws] =
        [
            "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME",
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "AWS_REGION",
            "AWS_DEFAULT_REGION", "AWS_EC2_METADATA_DISABLED", "AWS_EC2_METADATA_SERVICE_ENDPOINT",
        ],
        [AmbientIdentityGroups.Gcp] =
        [
            "GOOGLE_APPLICATION_CREDENTIALS", "GCE_METADATA_HOST", "GCE_METADATA_IP", "GOOGLE_CLOUD_PROJECT",
        ],
    };

    /// <summary>The variable names for <paramref name="groups"/>, in group order. A group this build
    /// does not know contributes nothing: the load (and <c>pz connector test</c>) warns about it, and
    /// an unknown name never fails a run.</summary>
    public static IReadOnlyList<string> VariablesFor(IEnumerable<string> groups) =>
        [.. groups.Distinct(StringComparer.Ordinal)
            .SelectMany(group => VariablesByGroup.TryGetValue(group, out var names) ? names : [])];

    /// <summary>The groups this build has a variable list for. Internal so a test can pin that it is
    /// exactly <see cref="AmbientIdentityGroups.All"/>.</summary>
    internal static IEnumerable<string> Groups => VariablesByGroup.Keys;

    /// <summary>The names in <paramref name="groups"/> this build has no variable list for, in the
    /// order declared.</summary>
    public static IReadOnlyList<string> UnknownGroups(IEnumerable<string> groups) =>
        [.. groups.Where(group => !VariablesByGroup.ContainsKey(group)).Distinct(StringComparer.Ordinal)];
}
