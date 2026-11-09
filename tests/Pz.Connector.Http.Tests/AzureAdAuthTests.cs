using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Azure;
using Azure.Core;
using Azure.Identity;
using Microsoft.Identity.Client;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

public class AzureAdAuthTests
{
    private const string Scope = "https://graph.microsoft.com/.default";
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private static Dictionary<string, object?> Auth(params (string Key, object? Value)[] pairs)
    {
        var d = new Dictionary<string, object?>();
        foreach (var (key, value) in pairs)
        {
            d[key] = value;
        }

        return d;
    }

    private static AzureAdOptions Parse(Dictionary<string, object?> auth, string? baseDir = null)
    {
        var errors = new List<string>();
        var options = AzureAdOptions.TryParse(auth, baseDir, errors);
        Assert.Empty(errors);
        return Assert.IsType<AzureAdOptions>(options);
    }

    private static List<string> Errors(Dictionary<string, object?> auth)
    {
        var errors = new List<string>();
        Assert.Null(AzureAdOptions.TryParse(auth, null, errors));
        return errors;
    }

    [Fact]
    public void Service_principal_requires_tenant_client_and_one_secret_kind()
    {
        var errors = Errors(Auth(("method", "service_principal"), ("scope", Scope)));
        Assert.Contains(errors, e => e.Contains("tenant_id"));
        Assert.Contains(errors, e => e.Contains("client_id"));
        Assert.Contains(errors, e => e.Contains("client_secret") && e.Contains("certificate_path"));
    }

    [Fact]
    public void Service_principal_refuses_both_secret_and_certificate()
    {
        var errors = Errors(Auth(
            ("method", "service_principal"), ("scope", Scope), ("tenant_id", "t"), ("client_id", "c"),
            ("client_secret", "s"), ("certificate_path", "/x.pem")));
        Assert.Contains(errors, e => e.Contains("client_secret") && e.Contains("certificate_path"));
    }

    [Fact]
    public void Scope_is_required_and_the_error_gives_an_example()
    {
        var errors = Errors(Auth(("method", "managed_identity")));
        Assert.Contains(errors, e => e.Contains("auth.scope") && e.Contains("https://graph.microsoft.com/.default"));
    }

    [Fact]
    public void Unknown_method_lists_the_three()
    {
        var errors = Errors(Auth(("method", "magic"), ("scope", Scope)));
        var error = Assert.Single(errors);
        Assert.Contains("service_principal", error);
        Assert.Contains("managed_identity", error);
        Assert.Contains("credential_chain", error);
    }

    [Fact]
    public void Missing_method_is_an_error()
    {
        Assert.Contains(Errors(Auth(("scope", Scope))), e => e.Contains("auth.method"));
    }

    [Fact]
    public void Authority_host_must_be_an_absolute_https_url()
    {
        var errors = Errors(Auth(("method", "credential_chain"), ("scope", Scope), ("authority_host", "http://login.example.com")));
        Assert.Contains(errors, e => e.Contains("authority_host"));
        var ok = Parse(Auth(("method", "credential_chain"), ("scope", Scope), ("authority_host", "https://login.microsoftonline.us")));
        Assert.Equal(new Uri("https://login.microsoftonline.us"), ok.AuthorityHost);
    }

    [Fact]
    public void Relative_certificate_path_resolves_against_base_dir()
    {
        var o = Parse(
            Auth(("method", "service_principal"), ("scope", Scope), ("tenant_id", "t"), ("client_id", "c"),
                ("certificate_path", "certs/app.pem")),
            "/proj");
        Assert.Equal(Path.GetFullPath("/proj/certs/app.pem"), o.CertificatePath);
    }

    [Theory]
    [InlineData("~/certs/app.pem", "certs/app.pem")]
    [InlineData("~", "")]
    public void A_home_relative_certificate_path_resolves_under_the_user_profile(string configured, string rest)
    {
        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        var o = Parse(
            Auth(("method", "service_principal"), ("scope", Scope), ("tenant_id", "t"), ("client_id", "c"),
                ("certificate_path", configured)),
            "/proj");
        Assert.Equal(rest.Length == 0 ? home : Path.Combine(home, rest), o.CertificatePath);
    }

    private static TokenCredential Create(AzureAdOptions o)
    {
        var credential = o.CreateCredential(TimeSpan.FromSeconds(7), out var owned);
        owned?.Dispose();
        return credential;
    }

    [Theory]
    [InlineData("service_principal", false)]
    [InlineData("service_principal", true)]
    [InlineData("managed_identity", false)]
    [InlineData("credential_chain", false)]
    public void Credential_options_carry_the_connection_timeout_and_no_sdk_retries(string method, bool certificate)
    {
        var fields = new List<(string, object?)> { ("method", method), ("scope", Scope) };
        if (method == "service_principal")
        {
            fields.AddRange([("tenant_id", "t"), ("client_id", "c"), certificate ? ("certificate_path", (object?)"app.pem") : ("client_secret", "s")]);
        }

        var options = Parse(Auth(fields.ToArray())).CreateCredentialOptions(TimeSpan.FromSeconds(7));
        Assert.Equal(0, options.Retry.MaxRetries);
        Assert.Equal(TimeSpan.FromSeconds(7), options.Retry.NetworkTimeout);
    }

    [Fact]
    public void An_sdk_network_timeout_is_transient()
    {
        var timeout = new TaskCanceledException("The operation was cancelled because it exceeded the configured timeout of 0:00:05");
        Assert.True(AzureAdAuth.Classify(new AuthenticationFailedException("failed", timeout), "service_principal", Scope).IsTransient);
        Assert.True(AzureAdAuth.Classify(new RequestFailedException("timeout", timeout), "service_principal", Scope).IsTransient);
        Assert.True(AzureAdAuth.Classify(new OperationCanceledException("timeout", timeout), "service_principal", Scope).IsTransient);
    }

    [Fact]
    public void Credential_kind_follows_the_method()
    {
        var secret = Parse(Auth(("method", "service_principal"), ("scope", Scope), ("tenant_id", "t"), ("client_id", "c"), ("client_secret", "s")));
        Assert.IsType<ClientSecretCredential>(Create(secret));

        Assert.IsType<ManagedIdentityCredential>(Create(Parse(Auth(("method", "managed_identity"), ("scope", Scope)))));
        Assert.IsType<ManagedIdentityCredential>(
            Create(Parse(Auth(("method", "managed_identity"), ("scope", Scope), ("client_id", "uami")))));
        Assert.IsType<DefaultAzureCredential>(
            Create(Parse(Auth(("method", "credential_chain"), ("scope", Scope), ("authority_host", "https://login.microsoftonline.us")))));

        var pem = WriteThrowawayCertificate();
        try
        {
            var cert = Parse(Auth(("method", "service_principal"), ("scope", Scope), ("tenant_id", "t"), ("client_id", "c"), ("certificate_path", pem)));
            Assert.IsType<ClientCertificateCredential>(Create(cert));
        }
        finally
        {
            File.Delete(pem);
        }
    }

    private const string CertPassword = "Cert-P4ss-xyz";

    private static AzureAdOptions CertOptions(string path, string? password) => Parse(Auth(
        ("method", "service_principal"), ("scope", Scope), ("tenant_id", "t"), ("client_id", "c"),
        ("certificate_path", path), ("certificate_password", password)));

    private static string WriteTemp(string content)
    {
        var path = Path.Combine(Path.GetTempPath(), $"pz-azuread-{Guid.NewGuid():N}.bin");
        File.WriteAllText(path, content);
        return path;
    }

    private static string WritePfx(string password)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=pz-test", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        var path = Path.Combine(Path.GetTempPath(), $"pz-azuread-{Guid.NewGuid():N}.pfx");
        File.WriteAllBytes(path, cert.Export(X509ContentType.Pfx, password));
        return path;
    }

    private static string WriteEncryptedPem(string password)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=pz-test", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        var pbe = new PbeParameters(PbeEncryptionAlgorithm.Aes256Cbc, HashAlgorithmName.SHA256, 1000);
        return WriteTemp(cert.ExportCertificatePem() + "\n" + key.ExportEncryptedPkcs8PrivateKeyPem(password, pbe));
    }

    [Theory]
    [InlineData("bad tenant!")]
    [InlineData("tenant/evil")]
    [InlineData("a_b")]
    public void A_malformed_tenant_id_is_a_validation_error_for_service_principal(string tenant)
    {
        var errors = Errors(Auth(
            ("method", "service_principal"), ("scope", Scope), ("tenant_id", tenant), ("client_id", "c"), ("client_secret", "s")));
        Assert.Equal(
            $"auth.tenant_id '{tenant}' is not a valid tenant id (a GUID or a domain such as contoso.onmicrosoft.com)",
            Assert.Single(errors));
    }

    [Theory]
    [InlineData("72f988bf-86f1-41af-91ab-2d7cd011db47")]
    [InlineData("contoso.onmicrosoft.com")]
    [InlineData("common")]
    [InlineData("organizations")]
    [InlineData("consumers")]
    public void A_guid_a_domain_and_the_well_known_tenants_are_accepted(string tenant)
    {
        var o = Parse(Auth(
            ("method", "service_principal"), ("scope", Scope), ("tenant_id", tenant), ("client_id", "c"), ("client_secret", "s")));
        Assert.IsType<ClientSecretCredential>(Create(o));
    }

    [Fact]
    public void Tenant_validation_agrees_with_the_credential_library_on_ascii_and_sampled_non_ascii_characters()
    {
        var characters = Enumerable.Range(0x20, 0x7f - 0x20).Select(i => (char)i)
            .Concat(['\u00e9', '\u0436', '\u0663', '\uff11']);
        foreach (var c in characters)
        {
            var tenant = "a" + c + "b";
            var errors = new List<string>();
            var parsed = AzureAdOptions.TryParse(
                Auth(("method", "service_principal"), ("scope", Scope), ("tenant_id", tenant), ("client_id", "c"), ("client_secret", "s")),
                null, errors);
            var sdkAccepts = true;
            try
            {
                _ = new ClientSecretCredential(tenant, "c", "s");
            }
            catch (ArgumentException)
            {
                sdkAccepts = false;
            }

            Assert.True(sdkAccepts == (parsed is not null), $"tenant '{tenant}': sdk accepts={sdkAccepts}");
        }
    }

    private static AzureAdOptions Unvalidated(string tenant, string? certificatePath = null, string? password = null) => new(
        "service_principal", tenant, "c", certificatePath is null ? "s" : null, certificatePath, password, Scope, null);

    [Fact]
    public void A_credential_the_library_refuses_is_a_permanent_connector_error_without_its_text()
    {
        var ex = Assert.Throws<PzConnectorException>(() => Unvalidated("bad tenant!").CreateAuthenticator(Timeout, TimeProvider.System));
        Assert.False(ex.IsTransient);
        Assert.Contains("azure_ad", ex.Message, StringComparison.Ordinal);
        Assert.Contains("tenant_id", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("bad tenant!", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Invalid tenant id", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_refused_credential_disposes_the_certificate_it_loaded()
    {
        var path = WritePfx(CertPassword);
        try
        {
            IDisposable? owned = null;
            Assert.Throws<PzConnectorException>(
                () => Unvalidated("bad tenant!", path, CertPassword).CreateCredential(Timeout, out owned));
            var certificate = Assert.IsType<X509Certificate2>(owned);
            Assert.Equal(IntPtr.Zero, certificate.Handle);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Password_protected_pkcs12_and_encrypted_pem_both_load()
    {
        foreach (var path in new[] { WritePfx(CertPassword), WriteEncryptedPem(CertPassword) })
        {
            try
            {
                var credential = CertOptions(path, CertPassword).CreateCredential(TimeSpan.FromSeconds(7), out var owned);
                Assert.IsType<ClientCertificateCredential>(credential);
                Assert.NotNull(owned);
                owned.Dispose();
            }
            finally
            {
                File.Delete(path);
            }
        }
    }

    [Theory]
    [InlineData("pfx")]
    [InlineData("pem")]
    public void Wrong_certificate_password_is_a_coded_error_without_the_password(string kind)
    {
        var path = kind == "pfx" ? WritePfx(CertPassword) : WriteEncryptedPem(CertPassword);
        try
        {
            var ex = Assert.Throws<PzConnectorException>(() => CertOptions(path, "wrong-pw-123").CreateCredential(TimeSpan.FromSeconds(7), out _));
            Assert.False(ex.IsTransient);
            Assert.Contains("certificate_path", ex.Message);
            Assert.DoesNotContain("wrong-pw-123", ex.Message);
            Assert.DoesNotContain(CertPassword, ex.Message);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Missing_certificate_file_is_a_coded_error()
    {
        var path = Path.Combine(Path.GetTempPath(), $"pz-azuread-missing-{Guid.NewGuid():N}.pfx");
        var ex = Assert.Throws<PzConnectorException>(() => CertOptions(path, CertPassword).CreateCredential(TimeSpan.FromSeconds(7), out _));
        Assert.False(ex.IsTransient);
        Assert.Contains("certificate_path", ex.Message);
        Assert.DoesNotContain(CertPassword, ex.Message);
    }

    private static string WriteThrowawayCertificate()
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=pz-test", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        var path = Path.Combine(Path.GetTempPath(), $"pz-azuread-{Guid.NewGuid():N}.pem");
        File.WriteAllText(path, cert.ExportCertificatePem() + "\n" + key.ExportPkcs8PrivateKeyPem());
        return path;
    }

    private sealed class FakeCredential(Func<AccessToken> next) : TokenCredential
    {
        public int Calls;
        public string[]? Scopes;

        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            Calls++;
            Scopes = requestContext.Scopes;
            return new(next());
        }
    }

    private sealed class Clock : TimeProvider
    {
        public DateTimeOffset Now = DateTimeOffset.UnixEpoch.AddDays(1000);
        public override DateTimeOffset GetUtcNow() => Now;
    }

    [Fact]
    public async Task Token_and_expiry_come_from_the_credential()
    {
        var clock = new Clock();
        var cred = new FakeCredential(() => new AccessToken("t", clock.Now.AddHours(1)));
        using var auth = new AzureAdAuthenticator(cred, Scope, "service_principal", clock);

        using var first = new HttpRequestMessage(HttpMethod.Get, "https://api.example/x");
        await auth.ApplyAsync(first, CancellationToken.None).AsTask().WaitAsync(Timeout);
        Assert.Equal("Bearer t", first.Headers.Authorization!.ToString());
        Assert.Equal(Scope, Assert.Single(cred.Scopes!));

        clock.Now = clock.Now.AddMinutes(30);
        using var second = new HttpRequestMessage(HttpMethod.Get, "https://api.example/x");
        await auth.ApplyAsync(second, CancellationToken.None).AsTask().WaitAsync(Timeout);
        Assert.Equal(1, cred.Calls);

        clock.Now = clock.Now.AddMinutes(30);
        using var third = new HttpRequestMessage(HttpMethod.Get, "https://api.example/x");
        await auth.ApplyAsync(third, CancellationToken.None).AsTask().WaitAsync(Timeout);
        Assert.Equal(2, cred.Calls);
    }

    [Fact]
    public async Task A_failing_credential_surfaces_as_a_classified_connector_exception()
    {
        var cred = new FakeCredential(() => throw new CredentialUnavailableException("no managed identity here; Trace ID: abc"));
        using var auth = new AzureAdAuthenticator(cred, Scope, "managed_identity", new Clock());
        using var request = new HttpRequestMessage(HttpMethod.Get, "https://api.example/x");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await auth.ApplyAsync(request, CancellationToken.None).AsTask().WaitAsync(Timeout));
        Assert.Contains("no managed identity is available here (are you running outside Azure? use `credential_chain` to fall back to `az login`)", ex.Message);
        Assert.DoesNotContain("Trace ID", ex.Message);
    }

    [Theory]
    [InlineData("scope")]
    [InlineData("client_id")]
    [InlineData("tenant_id")]
    [InlineData("method")]
    [InlineData("authority_host")]
    public void A_list_or_map_where_a_single_value_is_expected_is_an_aggregated_error(string key)
    {
        var auth = Auth(("method", "service_principal"), ("scope", Scope), ("tenant_id", "t"), ("client_id", "c"),
            ("client_secret", "s"), ("authority_host", "https://login.example.com"));
        auth[key] = new List<object?> { "a", "b" };
        Assert.Contains($"auth.{key} must be a single value, not a list", Errors(auth));

        auth[key] = new Dictionary<string, object?> { ["x"] = "y" };
        Assert.Contains($"auth.{key} must be a single value, not a map", Errors(auth));
    }

    [Fact]
    public void Several_non_scalar_values_are_all_reported_together()
    {
        var errors = Errors(Auth(("method", "service_principal"), ("scope", new List<object?> { "a" }), ("tenant_id", "t"),
            ("client_id", new Dictionary<string, object?>()), ("client_secret", "s")));
        Assert.Contains("auth.scope must be a single value, not a list", errors);
        Assert.Contains("auth.client_id must be a single value, not a map", errors);
        Assert.DoesNotContain(errors, e => e.Contains("System."));
    }

    [Fact]
    public void A_non_scalar_certificate_path_or_password_is_refused()
    {
        var errors = Errors(Auth(("method", "service_principal"), ("scope", Scope), ("tenant_id", "t"), ("client_id", "c"),
            ("certificate_path", new List<object?> { "a" }), ("certificate_password", new Dictionary<string, object?>())));
        Assert.Contains("auth.certificate_path must be a single value, not a list", errors);
        Assert.Contains("auth.certificate_password must be a single value, not a map", errors);
    }

    [Fact]
    public async Task An_argument_exception_from_the_credential_is_a_fixed_permanent_error()
    {
        var cred = new FakeCredential(() => throw new ArgumentException("scope 'S3CRET-ish' is bad"));
        using var auth = new AzureAdAuthenticator(cred, Scope, "service_principal", new Clock());
        using var request = new HttpRequestMessage(HttpMethod.Get, "https://api.example/x");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await auth.ApplyAsync(request, CancellationToken.None).AsTask().WaitAsync(Timeout));
        Assert.False(ex.IsTransient);
        Assert.Contains("azure_ad sign-in failed (service_principal)", ex.Message);
        Assert.DoesNotContain("S3CRET", ex.Message);
    }

    [Fact]
    public void Credential_unavailable_for_managed_identity_hints_at_credential_chain()
    {
        var ex = AzureAdAuth.Classify(new CredentialUnavailableException("ManagedIdentityCredential authentication unavailable"), "managed_identity", Scope);
        Assert.False(ex.IsTransient);
        Assert.Contains("no managed identity is available here (are you running outside Azure? use `credential_chain` to fall back to `az login`)", ex.Message);
    }

    [Fact]
    public void Authentication_failure_carries_the_AADSTS_code_only()
    {
        var inner = new AuthenticationFailedException(
            "ClientSecretCredential authentication failed: AADSTS7000215: Invalid client secret provided. Trace ID: 0000-1111 Correlation ID: 2222-3333 https://login.microsoftonline.com/x");
        var ex = AzureAdAuth.Classify(inner, "service_principal", Scope);
        Assert.False(ex.IsTransient);
        Assert.Contains("AADSTS7000215", ex.Message);
        Assert.DoesNotContain("Trace ID", ex.Message);
        Assert.DoesNotContain("Correlation", ex.Message);
        Assert.DoesNotContain("login.microsoftonline.com", ex.Message);
        Assert.DoesNotContain("Invalid client secret", ex.Message);
    }

    [Theory]
    [InlineData("AADSTS70011")]
    [InlineData("AADSTS1002012")]
    [InlineData("AADSTS500011")]
    public void An_invalid_scope_aadsts_code_names_the_scope_and_an_example(string code)
    {
        var inner = new AuthenticationFailedException($"ClientSecretCredential authentication failed: {code}: The provided value for scope is not valid. Trace ID: 0000");
        var ex = AzureAdAuth.Classify(inner, "service_principal", "api://my-api/user_impersonation");
        Assert.False(ex.IsTransient);
        Assert.Contains("'api://my-api/user_impersonation'", ex.Message);
        Assert.Contains(code, ex.Message);
        Assert.Contains("/.default", ex.Message);
        Assert.Contains("https://graph.microsoft.com/.default", ex.Message);
        Assert.DoesNotContain("Trace ID", ex.Message);
    }

    [Theory]
    [InlineData("invalid_scope")]
    [InlineData("invalid_resource")]
    public void An_msal_invalid_scope_error_code_anywhere_in_the_chain_is_a_scope_error(string errorCode)
    {
        var inner = new AuthenticationFailedException("failed", new InvalidOperationException("wrapper", new MsalServiceException(errorCode, "x", 400)));
        var ex = AzureAdAuth.Classify(inner, "service_principal", "api://my-api/user_impersonation");
        Assert.False(ex.IsTransient);
        Assert.Contains("refused scope 'api://my-api/user_impersonation'", ex.Message);
        Assert.Contains("https://graph.microsoft.com/.default", ex.Message);
    }

    [Fact]
    public void An_inner_message_carrying_an_invalid_scope_code_is_a_scope_error()
    {
        var inner = new AuthenticationFailedException("failed", new MsalServiceException("x", "AADSTS70011: invalid scope", 400));
        var ex = AzureAdAuth.Classify(inner, "service_principal", "bad");
        Assert.Contains("refused scope 'bad' (AADSTS70011)", ex.Message);
    }

    [Fact]
    public void An_unrelated_aadsts_code_keeps_the_credential_hint()
    {
        var ex = AzureAdAuth.Classify(new AuthenticationFailedException("AADSTS700110: x"), "service_principal", Scope);
        Assert.Contains("tenant_id", ex.Message);
        Assert.DoesNotContain("refused scope", ex.Message);
    }

    [Fact]
    public void Authentication_failure_without_a_code_still_gives_the_fixed_hint()
    {
        var ex = AzureAdAuth.Classify(new AuthenticationFailedException("boom with secret-ish text"), "service_principal", Scope);
        Assert.False(ex.IsTransient);
        Assert.Contains("tenant_id", ex.Message);
        Assert.DoesNotContain("boom", ex.Message);
    }

    [Theory]
    [InlineData(429)]
    [InlineData(503)]
    public void Throttled_or_unavailable_identity_endpoint_is_transient(int status)
    {
        var inner = new AuthenticationFailedException("failed", new RequestFailedException(status, "throttled"));
        Assert.True(AzureAdAuth.Classify(inner, "service_principal", Scope).IsTransient);
    }

    [Fact]
    public void A_network_failure_is_transient()
    {
        var inner = new AuthenticationFailedException("failed", new HttpRequestException("down"));
        Assert.True(AzureAdAuth.Classify(inner, "credential_chain", Scope).IsTransient);
    }

    [Fact]
    public void A_client_error_from_the_identity_endpoint_is_permanent()
    {
        var inner = new AuthenticationFailedException("failed", new RequestFailedException(400, "bad"));
        Assert.False(AzureAdAuth.Classify(inner, "service_principal", Scope).IsTransient);
    }

    [Theory]
    [InlineData(429, "service_not_available")]
    [InlineData(503, "service_not_available")]
    [InlineData(0, "request_timeout")]
    public void Msal_throttling_or_timeout_is_transient(int status, string code)
    {
        var inner = new AuthenticationFailedException("failed", new MsalServiceException(code, "x", status));
        Assert.True(AzureAdAuth.Classify(inner, "service_principal", Scope).IsTransient);
    }

    [Fact]
    public void Msal_invalid_client_stays_permanent()
    {
        var inner = new AuthenticationFailedException("failed", new MsalServiceException("invalid_client", "x", 400));
        Assert.False(AzureAdAuth.Classify(inner, "service_principal", Scope).IsTransient);
    }

    [Fact]
    public void Timeouts_are_transient()
    {
        Assert.True(AzureAdAuth.Classify(new AuthenticationFailedException("f", new TimeoutException()), "credential_chain", Scope).IsTransient);
        Assert.True(AzureAdAuth.Classify(new AuthenticationFailedException("f", new TaskCanceledException()), "credential_chain", Scope).IsTransient);
    }

    private sealed class BlockingCredential : TokenCredential
    {
        public readonly TaskCompletionSource Started = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public override async ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            Started.SetResult();
            await Task.Delay(System.Threading.Timeout.Infinite, cancellationToken);
            return default;
        }
    }

    [Fact]
    public async Task Disposal_cancellation_is_not_reported_as_a_transient_failure()
    {
        var cred = new BlockingCredential();
        var auth = new AzureAdAuthenticator(cred, Scope, "service_principal", new Clock());
        using var request = new HttpRequestMessage(HttpMethod.Get, "https://api.example/x");
        var pending = auth.ApplyAsync(request, CancellationToken.None).AsTask();
        await cred.Started.Task.WaitAsync(Timeout);
        auth.Dispose();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pending.WaitAsync(Timeout));
    }
}
