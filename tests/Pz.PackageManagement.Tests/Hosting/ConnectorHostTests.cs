using System.Runtime.CompilerServices;
using Pz.Connectors.Abstractions;
using Pz.PackageManagement.Hosting;

namespace Pz.PackageManagement.Tests.Hosting;

[Collection("fake-connectors")]
public class ConnectorHostTests(FakeConnectorFixture fixture)
{
    private static readonly ConnectorPackageRef A = new("FakeConnectorA", "1.0.0");
    private static readonly ConnectorPackageRef B = new("FakeConnectorB", "1.0.0");
    private static readonly ConnectorPackageRef Old = new("FakeConnectorOld", "1.0.0");

    [Fact]
    public async Task Loads_connector_and_resolves_via_attribute()
    {
        await using var host = ConnectorHost.LoadFromDirectory(fixture.PackagesRoot, [A]);
        var connector = host.Get("fakeA");
        Assert.Equal("fakeA", connector.Info.Name);
        Assert.Single(host.Installed);
    }

    [Fact]
    public async Task Two_connectors_with_conflicting_private_dependency_versions_coexist()
    {
        await using var host = ConnectorHost.LoadFromDirectory(fixture.PackagesRoot, [A, B]);
        Assert.Equal("fakedep-v1", host.Get("fakeA").Info.Version);
        Assert.Equal("fakedep-v2", host.Get("fakeB").Info.Version);
    }

    [Fact]
    public async Task Abstractions_types_are_reference_equal_across_alcs()
    {
        await using var host = ConnectorHost.LoadFromDirectory(fixture.PackagesRoot, [A]);
        var connector = host.Get("fakeA");
        // IConnector here is the DEFAULT ALC's type; a plugin compiled against its own copy would fail this cast chain.
        Assert.IsAssignableFrom<IConnector>(connector);
        Assert.Same(typeof(IConnector).Assembly, connector.GetType().GetInterface("IConnector")!.Assembly);
    }

    [Fact]
    public void Protocol_major_mismatch_is_error_PZ0306()
    {
        var ex = Assert.Throws<ConnectorHostException>(
            () => ConnectorHost.LoadFromDirectory(fixture.PackagesRoot, [Old]));
        Assert.Equal("PZ0306", ex.Code);
        Assert.Contains("fakeOld", ex.Message);
        Assert.Contains("0", ex.Message);   // connector's major
        Assert.Contains("1", ex.Message);   // host's major
        Assert.NotNull(ex.Hint);
    }

    [Fact]
    public async Task Missing_connector_name_is_error_PZ0305()
    {
        await using var host = ConnectorHost.LoadFromDirectory(fixture.PackagesRoot, [A]);
        var ex = Assert.Throws<ConnectorHostException>(() => host.Get("nonexistent"));
        Assert.Equal("PZ0305", ex.Code);
        Assert.Contains("nonexistent", ex.Message);
    }

    [Fact]
    public void Missing_package_directory_is_error_PZ0304()
    {
        var ex = Assert.Throws<ConnectorHostException>(
            () => ConnectorHost.LoadFromDirectory(fixture.PackagesRoot, [new ConnectorPackageRef("NoSuchPkg", "9.9.9")]));
        Assert.Equal("PZ0304", ex.Code);
        Assert.Contains("NoSuchPkg", ex.Message);
    }

    // Regression for the ALC leak on partial multi-package load failure: FakeConnectorA loads fine, then
    // the second package (missing directory, PZ0304) fails. The fix must unload FakeConnectorA's
    // already-created ConnectorLoadContext before rethrowing.
    //
    // GC-reclaimability alone does NOT prove this: an unreachable collectible ALC becomes eligible for
    // collection whether or not Unload() was ever called, so a WeakReference/GC.Collect() check passes
    // even with the fix's context.Unload() call deleted. Instead, this test subscribes to the context's
    // AssemblyLoadContext.Unloading event (fires synchronously inside Unload()) via the
    // OnContextCreatedForTests seam, and asserts it fired - which only happens if Unload() actually ran.
    [Fact]
    public void Partial_load_failure_unloads_prior_contexts()
    {
        var weakContext = LoadTwoPackagesWhereSecondFailsAndCaptureFirstContext(
            fixture.PackagesRoot, out var thrown, out var unloadingFireCount);

        Assert.NotNull(thrown);
        Assert.Equal("PZ0304", thrown!.Code);
        Assert.Contains("NoSuchPkg", thrown.Message);

        // Required assertion: proves Unload() was actually invoked on FakeConnectorA's context, exactly
        // once, before the exception propagated.
        Assert.Equal(1, unloadingFireCount);

        // Secondary assertion only: proves the context is no longer rooted, not that Unload() ran - kept
        // for extra signal, but it is not load-bearing (see comment above on why GC-reclaimability alone
        // can't distinguish fixed from buggy code).
        for (var i = 0; i < 10 && weakContext.IsAlive; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        Assert.False(weakContext.IsAlive);
    }

    // Marked NoInlining so this method's locals (the only strong reference to the loaded
    // ConnectorLoadContext) are guaranteed to go out of scope on return, per the documented
    // AssemblyLoadContext-unload WeakReference testing pattern.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference LoadTwoPackagesWhereSecondFailsAndCaptureFirstContext(
        string packagesRoot, out ConnectorHostException? thrown, out int unloadingFireCount)
    {
        ConnectorLoadContext? captured = null;
        var fireCount = 0;
        ConnectorHost.OnContextCreatedForTests = context =>
        {
            captured ??= context;
            context.Unloading += _ => Interlocked.Increment(ref fireCount);
        };
        try
        {
            try
            {
                ConnectorHost.LoadFromDirectory(
                    packagesRoot, [A, new ConnectorPackageRef("NoSuchPkg", "9.9.9")]);
                thrown = null;
            }
            catch (ConnectorHostException ex)
            {
                thrown = ex;
            }
        }
        finally
        {
            ConnectorHost.OnContextCreatedForTests = null;
        }

        unloadingFireCount = fireCount;
        return new WeakReference(captured);
    }
}
