using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

public sealed class HostArgumentsTests
{
    [Fact]
    public void Serve_mode_takes_the_socket_path()
    {
        var command = Assert.IsType<ServeCommand>(HostArguments.Parse(["--pz-socket", "/tmp/x.sock"]));
        Assert.Equal("/tmp/x.sock", command.SocketPath);
    }

    [Fact]
    public void Manifest_mode_collects_entrypoints_sorted_by_rid()
    {
        var command = Assert.IsType<ManifestCommand>(HostArguments.Parse([
            "--pz-manifest", "--out", "m.json",
            "--entrypoint", "win-x64=native/c.exe", "--entrypoint", "linux-x64=native/c"]));
        Assert.Equal("m.json", command.OutPath);
        Assert.Equal(["linux-x64", "win-x64"], command.Entrypoints.Keys.ToArray());
        Assert.Equal("native/c.exe", command.Entrypoints["win-x64"]);
    }

    [Fact]
    public void Manifest_mode_collects_the_project_directory_anchor_flag()
    {
        var command = Assert.IsType<ManifestCommand>(HostArguments.Parse(
            ["--pz-manifest", "--out", "m.json", "--project-directory-anchor"]));
        Assert.True(command.ProjectDirectoryAnchor);
    }

    [Fact]
    public void Manifest_mode_defaults_the_project_directory_anchor_flag_to_false()
    {
        var command = Assert.IsType<ManifestCommand>(HostArguments.Parse(["--pz-manifest", "--out", "m.json"]));
        Assert.False(command.ProjectDirectoryAnchor);
    }

    [Theory]
    // Each case is ONE argv, so the string[] is wrapped: xunit would otherwise spread a bare
    // string[] across the theory's parameters.
    [InlineData(new object[] { new string[0] })]
    [InlineData(new object[] { new[] { "--pz-socket" } })]
    [InlineData(new object[] { new[] { "--pz-manifest" } })]
    [InlineData(new object[] { new[] { "--pz-manifest", "--out" } })]
    [InlineData(new object[] { new[] { "--pz-manifest", "--out", "m.json", "--entrypoint", "no-equals" } })]
    [InlineData(new object[] { new[] { "--pz-socket", "/tmp/x", "--pz-manifest", "--out", "m.json" } })]
    [InlineData(new object[] { new[] { "--pz-socket", "/tmp/x", "--verbose" } })]
    [InlineData(new object[] { new[] { "--root", "/data" } })]
    [InlineData(new object[] { new[] { "--pz-socket", "/tmp/x", "--project-directory-anchor" } })]
    [InlineData(new object[] { new[] { "--project-directory-anchor" } })]
    public void Anything_the_sdk_does_not_own_is_invalid(string[] args)
    {
        Assert.IsType<InvalidCommand>(HostArguments.Parse(args));
    }
}
