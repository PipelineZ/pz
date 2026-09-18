using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

/// <summary>Unit-level (no engine, no live server) proof that <see cref="SftpSource"/> and
/// <see cref="SftpSink"/> notice an unpinned host key exactly the way <see cref="SftpConnector"/>'s
/// offline warning and online probe do: naming <c>host_key_fingerprint</c>, never firing when a pin is
/// declared. The engine-level "called at most once per open" guarantee is
/// <c>NoticeAwareWiringTests</c>' job (Pz.Engine.Tests); these facts only prove the connectors' own
/// decision of WHETHER to notice.</summary>
public sealed class SftpNoticeAwareTests
{
    private static readonly FakeSftpFileSystem Fake = new();

    private static SftpConnectionSettings Settings(string? hostKeyFingerprint) =>
        new("sftp.example", 22, "u", "p", null, null, hostKeyFingerprint, Root: null);

    [Fact]
    public void Source_notices_an_unpinned_host_key()
    {
        var source = new SftpSource(Settings(hostKeyFingerprint: null), _ => Fake);
        string? message = null;

        source.UseNotice(m => message = m);

        Assert.NotNull(message);
        Assert.Contains("host_key_fingerprint", message, StringComparison.Ordinal);
        Assert.DoesNotContain("sftp.example", message, StringComparison.Ordinal);
    }

    [Fact]
    public void Source_with_a_pinned_host_key_notices_nothing()
    {
        var source = new SftpSource(Settings("SHA256:pinned"), _ => Fake);
        var called = false;

        source.UseNotice(_ => called = true);

        Assert.False(called);
    }

    [Fact]
    public void Sink_notices_an_unpinned_host_key()
    {
        var sink = new SftpSink(Settings(hostKeyFingerprint: null), _ => Fake);
        string? message = null;

        sink.UseNotice(m => message = m);

        Assert.NotNull(message);
        Assert.Contains("host_key_fingerprint", message, StringComparison.Ordinal);
    }

    [Fact]
    public void Sink_with_a_pinned_host_key_notices_nothing()
    {
        var sink = new SftpSink(Settings("SHA256:pinned"), _ => Fake);
        var called = false;

        sink.UseNotice(_ => called = true);

        Assert.False(called);
    }
}
