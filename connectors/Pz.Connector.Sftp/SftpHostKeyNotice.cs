namespace Pz.Connector.Sftp;

/// <summary>The run-time notice text <see cref="SftpSource"/> and <see cref="SftpSink"/> both hand to
/// <c>INoticeAware.UseNotice</c> when their connection declares no <c>host_key_fingerprint</c> --
/// shared so the two connectors' phrasing cannot drift apart.</summary>
internal static class SftpHostKeyNotice
{
    /// <summary>Carries no host or any other configured value: the engine leads the line with the
    /// connection's name, and the same text is recorded as a run event.</summary>
    internal const string Unpinned =
        "accepting any SSH host key because 'host_key_fingerprint' is not set -- " +
        "no protection against a man-in-the-middle; run 'pz validate --connect' to see the fingerprint " +
        "the server presents, then pin it";
}
