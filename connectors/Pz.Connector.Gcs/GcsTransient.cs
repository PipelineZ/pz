using Google;
using Pz.Connectors.Toolkit.Http;

namespace Pz.Connector.Gcs;

/// <summary>Pure, offline-testable classifier for whether a raw failure at a universal-tier gcs
/// network boundary (SDK upload) should be surfaced to the engine as transient (worth a retry — see
/// <see cref="Pz.Connectors.Abstractions.PzConnectorException.IsTransient"/>) or permanent.
///
/// The connector never retries internally and cannot reference Pz.Engine, so — mirroring the azure
/// connector's local closed list, since the Google SDK exposes no driver-provided transient flag —
/// the HTTP status set delegates to the toolkit's canonical <see cref="TransientClassifier"/> (one
/// source of truth across connectors), plus the network-level exception shapes a dropped/reset
/// connection or timed-out request surfaces as.</summary>
internal static class GcsTransient
{
    public static bool IsTransient(Exception ex) => ex switch
    {
        GoogleApiException gae => TransientClassifier.IsTransientStatus((int)gae.HttpStatusCode),
        HttpRequestException => true,
        IOException => true,
        TimeoutException => true,
        System.Net.Sockets.SocketException => true,
        _ => false,
    };
}
