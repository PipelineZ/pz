using Azure;
using Pz.Connectors.Toolkit.Http;

namespace Pz.Connector.AzureBlob;

/// <summary>Pure, offline-testable classifier for whether a raw failure at a universal-tier Azure Storage
/// network boundary (blob download/list/upload, server-side copy-promote) should be surfaced to the engine
/// as transient (worth a retry -- see <see cref="PzConnectorException.IsTransient"/>) or permanent.
///
/// The connector never retries internally (CLAUDE.md) and cannot reference Pz.Engine, so -- mirroring how
/// Postgres/SqlServer forward their ADO driver's own <c>IsTransient</c> signal -- this is Azure's own local
/// closed list, since the Azure SDK exposes no driver-provided transient flag: the 408/429/5xx status set
/// documented for the DuckDB httpfs-family's own retry behavior, plus the network-level exception shapes a
/// dropped/reset connection or timed-out request surfaces as.
///
/// The status set delegates to the toolkit's canonical <see cref="TransientClassifier"/> — one source of
/// truth across connectors.</summary>
internal static class AzureTransient
{
    public static bool IsTransient(Exception ex) => ex switch
    {
        RequestFailedException rfe => TransientClassifier.IsTransientStatus(rfe.Status),
        IOException => true,
        TimeoutException => true,
        System.Net.Sockets.SocketException => true,
        _ => false,
    };
}
