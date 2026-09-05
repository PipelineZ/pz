using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

[assembly: PzConnector("azureblob", typeof(Pz.Connector.AzureBlob.AzureConnector))]

namespace Pz.Connector.AzureBlob;

/// <summary>Azure Blob Storage / ADLS Gen2 source + sink connector. Reads are NATIVE-ONLY
/// (<see cref="INativeOnlySource"/>): the DuckDB `azure` extension (CREATE SECRET TYPE azure +
/// read_parquet/read_csv/read_json) is the one read path.
/// The universal tier is WRITE-ONLY: Azure Storage SDK write sessions carry partition_by fan-out
/// writes; the SDK also backs GetSchemaAsync's schema peek. One config yields both a DuckDB
/// secret and an SDK client (see AzureAuth). GatedOperations: <see cref="AzureSink"/>
/// routes every write session's open_write/commit_copy/delete_temp op through an engine-supplied
/// <see cref="Pz.Connectors.Abstractions.IOperationGate"/> when one is provided; native COPY and
/// native scan are unaffected (out of .NET reach).</summary>
public sealed class AzureConnector : ISourceConnector, ISinkConnector, INativeOnlySource
{
    public ConnectorInfo Info => new("azureblob", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
        ConnectorCapabilities.ReplaceWrites |
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.PathTemplating |
        ConnectorCapabilities.GatedOperations;

    public string ConnectionConfigSchema =>
        """{ "type": "object", "required": ["auth"], "properties": { "auth": { "enum": ["connection_string","account_key","service_principal","credential_chain","managed_identity"] }, "connection_string": { "type": "string" }, "account_name": { "type": "string" }, "account_key": { "type": "string" }, "tenant_id": { "type": "string" }, "client_id": { "type": "string" }, "client_secret": { "type": "string" }, "chain": { "type": "string" }, "endpoint": { "type": "string" } }, "additionalProperties": false }""";

    // Strict source-dataset schema: unknown/typo'd options fail `pz validate` with PZ0301 instead of
    // being silently ignored — parity with postgres/localfiles. Mirrors what AzureUrl.ParseDataset/
    // AzureSource actually read: scheme (az/azure/abfss), container + path (required), format (parquet
    // default), and the generic columns: contract (AzureTypeNameMap's eight type names).
    // files_per_partition is deliberately ACCEPTED here (int-or-string, DagCompiler's PZ0222 owns value
    // shape) so the plan-time PZ0312 refusal on this native-only source keeps owning that case with its
    // targeted message. Sink OUTPUT options stay plan/probe-validated by AzureSink — tier 3 never
    // evaluates output options.
    public string DatasetConfigSchema =>
        """{ "type": "object", "required": ["container","path"], "properties": { "scheme": { "enum": ["az","azure","abfss"] }, "container": { "type": "string" }, "path": { "type": "string" }, """ + FileFormatCatalog.SchemaProperties +
        """, "columns": { "type": "object", "minProperties": 1, "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } }, "files_per_partition": { "type": ["integer","string"] } }, "additionalProperties": false }""";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(AzureAuth.Validate(config));

    // A service-level call (Get Blob Service Properties) needs no container name, so this works for a
    // sink-only connection that never declares one -- unlike a per-dataset probe, which only source
    // connections with declared Datasets get from ConnectivityValidator (src/Pz.Engine/Validation/
    // ConnectivityValidator.cs). Same (bool Ok, string Message) convention as Postgres/SqlServer's own
    // CheckConnectionAsync: transience folded into the message tag since ConnectionCheck carries no
    // separate field. A malformed config (bad connection string/URI) still throws PzConnectorException
    // out of AzureAuth.CreateBlobServiceClient -- that is a config-shape error, not a connectivity
    // outcome, so it is deliberately NOT caught here and propagates to ConnectivityValidator's own
    // generic catch.
    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        try
        {
            await AzureAuth.CreateBlobServiceClient(config).GetPropertiesAsync(ct).ConfigureAwait(false);
            return new ConnectionCheck(true);
        }
        catch (Exception ex) when (ex is Azure.RequestFailedException or IOException or TimeoutException
            or System.Net.Sockets.SocketException)
        {
            // Same exception domain AzureTransient itself classifies -- a genuinely unreachable host can
            // surface as a raw network exception before the SDK ever produces a RequestFailedException.
            return new ConnectionCheck(false, $"{(AzureTransient.IsTransient(ex) ? "transient" : "permanent")}: {ex.Message}");
        }
    }

    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new AzureSource(config));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new AzureSink(config));
}
