using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Iceberg;

/// <summary>Native-only sink over an attached catalog: append is CREATE-IF-NOT-EXISTS + INSERT,
/// replace is DELETE + INSERT in one transaction (two snapshots, a delete then an append, landing
/// together or not at all -- see <see cref="IcebergSql.TryCopySql"/>), merge is
/// CREATE-IF-NOT-EXISTS + MERGE INTO on the declared keys. A <c>files</c> connection is read-only:
/// only a catalog can commit new table metadata.</summary>
internal sealed class IcebergSink(ConnectorConfig config) : ISink
{
    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        if (IcebergCatalog.Of(config) == IcebergCatalog.Files)
        {
            throw new PzConnectorException(
                $"output '{spec.Output}': catalog 'files' is read-only -- writing an Iceberg table needs a rest, glue " +
                "or s3_tables catalog to commit to", isTransient: false);
        }

        var alias = IcebergSql.Alias(spec.Sink);
        if (!IcebergSql.TryCopySql(alias, spec.Output, spec.Mode, spec.Keys, out var sql, out var mechanism))
        {
            copy = null; // the planner's PZ0324 owns the error
            return false;
        }

        copy = new NativeCopy(sql, IcebergSql.SetupStatements(config, alias)) { Mechanism = mechanism };
        return true;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: output '{spec.Output}': iceberg writes are native-copy only; they cannot run on the " +
            "universal tier (remove engine.force_universal)", isTransient: false);

    public ValueTask DisposeAsync() => default;
}
