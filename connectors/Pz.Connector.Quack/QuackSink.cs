using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Quack;

/// <summary>Native-only sink: append is CREATE-IF-NOT-EXISTS + INSERT, replace is CREATE OR REPLACE
/// TABLE … AS, merge ships the batch into a scratch table on the server and runs one MERGE INTO there,
/// inside one transaction; all executed by the remote server. Commit semantics belong to the server,
/// so Transactional is not declared.</summary>
internal sealed class QuackSink(ConnectorConfig config) : ISink
{
    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        var alias = QuackSql.Alias(spec.Sink);
        var table = QuackSql.QualifiedTable(alias, spec.Output);
        if (!QuackSql.TryCopySql(alias, table, spec.Mode, spec.Keys, out var sql, out var mechanism))
        {
            copy = null; // the planner's PZ0324 owns the error
            return false;
        }

        copy = new NativeCopy(sql, QuackSql.SetupStatements(config, alias)) { Mechanism = mechanism };
        return true;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: output '{spec.Output}': quack writes are native-copy only; they cannot run on the universal tier (remove engine.force_universal)",
            isTransient: false, code: PzConnectorErrorCodes.Config);

    public ValueTask DisposeAsync() => default;
}
