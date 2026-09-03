using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MotherDuck;

/// <summary>Native-only sink: append is CREATE-IF-NOT-EXISTS + INSERT, replace is CREATE OR REPLACE
/// TABLE … AS, merge is a real MERGE INTO against the declared key columns (update on match, insert
/// otherwise) — MotherDuck's engine executes it server-side, so unlike a client-attached remote there
/// is no pull-rewrite-push round trip and no blast radius on the target's constraints or indexes. A
/// keyless merge is refused at compile time (PZ0324); the throw here is ABI defense-in-depth. Commit
/// semantics belong to MotherDuck, so Transactional is not declared.</summary>
internal sealed class MotherDuckSink(ConnectorConfig config) : ISink
{
    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        var table = MotherDuckSql.QualifiedTable(MotherDuckSql.Database(config), spec.Output);
        if (!MotherDuckSql.TryCopySql(table, spec.Mode, spec.Keys, out var sql, out var mechanism))
        {
            copy = null; // the planner's PZ0324 owns the error
            return false;
        }

        copy = new NativeCopy(sql, MotherDuckSql.SetupStatements(config)) { Mechanism = mechanism };
        return true;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: output '{spec.Output}': motherduck writes are native-copy only; they cannot run on the universal tier (remove engine.force_universal)",
            isTransient: false);

    public ValueTask DisposeAsync() => default;
}
