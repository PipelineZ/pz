using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DuckDb;

/// <summary>Native-only sink: every output is DuckDB SQL against the connection's attach — append
/// is CREATE-IF-NOT-EXISTS + INSERT, replace is CREATE OR REPLACE TABLE … AS, merge is
/// CREATE-IF-NOT-EXISTS + MERGE INTO on the declared keys. Each statement commits atomically in
/// the attached file, which is what the Transactional capability promises.
/// <see cref="BeginWriteAsync"/> always throws (the <see cref="INativeOnlySink"/> contract).</summary>
internal sealed class DuckDbSink(ConnectorConfig config) : ISink
{
    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        var alias = DuckDbSql.Alias(spec.Sink);
        var table = DuckDbSql.QualifiedTable(alias, spec.Output);
        if (!DuckDbSql.TryCopySql(table, spec.Mode, spec.Keys, out var sql, out var mechanism))
        {
            copy = null; // no native shape for this mode; the planner's PZ0324 owns the error
            return false;
        }

        copy = new NativeCopy(sql, DuckDbSql.SetupStatements(DuckDbSql.ResolvePath(config), alias))
        {
            Mechanism = mechanism,
        };
        return true;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: output '{spec.Output}': duckdb writes are native-copy only; they cannot run on the " +
            "universal tier (remove engine.force_universal)", isTransient: false);

    public ValueTask DisposeAsync() => default;
}
