using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Sqlite;

/// <summary>Native-only SQLite sink: every output is DuckDB SQL against the rw attach —
/// append is a CREATE-IF-NOT-EXISTS + INSERT batch (a first run needs no pre-created table; attaching
/// a missing file is valid sqlite semantics and creates it on first write), replace is a single
/// CREATE OR REPLACE TABLE … AS. <see cref="BeginWriteAsync"/> always throws (the
/// <see cref="INativeOnlySink"/> contract, S3/MySql precedent). DATE/TIMESTAMP/BOOLEAN/DECIMAL
/// columns are stored as TEXT/TEXT/BIGINT/TEXT by the extension (SQLite has no such storage classes)
/// — values round-trip losslessly as text, only the declared type flattens; documented in the
/// connector README. The sqlite-side OR REPLACE is drop+create, so the Transactional
/// capability is deliberately not declared.</summary>
internal sealed class SqliteSink(ConnectorConfig config) : ISink
{
    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        var alias = SqliteSql.SinkAlias(spec.Sink);
        var table = $"{alias}.{SqliteSql.QuoteIdent(spec.Output)}";
        (string Sql, string Mechanism)? statement = spec.Mode switch
        {
            "append" =>
                ($"create table if not exists {table} as select * from {{{{source}}}} limit 0;\n" +
                 $"insert into {table} select * from {{{{source}}}};", "sqlite insert"),
            "replace" =>
                ($"create or replace table {table} as select * from {{{{source}}}}", "sqlite create-or-replace"),
            _ => null, // merge (and anything future) has no native shape; the planner's PZ0324 owns the error
        };

        if (statement is not { } s)
        {
            copy = null;
            return false;
        }

        copy = new NativeCopy(s.Sql, SqliteSql.SinkSetupStatements(SqliteSql.ResolvePath(config), alias))
        {
            Mechanism = s.Mechanism,
        };
        return true;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: output '{spec.Output}': sqlite writes are native-copy only; they cannot run on the " +
            "universal tier (remove engine.force_universal)", isTransient: false);

    public ValueTask DisposeAsync() => default;
}
