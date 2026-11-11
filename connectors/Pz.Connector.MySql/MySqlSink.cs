using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Shared.Sql;

namespace Pz.Connector.MySql;

/// <summary>Native-only MySQL sink: every output is DuckDB SQL against the rw attach —
/// append is a CREATE-IF-NOT-EXISTS + INSERT batch (first run needs no pre-created table), replace is
/// a single CREATE OR REPLACE TABLE … AS. <see cref="BeginWriteAsync"/> always throws (the
/// <see cref="INativeOnlySink"/> contract). The MySQL-side replace swap is NOT
/// atomic (the extension's OR REPLACE is drop+create and MySQL DDL commits implicitly) — documented
/// in the connector README; the Transactional capability is deliberately not declared.</summary>
internal sealed class MySqlSink(ConnectorConfig config) : ISink
{
    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        var alias = MySqlSql.SinkAlias(spec.Sink);
        // DuckDB parses this statement, not MySQL, and DuckDB has no backtick-quoted identifiers: the
        // table reference takes DuckDB's double quotes, unlike MySqlSql.QuoteIdent's backticks for SQL
        // that rides inside mysql_query('alias', '…') and reaches MySQL itself.
        var table = $"{alias}.{DuckDbSqlText.QuoteIdentifier(spec.Output)}";
        (string Sql, string Mechanism)? statement = spec.Mode switch
        {
            "append" =>
                ($"create table if not exists {table} as select * from {{{{source}}}} limit 0;\n" +
                 $"insert into {table} by name select * from {{{{source}}}};", "mysql insert"),
            "replace" =>
                ($"create or replace table {table} as select * from {{{{source}}}}", "mysql create-or-replace"),
            _ => null, // merge (and anything future) has no native shape; the planner's PZ0324 owns the error
        };

        if (statement is not { } s)
        {
            copy = null;
            return false;
        }

        copy = new NativeCopy(s.Sql, MySqlSql.SetupStatements(config, alias, readOnly: false))
        {
            Mechanism = s.Mechanism,
        };
        return true;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: output '{spec.Output}': mysql writes are native-copy only; they cannot run on the " +
            "universal tier (remove engine.force_universal)", isTransient: false, code: PzConnectorErrorCodes.Config);

    public ValueTask DisposeAsync() => default;
}
