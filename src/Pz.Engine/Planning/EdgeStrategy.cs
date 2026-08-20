namespace Pz.Engine.Planning;

/// <summary>How one DAG edge moves data. Chosen once per invocation by <see cref="ExecutionPlanner"/>.</summary>
public enum EdgeStrategy
{
    /// <summary>DuckDB reads the source directly via a connector-provided SQL fragment.</summary>
    NativeScan,
    /// <summary>DuckDB writes the destination directly via a connector-provided COPY.</summary>
    NativeCopy,
    /// <summary>The universal batch path: connector streams Arrow batches through the engine.</summary>
    ArrowStream,
    /// <summary>Executes inside DuckDB (pipelines, checks).</summary>
    DuckSql,
}
