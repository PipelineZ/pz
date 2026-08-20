namespace Pz.DuckDb;

/// <summary>
/// A connection to a single DuckDB database. The seam the whole engine consumes.
/// </summary>
public interface IDuckSession : IAsyncDisposable
{
    Task ExecuteAsync(string sql, CancellationToken ct = default);

    Task<T> ScalarAsync<T>(string sql, CancellationToken ct = default);

    /// <summary>Creates <paramref name="targetTable"/> (qualified name, e.g. "staging.src_a__b") from the
    /// Arrow stream, consuming batches as they arrive (streaming — never buffers the full input) and
    /// disposing each batch after it is consumed. Returns rows ingested.
    /// On failure or cancellation, the target table is removed; a successful return is the only way the
    /// table exists.</summary>
    Task<long> IngestArrowAsync(string targetTable, Apache.Arrow.Schema schema,
        IAsyncEnumerable<Apache.Arrow.RecordBatch> batches, CancellationToken ct = default);

    /// <summary>Streams the query result as Arrow batches shaped by <paramref name="targetBatchBytes"/>.
    /// Batches are caller-owned (caller disposes). Enumeration honors <paramref name="ct"/> between batches.</summary>
    IAsyncEnumerable<Apache.Arrow.RecordBatch> QueryArrowAsync(string sql, int targetBatchBytes = 32 * 1024 * 1024,
        CancellationToken ct = default);

    /// <summary>The Arrow schema the query's result will carry (executed with LIMIT 0 semantics — cheap).</summary>
    Task<Apache.Arrow.Schema> GetResultSchemaAsync(string sql, CancellationToken ct = default);

    /// <summary>Creates <paramref name="targetTable"/> empty from the Arrow schema.
    /// Unlike <see cref="IngestArrowAsync"/> the table's lifecycle is the CALLER's — it survives
    /// later failures; nothing here drops it.</summary>
    Task CreateEmptyTableAsync(string targetTable, Apache.Arrow.Schema schema, CancellationToken ct = default);

    /// <summary>Appends ONE batch to an existing table. Acquires the connection gate
    /// per call (a fresh native appender per call, autocommit), so concurrent callers interleave at
    /// batch granularity instead of serializing whole streams. Disposes <paramref name="batch"/>
    /// before returning, success or failure — same ownership rule as <see cref="IngestArrowAsync"/>.
    /// Returns rows appended. Note: appender errors may surface at close, i.e. still inside this
    /// call — never later.</summary>
    Task<long> AppendArrowBatchAsync(string targetTable, Apache.Arrow.RecordBatch batch, CancellationToken ct = default);

    /// <summary>Runs <paramref name="statements"/> (each a single statement) as one BEGIN…COMMIT
    /// transaction under a single connection-gate hold (the run's one shared DuckDB connection is
    /// serialized per statement, so a transactional sequence must own the gate for its whole,
    /// deliberately short, duration). Any
    /// failure ROLLBACKs (best-effort) and rethrows.</summary>
    Task ExecuteTransactionAsync(IReadOnlyList<string> statements, CancellationToken ct = default);
}
