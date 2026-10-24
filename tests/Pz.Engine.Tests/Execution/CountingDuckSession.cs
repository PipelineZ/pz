using Apache.Arrow;
using Pz.DuckDb;

namespace Pz.Engine.Tests.Execution;

/// <summary>Wraps a real <see cref="IDuckSession"/> and records the SQL of every scalar and execute
/// statement in order, so a test can assert how many statements a code path issues.</summary>
internal sealed class CountingDuckSession(IDuckSession inner) : IDuckSession
{
    public List<string> Statements { get; } = [];

    public async Task ExecuteAsync(string sql, CancellationToken ct = default)
    {
        Statements.Add(sql);
        await inner.ExecuteAsync(sql, ct).ConfigureAwait(false);
    }

    public Task<T> ScalarAsync<T>(string sql, CancellationToken ct = default)
    {
        Statements.Add(sql);
        return inner.ScalarAsync<T>(sql, ct);
    }

    public Task<long> IngestArrowAsync(string targetTable, Schema schema, IAsyncEnumerable<RecordBatch> batches,
        CancellationToken ct = default) => inner.IngestArrowAsync(targetTable, schema, batches, ct);

    public IAsyncEnumerable<RecordBatch> QueryArrowAsync(string sql, int targetBatchBytes = 32 * 1024 * 1024,
        CancellationToken ct = default) => inner.QueryArrowAsync(sql, targetBatchBytes, ct);

    public Task<Schema> GetResultSchemaAsync(string sql, CancellationToken ct = default) =>
        inner.GetResultSchemaAsync(sql, ct);

    public Task CreateEmptyTableAsync(string targetTable, Schema schema, CancellationToken ct = default) =>
        inner.CreateEmptyTableAsync(targetTable, schema, ct);

    public Task<long> AppendArrowBatchAsync(string targetTable, RecordBatch batch, Schema? schema = null, CancellationToken ct = default) =>
        inner.AppendArrowBatchAsync(targetTable, batch, schema, ct);

    public Task ExecuteTransactionAsync(IReadOnlyList<string> statements, CancellationToken ct = default) =>
        inner.ExecuteTransactionAsync(statements, ct);

    public ValueTask DisposeAsync() => default;
}
