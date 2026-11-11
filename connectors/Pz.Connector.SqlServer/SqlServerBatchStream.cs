using System.Runtime.CompilerServices;
using Apache.Arrow;
using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;
using Pz.Shared;

namespace Pz.Connector.SqlServer;

/// <summary>Drains an open <see cref="SqlDataReader"/> into Arrow batches and owns the reader from here
/// on (disposed on every exit path, including abandoned enumeration). CS1626 forbids a
/// <c>yield return</c> inside a try/catch, but not one that merely follows it, so the inner enumerator is
/// driven by hand: only <c>MoveNextAsync</c> sits inside the catch, which lets a mid-stream
/// <see cref="SqlException"/> surface as the classified exception <c>wrap</c> builds.</summary>
internal static class SqlServerBatchStream
{
    public static async IAsyncEnumerable<RecordBatch> ReadAsync(
        SqlDataReader reader, BatchOptions options, Func<SqlException, PzConnectorException> wrap,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var enumerator = SqlServerArrowReader.ReadBatchesAsync(reader, options, ct).GetAsyncEnumerator(ct);
        try
        {
            while (true)
            {
                bool moved;
                try
                {
                    moved = await enumerator.MoveNextAsync().ConfigureAwait(false);
                }
                catch (SqlException ex)
                {
                    throw wrap(ex);
                }

                if (!moved)
                {
                    yield break;
                }

                yield return enumerator.Current;
            }
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
            await reader.DisposeAsync().ConfigureAwait(false);
        }
    }

    public static PzConnectorException MidStreamFailure(SqlException ex) =>
        new($"sqlserver read failed mid-stream: {ex.Message}", MsTransient.IsTransient(ex), innerException: ex);
}
