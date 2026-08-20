using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Pz.Core.Validation;
using Pz.Engine.State;

namespace Pz.State.SqlServer;

/// <summary>The SQL Server implementation of
/// <see cref="IKeyedStateStore{T}"/>, backed by <c>{schema}.state (scope, state_key, payload, version,
/// updated_at)</c> (see <see cref="SqlStateSchema"/>).
///
/// **Optimistic concurrency lives here, not on the interface.** This instance remembers, per key, the
/// `version` the last <see cref="Get"/> read, in a private dictionary -- the concurrency token
/// deliberately kept off <see cref="IKeyedStateStore{T}"/> because it matches the run's real access
/// pattern: `Get` at plan time, `Set` once at advancement time, one store instance per run. `Set` for a
/// key this instance never read carries no expected version, so it is an insert-if-absent -- which also
/// conflicts (PZ0520) if another writer inserted that key first.
///
/// Every read/write is parameterized; the operator-supplied schema name is quoted via SQL Server's own
/// `QUOTENAME`, evaluated server-side inside dynamic SQL built into a local `@sql` variable first (never
/// interpolated in C#) -- table/column names are our own fixed literals. A key or payload is never
/// concatenated into SQL text.</summary>
public sealed class SqlKeyedStateStore<T>(
    SqlStateConnection connection,
    string scope,
    Func<JsonElement, T?> readEntry,
    Action<Utf8JsonWriter, T> writeEntry) : IKeyedStateStore<T> where T : class
{
    /// <summary>The version each key was last read at, by THIS instance. Populated by <see cref="Get"/>
    /// (even when the payload turns out to be corrupt -- see below) and by a successful <see cref="Set"/>
    /// (which bumps it by one, or seeds it at 1 for a fresh insert), so a later <see cref="Set"/> on the
    /// same key in the same instance always carries the right expected version without a re-read.
    ///
    /// **Concurrent, not plain.** One store instance serves a whole run (<c>StateBackendFactory.Create</c>),
    /// and <see cref="Get"/> is called per node from the executors while SourceLoad nodes run in parallel
    /// under the topological dispatcher -- so this dictionary is written from several threads at once. A
    /// plain <see cref="Dictionary{TKey,TValue}"/> can corrupt its bucket chain on a concurrent resize
    /// (an infinite loop or an IndexOutOfRangeException inside a later lookup), and even a merely-lost
    /// entry would silently downgrade <see cref="Set"/> from compare-and-swap to insert-if-absent,
    /// dropping the PZ0520 guarantee. <see cref="ListAll"/>'s bulk populate and <see cref="Set"/>'s
    /// read-then-write are both safe under a <see cref="ConcurrentDictionary{TKey,TValue}"/> without
    /// further locking, because <see cref="Set"/> is once-per-key-at-advancement (see the class doc).</summary>
    private readonly ConcurrentDictionary<string, int> _versions = new(StringComparer.Ordinal);

    public T? Get(string key, Action<string>? notice = null)
    {
        using var sqlConnection = connection.Open();
        try
        {
            using var command = new SqlCommand(
                "DECLARE @sql NVARCHAR(MAX) = N'SELECT payload, version FROM ' + QUOTENAME(@schema) + " +
                "N'.state WHERE scope = @scope AND state_key = @key'; " +
                "EXEC sp_executesql @sql, N'@scope NVARCHAR(32), @key NVARCHAR(512)', " +
                "@scope = @scope, @key = @key;",
                sqlConnection);
            command.Parameters.AddWithValue("@schema", connection.Schema);
            command.Parameters.AddWithValue("@scope", scope);
            command.Parameters.AddWithValue("@key", key);

            string payload;
            int version;
            using (var reader = command.ExecuteReader())
            {
                if (!reader.Read())
                {
                    return null;
                }

                payload = reader.GetString(0);
                version = reader.GetInt32(1);
            }

            // Remembered even when the payload below turns out to be corrupt: a Set that follows a
            // corrupt Get must still be able to overwrite the row (mirrors the local backend, which
            // treats a corrupt file as empty and rewrites it -- corrupt state is never an error).
            _versions[key] = version;

            var value = ParsePayload(payload);
            if (value is null)
            {
                notice?.Invoke(
                    $"state entry '{key}' (scope '{scope}') is corrupt or has an unexpected shape -- a full extract will occur.");
                return null;
            }

            return value;
        }
        catch (Exception ex) when (ex is SqlException or InvalidOperationException)
        {
            throw connection.Unavailable(ex);
        }
    }

    public IReadOnlyList<KeyValuePair<string, T>>? ListAll(Action<string>? notice = null)
    {
        using var sqlConnection = connection.Open();
        try
        {
            using var command = new SqlCommand(
                "DECLARE @sql NVARCHAR(MAX) = N'SELECT state_key, payload, version FROM ' + " +
                "QUOTENAME(@schema) + N'.state WHERE scope = @scope'; " +
                "EXEC sp_executesql @sql, N'@scope NVARCHAR(32)', @scope = @scope;",
                sqlConnection);
            command.Parameters.AddWithValue("@schema", connection.Schema);
            command.Parameters.AddWithValue("@scope", scope);

            var rows = new List<(string Key, string Payload, int Version)>();
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    rows.Add((reader.GetString(0), reader.GetString(1), reader.GetInt32(2)));
                }
            }

            var results = new List<KeyValuePair<string, T>>();
            foreach (var (key, payload, version) in rows)
            {
                _versions[key] = version;

                var value = ParsePayload(payload);
                if (value is null)
                {
                    notice?.Invoke(
                        $"state (scope '{scope}') contains a corrupt or unexpected entry for key '{key}' -- a full extract will occur.");
                    return null;
                }

                results.Add(new(key, value));
            }

            // Sorted here rather than via ORDER BY: SQL Server's default collation is
            // case-insensitive, which would not reproduce the ordinal order the contract (and
            // KeyedJsonStateStore) guarantee.
            return results.OrderBy(kv => kv.Key, StringComparer.Ordinal).ToList();
        }
        catch (Exception ex) when (ex is SqlException or InvalidOperationException)
        {
            throw connection.Unavailable(ex);
        }
    }

    public void Set(string key, T value)
    {
        using var sqlConnection = connection.Open();
        var payload = SerializePayload(value);
        var now = DateTime.UtcNow;

        try
        {
            if (_versions.TryGetValue(key, out var expectedVersion))
            {
                var rows = ExecuteUpdate(sqlConnection, key, payload, expectedVersion, now);
                if (rows == 0)
                {
                    throw Conflict(key);
                }

                _versions[key] = expectedVersion + 1;
                return;
            }

            var inserted = ExecuteInsertIfAbsent(sqlConnection, key, payload, now);
            if (!inserted)
            {
                throw Conflict(key);
            }

            _versions[key] = 1;
        }
        catch (SqlException ex) when (ex.Number is 2627 or 2601)
        {
            // A genuine concurrent insert can race past the WHERE NOT EXISTS guard below and hit the
            // primary key (scope, state_key) instead -- same conflict, reported the same way.
            throw Conflict(key);
        }
        catch (Exception ex) when (ex is SqlException or InvalidOperationException)
        {
            throw connection.Unavailable(ex);
        }
    }

    public void Remove(string key)
    {
        using var sqlConnection = connection.Open();
        try
        {
            using var command = new SqlCommand(
                "DECLARE @sql NVARCHAR(MAX) = N'DELETE FROM ' + QUOTENAME(@schema) + " +
                "N'.state WHERE scope = @scope AND state_key = @key'; " +
                "EXEC sp_executesql @sql, N'@scope NVARCHAR(32), @key NVARCHAR(512)', " +
                "@scope = @scope, @key = @key;",
                sqlConnection);
            command.Parameters.AddWithValue("@schema", connection.Schema);
            command.Parameters.AddWithValue("@scope", scope);
            command.Parameters.AddWithValue("@key", key);
            command.ExecuteNonQuery();
        }
        catch (Exception ex) when (ex is SqlException or InvalidOperationException)
        {
            throw connection.Unavailable(ex);
        }

        _versions.TryRemove(key, out _);
    }

    private int ExecuteUpdate(SqlConnection sqlConnection, string key, string payload, int expectedVersion, DateTime now)
    {
        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = N'UPDATE ' + QUOTENAME(@schema) + N'.state SET payload = @payload, " +
            "version = version + 1, updated_at = @now WHERE scope = @scope AND state_key = @key AND " +
            "version = @expected; SELECT @@ROWCOUNT;'; " +
            "EXEC sp_executesql @sql, " +
            "N'@payload NVARCHAR(MAX), @now DATETIME2, @scope NVARCHAR(32), @key NVARCHAR(512), @expected INT', " +
            "@payload = @payload, @now = @now, @scope = @scope, @key = @key, @expected = @expected;",
            sqlConnection);
        command.Parameters.AddWithValue("@schema", connection.Schema);
        command.Parameters.Add("@payload", System.Data.SqlDbType.NVarChar, -1).Value = payload;
        command.Parameters.AddWithValue("@now", now);
        command.Parameters.AddWithValue("@scope", scope);
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@expected", expectedVersion);
        return (int)command.ExecuteScalar()!;
    }

    private bool ExecuteInsertIfAbsent(SqlConnection sqlConnection, string key, string payload, DateTime now)
    {
        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = N'INSERT INTO ' + QUOTENAME(@schema) + N'.state " +
            "(scope, state_key, payload, version, updated_at) " +
            "SELECT @scope, @key, @payload, 1, @now WHERE NOT EXISTS " +
            "(SELECT 1 FROM ' + QUOTENAME(@schema) + N'.state WHERE scope = @scope AND state_key = @key); " +
            "SELECT @@ROWCOUNT;'; " +
            "EXEC sp_executesql @sql, " +
            "N'@scope NVARCHAR(32), @key NVARCHAR(512), @payload NVARCHAR(MAX), @now DATETIME2', " +
            "@scope = @scope, @key = @key, @payload = @payload, @now = @now;",
            sqlConnection);
        command.Parameters.AddWithValue("@schema", connection.Schema);
        command.Parameters.AddWithValue("@scope", scope);
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.Add("@payload", System.Data.SqlDbType.NVarChar, -1).Value = payload;
        command.Parameters.AddWithValue("@now", now);
        var rows = (int)command.ExecuteScalar()!;
        return rows > 0;
    }

    private PzConfigException Conflict(string key) =>
        new(new PzError(PzErrorCode.StateConcurrencyConflict,
            $"state key '{key}' (scope '{scope}') was advanced by another run while this run was executing.",
            "project.yml", null,
            "re-run; if concurrent runs over the same datasets are intended, split them by dataset"));

    private T? ParsePayload(string payload)
    {
        try
        {
            using var document = JsonDocument.Parse(payload);
            return readEntry(document.RootElement);
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or KeyNotFoundException)
        {
            return null;
        }
    }

    private string SerializePayload(T value)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writeEntry(writer, value);
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }
}
