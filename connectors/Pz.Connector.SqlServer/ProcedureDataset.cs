using System.Data;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.SqlServer;

/// <summary>Stored-procedure dataset mode: `procedure:` + `parameters:` is the third
/// dataset mode alongside `table`/`query`. Execution is always <see cref="CommandType.StoredProcedure"/>
/// with typed <see cref="SqlParameter"/>s -- there is no SQL text assembly, so the proc name validation
/// below is defense-in-depth, not the injection guard (RPC invocation already can't concatenate SQL
/// text). The sentinel parameter values "$watermark" and "$watermark_upper" bind the engine's canonical
/// cursor value / window upper bound; both are DBNull when unset, which is always true for a
/// planning-time schema probe (the planner never carries a watermark) -- procs must treat a NULL bound
/// as unbounded. The connector applies no additional WHERE for proc
/// datasets: the proc itself is the pushdown, and universal-tier over-extraction is engine-safe (merge
/// dedups; staging trim caps the window). Schema comes from a SchemaOnly probe of the proc command, or
/// -- for procs FMTONLY cannot describe (e.g. #temp-staging procs) -- from the
/// dataset's declared `columns:` contract, verified against the actual result schema at read time
/// (<see cref="VerifySchema"/>). There is no escape from the sentinel: a `parameters:` value that is
/// literally the string "$watermark" or "$watermark_upper" is always bound as the watermark cursor or
/// window upper bound, never as that literal string.</summary>
internal static class ProcedureDataset
{
    private static readonly Regex ValidProcedureName = new(@"^[A-Za-z0-9_\[\]\. ]+$", RegexOptions.Compiled);

    private static readonly IReadOnlyDictionary<string, IArrowType> ContractTypeMap =
        new Dictionary<string, IArrowType>(StringComparer.Ordinal)
        {
            ["int"] = Int32Type.Default,
            ["bigint"] = Int64Type.Default,
            ["double"] = DoubleType.Default,
            ["decimal"] = new Decimal128Type(38, 9),
            ["varchar"] = StringType.Default,
            ["boolean"] = BooleanType.Default,
            ["date"] = Date32Type.Default,
            ["timestamp"] = new TimestampType(TimeUnit.Microsecond, "+00:00"),
        };

    public static bool IsProcedure(DatasetSpec spec) =>
        spec.Options.TryGetValue("procedure", out var value) && value is not null;

    /// <summary>Builds the RPC command: CommandType.StoredProcedure, one typed SqlParameter per entry in
    /// `parameters:`. "$watermark"/"$watermark_upper" sentinel values bind the spec's watermark fields
    /// (DBNull when null); every other value binds as a literal via the SqlParameter(name, value)
    /// constructor (SqlClient infers SqlDbType from the CLR type).</summary>
    public static SqlCommand BuildCommand(SqlConnection connection, DatasetSpec spec)
    {
        var name = GetValidatedProcedureName(spec);
        var command = new SqlCommand(name, connection) { CommandType = CommandType.StoredProcedure };
        foreach (var (key, value) in GetParameters(spec))
        {
            command.Parameters.Add(BuildParameter(key, value, spec));
        }

        return command;
    }

    /// <summary>Builds the Arrow schema from a declared `columns:` contract, bypassing any server probe
    /// entirely. Returns null when the dataset declares no `columns:` contract (the caller should fall
    /// back to a SchemaOnly probe in that case). Unknown type name -&gt; non-transient error naming the
    /// column and the valid set.</summary>
    public static Schema? BuildContractSchema(DatasetSpec spec)
    {
        var columns = ExtractColumnsContract(spec);
        if (columns is null)
        {
            return null;
        }

        var fields = new List<Field>(columns.Count);
        foreach (var (column, typeName) in columns)
        {
            if (!ContractTypeMap.TryGetValue(typeName, out var arrowType))
            {
                throw new PzConnectorException(
                    $"dataset '{spec.Dataset}': column '{column}' has unknown columns: type '{typeName}' -- " +
                    $"valid types are {string.Join(", ", ContractTypeMap.Keys)}",
                    isTransient: false);
            }

            fields.Add(new Field(column, arrowType, nullable: true));
        }

        return new Schema(fields, null);
    }

    /// <summary>Verifies the actual reader schema (as SqlServerArrowReader.BuildSchema would build it)
    /// matches a declared `columns:` contract field-by-field (name, order, Arrow type) -- the ABI
    /// requires every batch to carry the promised schema exactly. Throws naming the first mismatched
    /// column plus the full expected/actual lists.</summary>
    public static void VerifySchema(DatasetSpec spec, Schema contract, Schema actual)
    {
        if (contract.FieldsList.Count != actual.FieldsList.Count)
        {
            throw Mismatch(spec, contract, actual,
                $"expected {contract.FieldsList.Count} column(s), got {actual.FieldsList.Count}");
        }

        for (var i = 0; i < contract.FieldsList.Count; i++)
        {
            var expected = contract.FieldsList[i];
            var got = actual.FieldsList[i];
            if (!string.Equals(expected.Name, got.Name, StringComparison.Ordinal) ||
                !TypesEqual(expected.DataType, got.DataType))
            {
                throw Mismatch(spec, contract, actual,
                    $"column '{expected.Name}': declared columns: type maps to Arrow {expected.DataType}, but " +
                    $"the actual result column at position {i} is '{got.Name}' with type {got.DataType}");
            }
        }
    }

    private static PzConnectorException Mismatch(DatasetSpec spec, Schema expected, Schema actual, string detail) =>
        new(
            $"dataset '{spec.Dataset}': procedure result schema does not match the declared columns: contract " +
            $"-- {detail} (expected [{Describe(expected)}], actual [{Describe(actual)}])",
            isTransient: false);

    private static string Describe(Schema schema) =>
        string.Join(", ", schema.FieldsList.Select(f => $"{f.Name}:{f.DataType}"));

    private static bool TypesEqual(IArrowType expected, IArrowType actual)
    {
        if (expected.TypeId != actual.TypeId)
        {
            return false;
        }

        return (expected, actual) switch
        {
            (Decimal128Type e, Decimal128Type a) => e.Precision == a.Precision && e.Scale == a.Scale,
            (TimestampType e, TimestampType a) => e.Unit == a.Unit && string.Equals(e.Timezone, a.Timezone, StringComparison.Ordinal),
            _ => true,
        };
    }

    private static string GetValidatedProcedureName(DatasetSpec spec)
    {
        var name = spec.Options.TryGetValue("procedure", out var raw) ? raw?.ToString() : null;
        // The allowed character class already excludes ';' -- the explicit check below restates that as
        // defense-in-depth, since CommandType.StoredProcedure means this string is RPC-invoked
        // (sp_executesql-style batch concatenation never happens), not because the regex could miss it.
        if (string.IsNullOrWhiteSpace(name) || !ValidProcedureName.IsMatch(name) || name.Contains(';', StringComparison.Ordinal))
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': invalid procedure name '{name}' -- hint: use a schema-qualified " +
                "T-SQL identifier (letters, digits, underscore, brackets, dots, spaces only)",
                isTransient: false);
        }

        return name;
    }

    private static SqlParameter BuildParameter(string key, object? value, DatasetSpec spec)
    {
        var name = $"@{key}";
        return value switch
        {
            "$watermark" => new SqlParameter(name, (object?)spec.WatermarkValue ?? DBNull.Value),
            "$watermark_upper" => new SqlParameter(name, (object?)spec.WatermarkUpperBound ?? DBNull.Value),
            null => new SqlParameter(name, DBNull.Value),
            _ => new SqlParameter(name, value),
        };
    }

    /// <summary>`parameters:` arrives as an <see cref="IReadOnlyDictionary{TKey,TValue}"/> of
    /// <c>string, object?</c> through the engine's normal YAML-loaded path; offline unit tests sometimes
    /// hand-build the same shape directly, so accepting any <see cref="IEnumerable{T}"/> of that KVP shape
    /// is a defensive widening for the production shape, not a real fallback. A `parameters:` value that
    /// isn't a mapping at all (e.g. a scalar) is a config error, not something to silently drop -- for a
    /// `$watermark`-bound proc, silently binding zero parameters means silent full re-extraction, which
    /// violates the no-silent-failures rule.</summary>
    private static IEnumerable<KeyValuePair<string, object?>> GetParameters(DatasetSpec spec)
    {
        if (!spec.Options.TryGetValue("parameters", out var raw) || raw is null)
        {
            return [];
        }

        if (raw is IEnumerable<KeyValuePair<string, object?>> kvps)
        {
            return kvps;
        }

        throw new PzConnectorException(
            $"dataset '{spec.Dataset}': 'parameters' must be a mapping of parameter names to scalar values",
            isTransient: false);
    }

    /// <summary>`columns:` can arrive either as <see cref="IReadOnlyDictionary{TKey,TValue}"/> of
    /// <c>string, string</c> (the shape <c>SpecBuilder</c> merges in from <c>DatasetDef.Columns</c> on the
    /// real engine path) or of <c>string, object?</c> (the shape a hand-built test spec or a raw
    /// JSON-schema-validated options bag might carry) -- parsed defensively so either shape works.</summary>
    private static IReadOnlyDictionary<string, string>? ExtractColumnsContract(DatasetSpec spec)
    {
        if (!spec.Options.TryGetValue("columns", out var raw) || raw is null)
        {
            return null;
        }

        return raw switch
        {
            IReadOnlyDictionary<string, string> typed => typed,
            IEnumerable<KeyValuePair<string, object?>> kvps =>
                kvps.ToDictionary(kv => kv.Key, kv => kv.Value?.ToString() ?? string.Empty),
            _ => null,
        };
    }
}

/// <summary>One (always single) partition for a procedure dataset: opens its own connection, runs the
/// RPC command built by <see cref="ProcedureDataset.BuildCommand"/>, and streams via
/// <see cref="SqlServerArrowReader.ReadBatchesAsync"/>. Mirrors <see cref="SqlServerPartition"/>'s
/// manual-enumerator pattern (yield cannot live inside try/catch, so MoveNextAsync is driven by hand):
/// mid-stream SqlExceptions surface classified, and `await using` disposes the connection on every exit
/// path including abandoned enumeration. When the dataset declares a `columns:` contract, the actual
/// reader schema is verified against it before any row is streamed.</summary>
internal sealed class SqlServerProcedurePartition(string connectionString, DatasetSpec spec) : IDatasetPartition
{
    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        SqlDataReader reader;
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            var command = ProcedureDataset.BuildCommand(connection, spec);
            reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, ct).ConfigureAwait(false);
        }
        catch (SqlException ex)
        {
            throw new PzConnectorException($"sqlserver read failed: {ex.Message}", ex.IsTransient, innerException: ex);
        }

        try
        {
            var contract = ProcedureDataset.BuildContractSchema(spec);
            if (contract is not null)
            {
                var actual = SqlServerArrowReader.BuildSchema(reader, $"dataset '{spec.Dataset}'");
                ProcedureDataset.VerifySchema(spec, contract, actual);
            }
        }
        catch
        {
            await reader.DisposeAsync().ConfigureAwait(false);
            throw;
        }

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
                    throw new PzConnectorException(
                        $"sqlserver read failed mid-stream: {ex.Message}", ex.IsTransient, innerException: ex);
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
}
