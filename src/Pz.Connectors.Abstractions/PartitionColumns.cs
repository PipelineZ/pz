using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Pz.Connectors.Abstractions;

/// <summary>Reads an output's <c>partition_by:</c> option into the columns it names — the one place it
/// is parsed, so a connector never invents its own spelling for it and never has to guess what a YAML
/// sequence deserialized into.
///
/// <para>ONE MEANING: <c>partition_by:</c> names the columns an output is partitioned by. What that
/// produces on disk is the destination's business. A store whose layout pz renders — an object store
/// with calendar tokens in its <c>path:</c> — takes exactly one timestamp column and fans rows into the
/// folder those tokens render to (<see cref="ConnectorCapabilities.PathTemplating"/>). A format that
/// records partitioning in its own metadata and lays out the directories itself — Delta, Iceberg,
/// Hive-layout parquet — takes the list as it stands
/// (<see cref="ConnectorCapabilities.ColumnPartitionedWrites"/>).</para>
///
/// <para>Both a scalar and a sequence are accepted, because both read naturally in YAML
/// (<c>partition_by: ts</c>, <c>partition_by: [year, month]</c>). Calling <c>ToString()</c> on the raw
/// option instead is the trap this exists to close: a deserialized list stringifies to its CLR type
/// name, which is a non-empty string, so a presence check passes and everything downstream reads a
/// column named <c>System.Collections.Generic.List`1[System.Object]</c>.</para></summary>
public static class PartitionColumns
{
    public const string OptionName = "partition_by";

    /// <summary>Empty when the option is absent, null, an empty sequence, or blank. Never throws — a
    /// shape that names no column reads as "no partitioning declared", and it is
    /// <c>DagCompiler</c>'s job to refuse a malformed declaration with a coded error before a connector
    /// ever sees it (see <see cref="TryRead"/>).</summary>
    public static IReadOnlyList<string> Read(IReadOnlyDictionary<string, object?> options) =>
        TryRead(options, out var columns, out _) ? columns : [];

    /// <summary>The validating read: false when the option is present but names something other than a
    /// non-empty string or a sequence of non-empty strings, with <paramref name="problem"/> describing
    /// what was found. An absent option is true with no columns — declaring nothing is not an error.</summary>
    public static bool TryRead(
        IReadOnlyDictionary<string, object?> options, out IReadOnlyList<string> columns, out string? problem)
    {
        columns = [];
        problem = null;

        if (!options.TryGetValue(OptionName, out var raw) || raw is null)
        {
            return true;
        }

        if (raw is string scalar)
        {
            if (scalar.Trim().Length == 0)
            {
                problem = "it is empty";
                return false;
            }

            columns = [scalar.Trim()];
            return true;
        }

        // A YAML sequence deserializes to a non-generic IEnumerable of object; string is excluded above
        // so it cannot be walked character by character here.
        if (raw is IEnumerable sequence)
        {
            var names = new List<string>();
            foreach (var item in sequence)
            {
                if (item is not string name || name.Trim().Length == 0)
                {
                    problem = $"it contains an entry that is not a column name ('{item}')";
                    return false;
                }

                names.Add(name.Trim());
            }

            if (names.Count == 0)
            {
                problem = "it is an empty list";
                return false;
            }

            if (names.Distinct(System.StringComparer.Ordinal).Count() != names.Count)
            {
                problem = "it names the same column twice";
                return false;
            }

            columns = names;
            return true;
        }

        problem = $"it is '{raw}', which is neither a column name nor a list of column names";
        return false;
    }
}
