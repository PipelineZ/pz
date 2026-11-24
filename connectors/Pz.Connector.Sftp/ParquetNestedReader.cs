using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using Pz.Connectors.Abstractions;
using ArrowField = Apache.Arrow.Field;
using ParquetField = Parquet.Schema.Field;

namespace Pz.Connector.Sftp;

/// <summary>Reads a parquet file whose columns include lists, structs or maps: each top-level column's value per
/// row is rebuilt from its leaf columns' definition and repetition levels (Dremel record assembly) into the boxed
/// shapes <c>ArrowBatchBuilder</c> takes (a list as <see cref="List{T}"/>, a struct as an <c>object?[]</c> in
/// field order, a map as an <see cref="OrderedDictionary{TKey,TValue}"/>).
///
/// <para>Levels, per Parquet.Net's schema: a leaf's value is present at its max definition level. A struct is
/// present when the level reaches its own max. A list or map at max definition level <c>d</c> is null below
/// <c>d - 1</c>, empty at <c>d - 1</c>, and has elements from <c>d</c>; an element after the first repeats at the
/// list's max repetition level. Every leaf under a null or empty node records exactly one entry for it, but a
/// writer may add continuation entries there too (DuckDB writes a null fixed-size array as one entry per slot),
/// so skipping a node also skips the entries that repeat inside it.</para></summary>
internal static class ParquetNestedReader
{
    /// <summary>The Arrow type for a top-level parquet column; <paramref name="leafType"/> maps a leaf.</summary>
    public static IArrowType ArrowType(ParquetField field, Func<DataField, IArrowType> leafType) => field switch
    {
        DataField leaf => leafType(leaf),
        ListField list => new ListType(new ArrowField("element", ArrowType(list.Item, leafType), nullable: true)),
        MapField map => new MapType(
            new ArrowField("key", ArrowType(map.Key, leafType), nullable: false),
            new ArrowField("value", ArrowType(map.Value, leafType), nullable: true)),
        StructField @struct => new StructType(
            @struct.Fields.Select(f => new ArrowField(f.Name, ArrowType(f, leafType), nullable: true)).ToList()),
        _ => throw new NotSupportedException($"parquet field '{field.Name}' has an unknown shape ({field.GetType().Name})"),
    };

    /// <summary>One row group's values for <paramref name="fields"/>: <c>result[column][row]</c>.
    /// <paramref name="convert"/> turns a leaf's raw values into the boxed values the builder takes.</summary>
    public static async Task<object?[][]> ReadRowGroupAsync(ParquetRowGroupReader group, IReadOnlyList<ParquetField> fields,
        Func<DataField, RawColumnData, int, object?[]> convert, CancellationToken ct)
    {
        var rows = checked((int)group.RowCount);
        var columns = new object?[fields.Count][];
        for (var c = 0; c < fields.Count; c++)
        {
            var leaves = new Dictionary<DataField, Leaf>();
            foreach (var leaf in Leaves(fields[c]))
            {
                using var raw = await group.ReadRawColumnDataBaseAsync(leaf, ct).ConfigureAwait(false);
                var definitions = leaf.MaxDefinitionLevel > 0 ? raw.DefinitionLevels.ToArray() : null;
                var repetitions = leaf.MaxRepetitionLevel > 0 ? raw.RepetitionLevels.ToArray() : null;
                var present = definitions is null ? rows : definitions.Count(d => d == leaf.MaxDefinitionLevel);
                leaves[leaf] = new Leaf(leaf, definitions, repetitions, convert(leaf, raw, present));
            }

            var values = new object?[rows];
            for (var r = 0; r < rows; r++)
            {
                values[r] = Read(fields[c], leaves);
            }

            columns[c] = values;
        }

        return columns;
    }

    private static IEnumerable<DataField> Leaves(ParquetField field) => field switch
    {
        DataField leaf => [leaf],
        ListField list => Leaves(list.Item),
        MapField map => Leaves(map.Key).Concat(Leaves(map.Value)),
        StructField @struct => @struct.Fields.SelectMany(Leaves),
        _ => [],
    };

    private static object? Read(ParquetField field, Dictionary<DataField, Leaf> leaves)
    {
        switch (field)
        {
            case DataField data:
                return leaves[data].Next();

            case StructField @struct:
            {
                if (First(@struct, leaves).Definition < @struct.MaxDefinitionLevel)
                {
                    Skip(@struct, @struct.MaxRepetitionLevel, leaves);
                    return null;
                }

                var values = new object?[@struct.Fields.Count];
                for (var i = 0; i < values.Length; i++)
                {
                    values[i] = Read(@struct.Fields[i], leaves);
                }

                return values;
            }

            case ListField or MapField:
            {
                var first = First(field, leaves);
                var definition = first.Definition;
                if (definition < field.MaxDefinitionLevel)
                {
                    Skip(field, field.MaxRepetitionLevel - 1, leaves);
                    return definition < field.MaxDefinitionLevel - 1 ? null : field is MapField ? new OrderedDictionary<object, object?>() : new List<object?>();
                }

                if (field is ListField list)
                {
                    var items = new List<object?>();
                    do
                    {
                        items.Add(Read(list.Item, leaves));
                    }
                    while (first.Repeats(field.MaxRepetitionLevel));

                    return items;
                }

                var map = (MapField)field;
                var entries = new OrderedDictionary<object, object?>();
                do
                {
                    var key = Read(map.Key, leaves) ?? throw new InvalidDataException($"map '{field.Name}' has a null key");
                    entries[key] = Read(map.Value, leaves);
                }
                while (first.Repeats(field.MaxRepetitionLevel));

                return entries;
            }

            default:
                throw new NotSupportedException($"parquet field '{field.Name}' has an unknown shape ({field.GetType().Name})");
        }
    }

    private static Leaf First(ParquetField field, Dictionary<DataField, Leaf> leaves) => leaves[Leaves(field).First()];

    /// <summary>Consumes one null or empty instance of <paramref name="field"/> from every leaf under it, plus any
    /// entries repeating inside it (repetition level above <paramref name="context"/>).</summary>
    private static void Skip(ParquetField field, int context, Dictionary<DataField, Leaf> leaves)
    {
        foreach (var leaf in Leaves(field))
        {
            leaves[leaf].Skip(context);
        }
    }

    /// <summary>One leaf column's levels and its present values, packed, with read cursors.</summary>
    private sealed class Leaf(DataField column, int[]? definitions, int[]? repetitions, object?[] values)
    {
        private int _entry;
        private int _value;

        public int Definition => definitions is null ? column.MaxDefinitionLevel : definitions[_entry];

        /// <summary>The leaf's value at the current entry (null below its max definition level), advancing.</summary>
        public object? Next()
        {
            var present = Definition == column.MaxDefinitionLevel;
            _entry++;
            return present ? values[_value++] : null;
        }

        /// <summary>True when the current entry continues a list at <paramref name="level"/>.</summary>
        public bool Repeats(int level) =>
            repetitions is not null && _entry < repetitions.Length && repetitions[_entry] == level;

        public void Skip(int context)
        {
            if (Definition == column.MaxDefinitionLevel)
            {
                _value++;
            }

            _entry++;
            while (repetitions is not null && _entry < repetitions.Length && repetitions[_entry] > context)
            {
                if (Definition == column.MaxDefinitionLevel)
                {
                    _value++;
                }

                _entry++;
            }
        }
    }
}
