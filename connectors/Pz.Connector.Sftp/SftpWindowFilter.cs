using System.Globalization;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Abstractions.Paths;

namespace Pz.Connector.Sftp;

/// <summary>Row-level bounded-window filter for the universal-tier sftp read path. SSH.NET streams
/// remote files through managed format readers rather than a DuckDB native scan, so there is no
/// FROM fragment to wrap the way <c>S3WindowSql.Wrap</c> does — this applies the same
/// <c>cursor &gt; lo AND cursor &lt;= hi</c> window to each landed row instead, which is what lets a
/// connector with no native tier still declare <see cref="ConnectorCapabilities.BoundedWindow"/>.
/// Activation gate and the value-null invariant guard mirror S3WindowSql.Wrap exactly; only the
/// enforcement mechanism (typed row comparison vs. rendered SQL) differs.</summary>
internal sealed class SftpWindowFilter
{
    private readonly Func<IArrowArray, int, bool>? _rowKept;
    private readonly int _cursorOrdinal;

    /// <summary>True when <see cref="DatasetSpec.WatermarkCursor"/>, <see cref="DatasetSpec.WatermarkValue"/>,
    /// and <see cref="DatasetSpec.WatermarkUpperBound"/> are all present — the only case a windowed
    /// dataset is ever stamped with. When false, <see cref="Filter"/> is a pass-through.</summary>
    public bool IsActive { get; }

    public SftpWindowFilter(DatasetSpec spec, Schema schema, string cursorTypeName)
    {
        ArgumentNullException.ThrowIfNull(spec);
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(cursorTypeName);

        if (spec.WatermarkCursor is null || spec.WatermarkUpperBound is null)
        {
            IsActive = false;
            return;
        }

        // Self-defending guard on the caller-discipline invariant (the S3WindowSql precedent): the
        // engine always stamps WatermarkCursor/WatermarkUpperBound and WatermarkValue together for a
        // windowed dataset — never one without the other.
        if (spec.WatermarkValue is null)
        {
            throw new InvalidOperationException(
                "SftpWindowFilter: WatermarkCursor/WatermarkUpperBound are set but WatermarkValue is " +
                "null -- the engine always pairs a windowed dataset's lower and upper watermark bounds together");
        }

        IsActive = true;

        _cursorOrdinal = FindOrdinal(schema, spec.WatermarkCursor, spec.Dataset);
        _rowKept = BuildRowKept(cursorTypeName, spec.WatermarkValue, spec.WatermarkUpperBound);
    }

    private static int FindOrdinal(Schema schema, string cursorName, string dataset)
    {
        for (var i = 0; i < schema.FieldsList.Count; i++)
        {
            if (schema.FieldsList[i].Name == cursorName)
            {
                return i;
            }
        }

        throw new PzConnectorException(
            $"dataset '{dataset}': window filter cursor column '{cursorName}' is not present in the read schema",
            isTransient: false);
    }

    /// <summary>Filters <paramref name="batch"/> to rows satisfying <c>cursor &gt; lo AND cursor &lt;= hi</c>
    /// (lower exclusive; this connector never declares InclusiveWatermarkBound, so
    /// <see cref="DatasetSpec.WatermarkLowerInclusive"/> is not consulted). Never disposes
    /// <paramref name="batch"/> — the caller owns disposal. Fast paths: all rows kept returns
    /// <paramref name="batch"/> itself unchanged; none kept returns null.</summary>
    public RecordBatch? Filter(RecordBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (!IsActive)
        {
            return batch;
        }

        var cursorColumn = batch.Column(_cursorOrdinal);
        List<int>? kept = null;
        for (var row = 0; row < batch.Length; row++)
        {
            if (_rowKept!(cursorColumn, row))
            {
                (kept ??= new List<int>(batch.Length)).Add(row);
            }
        }

        if (kept is null)
        {
            return null;
        }

        if (kept.Count == batch.Length)
        {
            return batch;
        }

        var columns = new IArrowArray[batch.ColumnCount];
        for (var col = 0; col < columns.Length; col++)
        {
            columns[col] = batch.Column(col);
        }

        var builder = new ArrowBatchBuilder(batch.Schema);
        foreach (var row in kept)
        {
            builder.AppendFrom(columns, row);
        }

        return builder.Flush();
    }

    /// <summary>Parses lo/hi ONCE per the canonical watermark string forms (<see cref="DatasetSpec.WatermarkValue"/>'s
    /// doc comment) and returns a closure comparing one cell, typed per <paramref name="typeName"/>, against
    /// them — avoids re-parsing the bounds on every row. A null cell is always excluded.</summary>
    private static Func<IArrowArray, int, bool> BuildRowKept(string typeName, string loText, string hiText) =>
        typeName switch
        {
            "int" => IntRowKept(loText, hiText),
            "bigint" => BigintRowKept(loText, hiText),
            "double" => DoubleRowKept(loText, hiText),
            "decimal" => DecimalRowKept(loText, hiText),
            "date" => DateRowKept(loText, hiText),
            "timestamp" => TimestampRowKept(loText, hiText),
            "varchar" => VarcharRowKept(loText, hiText),
            _ => throw new PzConnectorException(
                $"window filter: unsupported cursor column type '{typeName}'", isTransient: false),
        };

    private static Func<IArrowArray, int, bool> IntRowKept(string loText, string hiText)
    {
        var lo = long.Parse(loText, CultureInfo.InvariantCulture);
        var hi = long.Parse(hiText, CultureInfo.InvariantCulture);
        return (column, row) =>
        {
            var array = (Int32Array)column;
            if (array.IsNull(row))
            {
                return false;
            }

            var value = array.GetValue(row)!.Value;
            return value > lo && value <= hi;
        };
    }

    private static Func<IArrowArray, int, bool> BigintRowKept(string loText, string hiText)
    {
        var lo = long.Parse(loText, CultureInfo.InvariantCulture);
        var hi = long.Parse(hiText, CultureInfo.InvariantCulture);
        return (column, row) =>
        {
            var array = (Int64Array)column;
            if (array.IsNull(row))
            {
                return false;
            }

            var value = array.GetValue(row)!.Value;
            return value > lo && value <= hi;
        };
    }

    private static Func<IArrowArray, int, bool> DoubleRowKept(string loText, string hiText)
    {
        var lo = double.Parse(loText, CultureInfo.InvariantCulture);
        var hi = double.Parse(hiText, CultureInfo.InvariantCulture);
        return (column, row) =>
        {
            var array = (DoubleArray)column;
            if (array.IsNull(row))
            {
                return false;
            }

            var value = array.GetValue(row)!.Value;
            return value > lo && value <= hi;
        };
    }

    private static Func<IArrowArray, int, bool> DecimalRowKept(string loText, string hiText)
    {
        var lo = decimal.Parse(loText, CultureInfo.InvariantCulture);
        var hi = decimal.Parse(hiText, CultureInfo.InvariantCulture);
        return (column, row) =>
        {
            var array = (Decimal128Array)column;
            if (array.IsNull(row))
            {
                return false;
            }

            var value = array.GetValue(row)!.Value;
            return value > lo && value <= hi;
        };
    }

    private static Func<IArrowArray, int, bool> DateRowKept(string loText, string hiText)
    {
        var lo = PathTemplate.ParseCanonical(loText);
        var hi = PathTemplate.ParseCanonical(hiText);
        return (column, row) =>
        {
            var array = (Date32Array)column;
            if (array.IsNull(row))
            {
                return false;
            }

            var cell = array.GetDateOnly(row)!.Value;
            var value = new DateTimeOffset(cell.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);
            return value > lo && value <= hi;
        };
    }

    private static Func<IArrowArray, int, bool> TimestampRowKept(string loText, string hiText)
    {
        var lo = PathTemplate.ParseCanonical(loText);
        var hi = PathTemplate.ParseCanonical(hiText);
        return (column, row) =>
        {
            var array = (TimestampArray)column;
            if (array.IsNull(row))
            {
                return false;
            }

            var value = array.GetTimestamp(row)!.Value;
            return value > lo && value <= hi;
        };
    }

    private static Func<IArrowArray, int, bool> VarcharRowKept(string loText, string hiText) =>
        (column, row) =>
        {
            var array = (StringArray)column;
            if (array.IsNull(row))
            {
                return false;
            }

            var value = array.GetString(row)!;
            return string.CompareOrdinal(value, loText) > 0 && string.CompareOrdinal(value, hiText) <= 0;
        };
}
