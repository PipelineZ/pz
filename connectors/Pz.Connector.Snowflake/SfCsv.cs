using System.Globalization;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Snowflake;

/// <summary>Snowflake-flavored CSV encoding for the sink's spool files, and the matching COPY
/// FILE_FORMAT clause -- the two halves of one contract, so a change to one without the other
/// silently breaks loads. NULL encodes as a bare <c>\N</c> (unquoted, matched by
/// <see cref="FileFormatClause"/>'s <c>null_if</c>); every string encodes double-quoted with
/// <c>""</c>-escaping, whatever its content, so an empty string is never confused with NULL; booleans
/// are <c>TRUE</c>/<c>FALSE</c>; dates are <c>yyyy-MM-dd</c>; timestamps are naive UTC
/// <c>yyyy-MM-dd HH:mm:ss.ffffff</c> (microsecond precision, matching the v0 matrix's Timestamp(us));
/// every numeric renders with <see cref="CultureInfo.InvariantCulture"/>, except a double's three
/// special values, which InvariantCulture spells in a way Snowflake's COPY can't parse for FLOAT --
/// see <see cref="FormatDouble"/>. Lines are terminated with a bare <c>\n</c> -- never <c>\r\n</c> --
/// so a spool file written on any platform is byte-identical.</summary>
internal static class SfCsv
{
    /// <summary>The unquoted NULL marker written for a null cell, and the token
    /// <see cref="FileFormatClause"/>'s <c>null_if</c> tells COPY to translate back to NULL. Backslash
    /// + N, matching Snowflake's own default CSV null spelling.</summary>
    private const string NullToken = "\\N";

    /// <summary>The COPY INTO option clause matching this class's encoding: <c>"</c> is the (optional)
    /// quote character, <c>\N</c> unquoted is NULL, and no separate unenclosed-field escape character
    /// applies (quoted fields carry their own <c>""</c> escaping).</summary>
    public const string FileFormatClause =
        "file_format = (type = csv field_optionally_enclosed_by = '\"' null_if = ('\\\\N') escape_unenclosed_field = none)";

    /// <summary>Appends every row of <paramref name="batch"/> to <paramref name="writer"/> as CSV rows
    /// (no header -- the target column list is carried by the COPY statement, not the file). Columns
    /// are written in the batch's own order, which the caller (the sink) keeps aligned with the
    /// COPY's explicit column list.
    ///
    /// <para>When <paramref name="sequenceStart"/> is non-null, every row also gets one trailing,
    /// unquoted, monotonically increasing integer column -- the sink's merge staging's
    /// <c>_pz_seq</c> (see <c>SfDdl.StagingSequenceColumn</c>). This is a real value written into the
    /// file, not left to a target-side autoincrement, because Snowflake's COPY can load a stage's
    /// files in parallel: an autoincrement's fill order would not reliably track arrival (write)
    /// order across files the way a value stamped into the CSV up front does, and
    /// <c>SfDdl.BuildMergeSql</c>'s last-writer-wins dedup depends on that order being
    /// trustworthy.</para></summary>
    /// <returns>The next unused sequence value -- <paramref name="sequenceStart"/> plus the number of
    /// rows written -- for the caller to pass into its next call across a session's batches/files.
    /// Meaningless (and ignored) when <paramref name="sequenceStart"/> is null.</returns>
    public static long WriteBatch(RecordBatch batch, TextWriter writer, long? sequenceStart = null)
    {
        var columnCount = batch.Schema.FieldsList.Count;
        var columns = new IArrowArray[columnCount];
        for (var col = 0; col < columnCount; col++)
        {
            columns[col] = batch.Column(col);
        }

        var sequence = sequenceStart ?? 0;
        var line = new StringBuilder();
        for (var row = 0; row < batch.Length; row++)
        {
            line.Clear();
            for (var col = 0; col < columnCount; col++)
            {
                if (col > 0)
                {
                    line.Append(',');
                }

                AppendCell(line, columns[col], row, batch.Schema.FieldsList[col].Name);
            }

            if (sequenceStart is not null)
            {
                line.Append(',').Append(sequence.ToString(CultureInfo.InvariantCulture));
                sequence++;
            }

            line.Append('\n');
            writer.Write(line.ToString());
        }

        return sequence;
    }

    private static void AppendCell(StringBuilder line, IArrowArray array, int row, string columnName)
    {
        if (array.IsNull(row))
        {
            line.Append(NullToken);
            return;
        }

        switch (array)
        {
            case Int32Array a:
                line.Append(a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture));
                break;
            case Int64Array a:
                line.Append(a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture));
                break;
            case DoubleArray a:
                line.Append(FormatDouble(a.GetValue(row)!.Value));
                break;
            case Decimal128Array a:
                AppendDecimal(line, a, row, columnName);
                break;
            case BooleanArray a:
                line.Append(a.GetValue(row)!.Value ? "TRUE" : "FALSE");
                break;
            case Date32Array a:
                line.Append(a.GetDateTime(row)!.Value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
                break;
            case TimestampArray a:
                line.Append(a.GetTimestamp(row)!.Value.UtcDateTime.ToString(
                    "yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture));
                break;
            case StringArray a:
                AppendQuotedString(line, a.GetString(row));
                break;
            default:
                // Unreachable via pz run: ISinkConnector.OpenAsync's schema always carries the v0
                // matrix (see its doc comment). Kept as ABI defense-in-depth, matching
                // SfTypeMap.ToSnowflakeDdl's outside-the-matrix guard.
                throw new PzConnectorException(
                    $"arrow array type '{array.GetType().Name}' has no snowflake CSV encoding -- outside the v0 matrix",
                    isTransient: false);
        }
    }

    private static void AppendQuotedString(StringBuilder line, string value)
    {
        line.Append('"');
        foreach (var c in value)
        {
            if (c == '"')
            {
                line.Append('"');
            }

            line.Append(c);
        }

        line.Append('"');
    }

    /// <summary>double.ToString(InvariantCulture) renders the three special values as "NaN",
    /// "Infinity", "-Infinity" -- none of which Snowflake's CSV COPY parses for a FLOAT column (it
    /// accepts <c>nan</c>, <c>inf</c>, <c>-inf</c>, case-insensitively). With <c>on_error =
    /// abort_statement</c> (see <see cref="BuildCopyIntoStagingSql"/> in SfDdl -- COPY always runs
    /// with it), an un-parseable field is a hard commit failure, not a dropped row.</summary>
    private static string FormatDouble(double value) => value switch
    {
        double.NaN => "nan",
        double.PositiveInfinity => "inf",
        double.NegativeInfinity => "-inf",
        _ => value.ToString(CultureInfo.InvariantCulture),
    };

    /// <summary>Decimal128Array.GetValue widens the stored 128-bit value into a CLR
    /// <see cref="decimal"/> (96-bit mantissa) and throws a raw <see cref="OverflowException"/> when
    /// a NUMBER(p,s) value at the high end of the v0 matrix's precision (up to 38 digits) doesn't fit
    /// -- caught here the same way <c>SnowflakeArrowReader</c>'s read-side decimal branch catches its
    /// own overflow, naming the column, non-transient, with the matching hint.</summary>
    private static void AppendDecimal(StringBuilder line, Decimal128Array array, int row, string columnName)
    {
        try
        {
            line.Append(array.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture));
        }
        catch (OverflowException ex)
        {
            var dec = (Decimal128Type)array.Data.DataType;
            throw new PzConnectorException(
                $"column '{columnName}': value exceeds decimal128({dec.Precision},{dec.Scale}) scale -- " +
                "cast the column in the write query to a narrower NUMBER(p,s), or split it into its own " +
                "varchar output",
                isTransient: false, innerException: ex);
        }
    }
}
