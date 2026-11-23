using Pz.Engine.Staging;

namespace Pz.Cli.Commands;

/// <summary>How <c>pz staging query</c> prints: one JSON object or a text table, cells formatted exactly as
/// <c>pz preview</c> formats them (see <see cref="PreviewOutput"/>).</summary>
internal static class StagingQueryOutput
{
    internal static string ToJson(StagingQueryResult result) => PreviewOutput.Write(json =>
    {
        json.WriteStartObject();
        if (result.RunId is null)
        {
            json.WriteNull("runId");
        }
        else
        {
            json.WriteString("runId", result.RunId);
        }

        json.WriteStartArray("columns");
        foreach (var column in result.Columns)
        {
            json.WriteStartObject();
            json.WriteString("name", column.Name);
            json.WriteString("type", column.Type);
            json.WriteEndObject();
        }

        json.WriteEndArray();
        PreviewOutput.WriteRows(json, result.Rows);
        json.WriteNumber("limit", result.Limit);
        json.WriteBoolean("truncated", result.Truncated);
        json.WriteEndObject();
    });

    internal static string ToTable(StagingQueryResult result) => PreviewOutput.Table(
        result.Columns.Select(c => c.Name).ToList(),
        result.Rows,
        result.Truncated
            ? $"{result.Rows.Count} rows shown; the query returned more (raise --limit, up to {StagingQueryRunner.MaxLimit})"
            : $"{result.Rows.Count} rows");
}
