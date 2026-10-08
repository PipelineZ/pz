using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace Pz.Cli.Commands;

/// <summary>One CDC dataset's status as `pz cdc status` reports it. <see cref="StoredToken"/> is printed
/// by the text table and reduced to a presence flag by the json stream, which never carries the value.</summary>
internal sealed record CdcStatusRow(string Dataset, string Connector, bool AdminSupported, bool? Healthy,
    string? PositionName, string? StoredToken, long? RetainedBytes, IReadOnlyList<string> Detail);

/// <summary>What `pz cdc drop` did. <see cref="ServerSideDropped"/> is false for sqlserver, where pz never
/// disables cdc and <see cref="Remediation"/> carries the statement to do it by hand.</summary>
internal sealed record CdcDropSummary(string Dataset, string Connector, string? PositionName,
    bool ServerSideDropped, IReadOnlyList<string> Remediation, bool LocalState);

internal interface ICdcOutput
{
    void Note(string text);
    void NoCdcDatasets();
    void StatusHeader();
    void Status(CdcStatusRow row);
    void Dropped(CdcDropSummary summary);
}

internal static class CdcOutput
{
    public static ICdcOutput For(string logFormat, TimeProvider time, TextWriter? stdout = null, TextWriter? stderr = null) =>
        logFormat == "json"
            ? new CdcJsonOutput(time, stdout ?? Console.Out, stderr ?? Console.Error)
            : new CdcTextOutput(stdout ?? Console.Out);
}

/// <summary>Today's human output, moved here unchanged.</summary>
internal sealed class CdcTextOutput(TextWriter stdout) : ICdcOutput
{
    public void Note(string text) => stdout.WriteLine(text);

    public void NoCdcDatasets() => stdout.WriteLine("no cdc datasets in this project");

    public void StatusHeader() =>
        stdout.WriteLine($"{"dataset",-28} {"position",-20} {"stored token",-20} {"retained",-12} health");

    public void Status(CdcStatusRow row)
    {
        var health = !row.AdminSupported ? "admin unsupported" : row.Healthy == true ? "healthy" : "unhealthy";
        stdout.WriteLine(
            $"{row.Dataset,-28} {row.PositionName ?? "-",-20} {row.StoredToken ?? "-",-20} " +
            $"{row.RetainedBytes?.ToString(CultureInfo.InvariantCulture) ?? "-",-12} {health}");
        if (row.AdminSupported && row.Healthy == false)
        {
            foreach (var line in row.Detail)
            {
                stdout.WriteLine($"    {line}");
            }
        }
    }

    public void Dropped(CdcDropSummary summary)
    {
        var entry = summary.LocalState
            ? "pz's local sync-state entry"
            : "pz's sync-state entry in the configured state store";

        if (summary.Connector == "sqlserver")
        {
            stdout.WriteLine($"{summary.Dataset}: cleared {entry} (the next run will re-snapshot).");
            stdout.WriteLine(
                "SQL Server cdc was NOT disabled server-side -- pz never runs sp_cdc_disable_table. " +
                "To disable it yourself:");
            foreach (var line in summary.Remediation)
            {
                stdout.WriteLine($"  {line}");
            }

            return;
        }

        if (summary.Connector == "postgres")
        {
            stdout.WriteLine(
                $"{summary.Dataset}: dropped replication slot '{summary.PositionName}' and cleared {entry} " +
                "(the next run will re-snapshot).");
            return;
        }

        stdout.WriteLine(
            $"{summary.Dataset}: dropped server-side change-capture state and cleared {entry} " +
            "(the next run will re-snapshot).");
    }
}

/// <summary>`--log-format json`: NDJSON with the run-event envelope (`event`, `at`), byte-stable like
/// JsonRenderer. These events form their own stream and never appear in a `pz run` stream.</summary>
internal sealed class CdcJsonOutput(TimeProvider time, TextWriter stdout, TextWriter stderr) : ICdcOutput
{
    private const string AtFormat = "yyyy-MM-ddTHH:mm:ss.fffZ";

    public void Note(string text) => stderr.WriteLine(text);

    public void NoCdcDatasets() { }

    public void StatusHeader() { }

    public void Status(CdcStatusRow row) => Write("cdc_status", json =>
    {
        json.WriteString("dataset", row.Dataset);
        json.WriteString("connector", row.Connector);
        json.WriteBoolean("adminSupported", row.AdminSupported);
        if (row.Healthy is { } healthy) json.WriteBoolean("healthy", healthy); else json.WriteNull("healthy");
        json.WriteString("positionName", row.PositionName);
        json.WriteBoolean("hasStoredToken", row.StoredToken is not null);
        if (row.RetainedBytes is { } bytes) json.WriteNumber("retainedBytes", bytes); else json.WriteNull("retainedBytes");
        WriteArray(json, "detail", row.Detail);
    });

    public void Dropped(CdcDropSummary summary) => Write("cdc_dropped", json =>
    {
        json.WriteString("dataset", summary.Dataset);
        json.WriteString("connector", summary.Connector);
        json.WriteString("positionName", summary.PositionName);
        json.WriteBoolean("serverSideDropped", summary.ServerSideDropped);
        WriteArray(json, "remediation", summary.Remediation);
        json.WriteString("stateCleared", summary.LocalState ? "local" : "remote");
    });

    private void Write(string eventName, Action<Utf8JsonWriter> fields)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var json = new Utf8JsonWriter(buffer, new JsonWriterOptions { Indented = false }))
        {
            json.WriteStartObject();
            json.WriteString("event", eventName);
            json.WriteString("at", time.GetUtcNow().UtcDateTime.ToString(AtFormat, CultureInfo.InvariantCulture));
            fields(json);
            json.WriteEndObject();
        }

        stdout.Write(Encoding.UTF8.GetString(buffer.WrittenSpan));
        stdout.Write('\n');
    }

    private static void WriteArray(Utf8JsonWriter json, string name, IReadOnlyList<string> items)
    {
        json.WriteStartArray(name);
        foreach (var item in items)
        {
            json.WriteStringValue(item);
        }

        json.WriteEndArray();
    }
}
