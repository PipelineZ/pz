using System.Text;
using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.LocalFiles.Tests;

/// <summary>The universal csv read may cut a large file into byte ranges and read them concurrently
/// (<see cref="CsvSplitPlan"/>). The failure mode that matters is silent: a boundary landing inside a
/// quoted field drops or duplicates rows and nothing complains, so these tests compare a split read
/// against the whole-file read of the SAME bytes, over content chosen to break a naive splitter —
/// quoted commas, quoted newlines, escaped quotes, and literal quotes in unquoted fields (which Sylvan
/// reads as data, not as quoting).
///
/// Every test drives the planner with a small <c>minBytesPerPartition</c>; production's is 32 MiB, which
/// is why the whole feature is invisible to every other fixture in the suite.</summary>
public sealed class CsvSplitReadTests : IDisposable
{
    private const long TinyPartition = 2048;

    private readonly string _work =
        Path.Combine(Path.GetTempPath(), "pz-csv-split-tests", Guid.NewGuid().ToString("N"));

    public CsvSplitReadTests() => Directory.CreateDirectory(_work);

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static readonly Dictionary<string, string> Contract = new()
    {
        ["id"] = "bigint",
        ["name"] = "varchar",
        ["amount"] = "double",
    };

    /// <summary>The header names exactly as the CSV reader resolves them — which is what production hands
    /// the planner, and the only thing that makes the delimiter proof mean anything: the planner compares
    /// them, comma-joined, against the file's raw header line.</summary>
    private static string[] HeaderOf(string path)
    {
        using var text = new StreamReader(path);
        using var csv = Sylvan.Data.Csv.CsvDataReader.Create(text, CsvSource.ReaderOptions());
        var names = new string[csv.FieldCount];
        for (var i = 0; i < names.Length; i++)
        {
            names[i] = csv.GetName(i);
        }

        return names;
    }

    /// <summary>Rows whose text is deliberately hostile to byte-range splitting: every third row carries a
    /// quoted field with an embedded newline, so a splitter that treats every newline as a record
    /// terminator will cut one in half.</summary>
    private static string[] AwkwardNames =>
    [
        "plain",
        "has,comma",
        "has\nnewline",
        "has\"\"escaped",
        "literal\"quote",
        "unicode-é-🌍",
        "trailing\r\ncarriage",
    ];

    private string WriteAwkwardCsv(int rows, string name = "orders.csv")
    {
        var builder = new StringBuilder();
        builder.Append("id,name,amount\n");
        for (var i = 1; i <= rows; i++)
        {
            var raw = AwkwardNames[i % AwkwardNames.Length];
            // Anything a reader could mistake for structure has to be quoted; a bare literal quote in an
            // unquoted field is left bare on purpose, because that is the case where this scanner and
            // Sylvan must agree that the quote is data.
            var field = raw.Contains(',') || raw.Contains('\n') || raw.Contains('"') && raw.Contains("\"\"")
                ? $"\"{raw}\""
                : raw;
            builder.Append(i).Append(',').Append(field).Append(',').Append(i).Append(".5\n");
        }

        var path = Path.Combine(_work, name);
        File.WriteAllText(path, builder.ToString());
        return path;
    }

    private static async Task<List<string>> ReadRowsAsync(IEnumerable<CsvPartition> partitions)
    {
        var rows = new List<string>();
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                using (batch)
                {
                    var id = (Int64Array)batch.Column(0);
                    var name = (StringArray)batch.Column(1);
                    var amount = (DoubleArray)batch.Column(2);
                    for (var i = 0; i < batch.Length; i++)
                    {
                        rows.Add($"{id.GetValue(i)}|{name.GetString(i)}|{amount.GetValue(i)}");
                    }
                }
            }
        }

        return rows;
    }

    private static IEnumerable<CsvPartition> SplitPartitions(string path, CsvSplitPlan plan) =>
        plan.Splits.Select(split => new CsvPartition(path, Contract, plan, split, plan.Splits.Count));

    [Fact]
    public async Task Split_read_returns_exactly_the_rows_of_a_whole_file_read()
    {
        var path = WriteAwkwardCsv(2000);
        var plan = CsvSplitPlanner.TryPlan(path, HeaderOf(path), maxPartitions: 8, minBytesPerPartition: TinyPartition);
        Assert.NotNull(plan);
        Assert.True(plan!.Splits.Count > 1, "the fixture should be large enough to split");

        var whole = await ReadRowsAsync([new CsvPartition(path, Contract)]);
        var split = await ReadRowsAsync(SplitPartitions(path, plan));

        Assert.Equal(2000, whole.Count);
        // Cross-partition order is not guaranteed (the partitions race through one channel), so compare
        // as multisets -- but nothing may be added, lost or altered.
        Assert.Equal(whole.Order().ToList(), split.Order().ToList());
    }

    [Fact]
    public async Task Split_boundaries_never_cut_a_record_containing_a_quoted_newline()
    {
        // One row per ~40 bytes with a quoted newline every third row means several boundaries land in or
        // near a multi-line record; a mis-cut shows up as a row count that is not exactly the file's.
        var path = WriteAwkwardCsv(5000);
        var plan = CsvSplitPlanner.TryPlan(path, HeaderOf(path), maxPartitions: 8, minBytesPerPartition: TinyPartition);
        Assert.NotNull(plan);

        var rows = await ReadRowsAsync(SplitPartitions(path, plan!));
        Assert.Equal(5000, rows.Count);
        Assert.Equal(5000, rows.Distinct().Count());
    }

    [Fact]
    public void Split_plan_refuses_a_file_whose_delimiter_is_not_a_comma()
    {
        var builder = new StringBuilder("id;name;amount\n");
        for (var i = 1; i <= 2000; i++)
        {
            builder.Append(i).Append(";name-").Append(i).Append(';').Append(i).Append(".5\n");
        }

        var path = Path.Combine(_work, "semicolons.csv");
        File.WriteAllText(path, builder.ToString());

        // The reader auto-detects the semicolon and reports three fields, so comma-joining them cannot
        // reproduce the file's header line — and without a proven delimiter the scanner cannot tell which
        // quotes are quoting, so it declines the whole file.
        Assert.Null(CsvSplitPlanner.TryPlan(
            path, HeaderOf(path), maxPartitions: 8, minBytesPerPartition: TinyPartition));
    }

    [Fact]
    public void Split_plan_refuses_a_file_that_ends_inside_a_quoted_field()
    {
        var builder = new StringBuilder("id,name,amount\n");
        for (var i = 1; i <= 2000; i++)
        {
            builder.Append(i).Append(",name-").Append(i).Append(',').Append(i).Append(".5\n");
        }

        builder.Append("2001,\"never closed,1.0\n");
        var path = Path.Combine(_work, "unterminated.csv");
        File.WriteAllText(path, builder.ToString());

        Assert.Null(CsvSplitPlanner.TryPlan(
            path, HeaderOf(path), maxPartitions: 8, minBytesPerPartition: TinyPartition));
    }

    [Fact]
    public void Split_plan_refuses_a_file_below_the_production_threshold()
    {
        var path = WriteAwkwardCsv(2000);
        Assert.True(new FileInfo(path).Length < CsvSplitPlanner.MinBytesPerPartition);
        Assert.Null(CsvSplitPlanner.TryPlan(path, HeaderOf(path), maxPartitions: 8));
    }

    [Fact]
    public async Task A_parse_error_in_a_split_names_the_same_row_as_an_unsplit_read()
    {
        var builder = new StringBuilder("id,name,amount\n");
        for (var i = 1; i <= 2000; i++)
        {
            // Row 1900 carries a value no double parse accepts, well past the first boundary.
            builder.Append(i).Append(",name-").Append(i).Append(',')
                .Append(i == 1900 ? "not-a-number" : $"{i}.5").Append('\n');
        }

        var path = Path.Combine(_work, "badvalue.csv");
        File.WriteAllText(path, builder.ToString());

        var plan = CsvSplitPlanner.TryPlan(path, HeaderOf(path), maxPartitions: 8, minBytesPerPartition: TinyPartition);
        Assert.NotNull(plan);
        Assert.True(plan!.Splits.Count > 1);

        var unsplit = await Assert.ThrowsAsync<PzConnectorException>(
            () => ReadRowsAsync([new CsvPartition(path, Contract)]));
        var split = await Assert.ThrowsAsync<PzConnectorException>(
            () => ReadRowsAsync(SplitPartitions(path, plan)));

        Assert.Contains("line 1900", unsplit.Message);
        Assert.Equal(unsplit.Message, split.Message);
    }

    [Fact]
    public async Task Split_read_handles_a_utf8_bom_and_crlf_line_endings()
    {
        var builder = new StringBuilder("id,name,amount\r\n");
        for (var i = 1; i <= 2000; i++)
        {
            builder.Append(i).Append(",\"name,").Append(i).Append("\",").Append(i).Append(".5\r\n");
        }

        var path = Path.Combine(_work, "bom-crlf.csv");
        File.WriteAllText(path, builder.ToString(), new UTF8Encoding(encoderShouldEmitUTF8Identifier: true));

        var plan = CsvSplitPlanner.TryPlan(path, HeaderOf(path), maxPartitions: 8, minBytesPerPartition: TinyPartition);
        Assert.NotNull(plan);

        var whole = await ReadRowsAsync([new CsvPartition(path, Contract)]);
        var split = await ReadRowsAsync(SplitPartitions(path, plan!));
        Assert.Equal(2000, whole.Count);
        Assert.Equal(whole.Order().ToList(), split.Order().ToList());
    }
}
