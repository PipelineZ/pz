#!/usr/bin/env dotnet
#:property PublishAot=false
#:project ../../src/Pz.Connectors.Toolkit/Pz.Connectors.Toolkit.csproj

// Isolates the universal (Arrow) csv sink's encoding cost from the rest of a run: builds Arrow batches
// of the stress harness's `orders` shape in memory, then writes them through the production
// CsvWriteCodec to /dev/null. Nothing here touches DuckDB, the engine, or a real file, so what it
// times is exactly the per-cell formatting that dominates universal-tier csv writes.
//
// usage: csv-write-probe.cs [rows] [batch-rows] [reps] [shape]
//   shape: orders (2 bigint + double + varchar, the stress harness's shape) | long4 | dbl4 | str4

using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Toolkit.Formats;

var rows = args.Length > 0 ? int.Parse(args[0]) : 5_000_000;
var batchRows = args.Length > 1 ? int.Parse(args[1]) : 100_000;
var reps = args.Length > 2 ? int.Parse(args[2]) : 3;
var shape = args.Length > 3 ? args[3] : "orders";

var kinds = shape switch
{
    "orders" => new[] { "i64", "i64", "dbl", "str" },
    "long4" => new[] { "i64", "i64", "i64", "i64" },
    "dbl4" => new[] { "dbl", "dbl", "dbl", "dbl" },
    "str4" => new[] { "str", "str", "str", "str" },
    _ => throw new ArgumentException($"unknown shape '{shape}'"),
};

var schema = new Schema(kinds.Select((k, i) => new Field($"c{i}", k switch
{
    "i64" => Int64Type.Default,
    "dbl" => (IArrowType)DoubleType.Default,
    _ => StringType.Default,
}, nullable: true)).ToArray(), null);

var statuses = new[] { "new", "paid", "shipped", "cancelled" };

RecordBatch BuildBatch(int start, int count)
{
    var arrays = new IArrowArray[kinds.Length];
    for (var c = 0; c < kinds.Length; c++)
    {
        switch (kinds[c])
        {
            case "i64":
                var longs = new Int64Array.Builder();
                for (var i = 0; i < count; i++) longs.Append((start + i) % 100_000);
                arrays[c] = longs.Build();
                break;
            case "dbl":
                var doubles = new DoubleArray.Builder();
                for (var i = 0; i < count; i++) doubles.Append((((start + i) % 10_000) + 1) * 1.37d);
                arrays[c] = doubles.Build();
                break;
            default:
                var strings = new StringArray.Builder();
                for (var i = 0; i < count; i++) strings.Append(statuses[(start + i) % statuses.Length]);
                arrays[c] = strings.Build();
                break;
        }
    }

    return new RecordBatch(schema, arrays, count);
}

var batches = new List<RecordBatch>();
for (var start = 0; start < rows; start += batchRows)
{
    batches.Add(BuildBatch(start, Math.Min(batchRows, rows - start)));
}

Console.WriteLine($"shape {shape}: {batches.Count} batches x {batchRows} rows");

for (var rep = 0; rep < reps; rep++)
{
    var before = GC.GetTotalAllocatedBytes(precise: false);
    var sw = Stopwatch.StartNew();
    long bytes;
    await using (var stream = new CountingStream())
    {
        await using var writer = new CsvWriteCodec(stream, schema, "probe", leaveOpen: true);
        foreach (var batch in batches)
        {
            await writer.WriteBatchAsync(batch, CancellationToken.None);
        }

        await writer.FlushAsync(CancellationToken.None);
        bytes = stream.Written;
    }

    sw.Stop();
    var allocated = (GC.GetTotalAllocatedBytes(precise: false) - before) / 1048576d;
    Console.WriteLine(
        $"rep {rep}: {sw.ElapsedMilliseconds} ms  {bytes / 1048576d:F1} MiB  " +
        $"{rows / sw.Elapsed.TotalSeconds / 1e6:F2} Mrow/s  alloc {allocated:F1} MiB");
}

foreach (var batch in batches)
{
    batch.Dispose();
}

/// <summary>Counts bytes and discards them — the point is to time the encoder, not the filesystem.</summary>
internal sealed class CountingStream : Stream
{
    public long Written { get; private set; }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => Written;
    public override long Position { get => Written; set => throw new NotSupportedException(); }

    public override void Write(byte[] buffer, int offset, int count) => Written += count;
    public override void Write(ReadOnlySpan<byte> buffer) => Written += buffer.Length;

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
    {
        Written += buffer.Length;
        return ValueTask.CompletedTask;
    }

    public override void Flush() { }
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
}
