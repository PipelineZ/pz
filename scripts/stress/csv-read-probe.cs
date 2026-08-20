#!/usr/bin/env dotnet
#:property PublishAot=false
#:project ../../connectors/Pz.Connector.LocalFiles/Pz.Connector.LocalFiles.csproj

// Splits the universal (Arrow) csv source's cost into its two halves: what Sylvan spends parsing the
// file, and what pz spends turning the parsed fields into Arrow batches. Nothing here touches DuckDB or
// the engine, so the difference between the two numbers is exactly pz's own read-side overhead. Drives
// the connector through its public ISource surface, so it reads the file the way a real run does.
//
// usage: csv-read-probe.cs <orders.csv> [reps]

using System.Diagnostics;
using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Sylvan.Data.Csv;

var path = args[0];
var reps = args.Length > 1 ? int.Parse(args[1]) : 3;

var contract = new Dictionary<string, string>
{
    ["id"] = "bigint",
    ["customer_id"] = "bigint",
    ["amount"] = "double",
    ["status"] = "varchar",
};

var spec = new DatasetSpec("bench", "orders", new Dictionary<string, object?>
{
    ["path"] = path,
    ["format"] = "csv",
    ["columns"] = contract,
});

var options = BatchOptions.Default;

for (var rep = 0; rep < reps; rep++)
{
    // 1. Sylvan alone: advance every row and touch every field's span, discarding it.
    var sw = Stopwatch.StartNew();
    long fields = 0, rows = 0;
    using (var text = new StreamReader(path))
    using (var csv = await CsvDataReader.CreateAsync(text, new CsvDataReaderOptions { HasHeaders = true }))
    {
        while (await csv.ReadAsync())
        {
            rows++;
            for (var i = 0; i < csv.FieldCount; i++)
            {
                fields += csv.GetFieldSpan(i).Length;
            }
        }
    }

    sw.Stop();
    var parseMs = sw.ElapsedMilliseconds;

    // 2. The production read path: Sylvan + parse + Arrow batch building.
    var before = GC.GetTotalAllocatedBytes(precise: false);
    sw.Restart();
    long batchRows = 0, batches = 0;
    await using var source = await ((ISourceConnector)new LocalFilesConnector())
        .OpenAsync(new ConnectorConfig(new Dictionary<string, object?>()), CancellationToken.None);
    var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
    foreach (var partition in partitions)
    {
        await foreach (var batch in partition.ReadAsync(options, CancellationToken.None))
        {
            using (batch)
            {
                batchRows += batch.Length;
                batches++;
            }
        }
    }

    sw.Stop();
    var allocated = (GC.GetTotalAllocatedBytes(precise: false) - before) / 1048576d;

    Console.WriteLine(
        $"rep {rep}: parse-only {parseMs} ms ({rows} rows, {fields} field chars) | " +
        $"full {sw.ElapsedMilliseconds} ms ({batchRows} rows, {batches} batches, alloc {allocated:F0} MiB) | " +
        $"pz overhead {sw.ElapsedMilliseconds - parseMs} ms");
}
