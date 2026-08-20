#!/usr/bin/env dotnet
#:property PublishAot=false
#:project ../src/Pz.DuckDb/Pz.DuckDb.csproj

// Passthrough floor probe: times the ONE
// statement a native-fusion planner would emit for macro-bench.sh's pure-EL csv->csv flow --
// COPY (SELECT * FROM read_csv(...)) TO ... -- through the production DuckSession. Elapsed
// covers session open + the COPY (no pz orchestration: no compile, no plan, no staging table),
// so its delta vs the bench's native leg is fusion's MAXIMUM possible win, not its expected win.
// The schema literal below is macro-bench.sh's generated orders schema on purpose; this probe
// is that harness's counterpart, not a general tool.
//
// Usage: dotnet scripts/passthrough-floor-probe.cs <input.csv> <output.csv>
// Stdout: floor: <seconds>s (DuckSession open + fused COPY)     <- parsed by macro-bench.sh

using System.Diagnostics;
using System.Globalization;
using Pz.DuckDb;

if (args.Length != 2)
{
    Console.Error.WriteLine("usage: passthrough-floor-probe <input.csv> <output.csv>");
    return 2;
}

var input = args[0].Replace("'", "''");
var output = args[1].Replace("'", "''");
var dbDir = Path.Combine(Path.GetTempPath(), "pz-floor-probe", Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(dbDir);

try
{
    var sw = Stopwatch.StartNew();
    await using (var duck = DuckSession.Open(Path.Combine(dbDir, "probe.duckdb")))
    {
        await duck.ExecuteAsync(
            "copy (select * from read_csv('" + input + "', header = true, columns = {" +
            "'id': 'BIGINT', 'customer_id': 'BIGINT', 'amount': 'DOUBLE', 'status': 'VARCHAR'" +
            "})) to '" + output + "' (format csv, header)");
    }

    sw.Stop();
    Console.WriteLine(
        $"floor: {sw.Elapsed.TotalSeconds.ToString("0.0000", CultureInfo.InvariantCulture)}s " +
        "(DuckSession open + fused COPY)");
    return 0;
}
finally
{
    try { Directory.Delete(dbDir, recursive: true); } catch { /* best-effort cleanup */ }
}
