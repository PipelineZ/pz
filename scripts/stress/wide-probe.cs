#!/usr/bin/env dotnet
#:property PublishAot=false
#:project ../../src/Pz.DuckDb/Pz.DuckDb.csproj

// Isolates the wide-table OOM: replays pz's own staging statements against a wide csv through the
// production DuckSession, with and without preserve_insertion_order, at a fixed memory_limit.
// usage: wide-probe.cs <wide.csv> <ncols> <memlimit> <preserve:true|false>

using System.Diagnostics;
using Pz.DuckDb;

var csv = args[0];
var ncols = int.Parse(args[1]);
var mem = args[2];
var preserve = args[3];
var threads = args.Length > 4 ? int.Parse(args[4]) : 2;

var cols = string.Join(", ", Enumerable.Range(0, ncols).Select(i => $"'c{i}': 'BIGINT'"));
var dbDir = Path.Combine(Environment.GetEnvironmentVariable("PZ_STRESS_ROOT") ?? "/tmp/pz-stress", "wideprobe", Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(dbDir);

try
{
    var sw = Stopwatch.StartNew();
    await using var duck = DuckSession.Open(Path.Combine(dbDir, "probe.duckdb"),
        new DuckOptions(MemoryLimit: mem, Threads: threads, TempDirectory: dbDir));

    await duck.ExecuteAsync($"set preserve_insertion_order = {preserve}");

    try
    {
        await duck.ExecuteAsync(
            $"create or replace table staged as select * from read_csv('{csv}', header = true, columns = {{{cols}}})");
        Console.WriteLine($"  stage:    OK  {sw.Elapsed.TotalSeconds:F1}s");
    }
    catch (Exception ex) { Console.WriteLine($"  stage:    FAIL {sw.Elapsed.TotalSeconds:F1}s :: {ex.Message.Split('\n')[0]}"); return 1; }

    sw.Restart();
    try
    {
        await duck.ExecuteAsync("create or replace table piped as select * from staged");
        Console.WriteLine($"  pipeline: OK  {sw.Elapsed.TotalSeconds:F1}s");
    }
    catch (Exception ex) { Console.WriteLine($"  pipeline: FAIL {sw.Elapsed.TotalSeconds:F1}s :: {ex.Message.Split('\n')[0]}"); return 1; }

    sw.Restart();
    try
    {
        await duck.ExecuteAsync($"copy (select * from piped) to '{dbDir}/out.csv' (format csv, header)");
        Console.WriteLine($"  sink:     OK  {sw.Elapsed.TotalSeconds:F1}s");
    }
    catch (Exception ex) { Console.WriteLine($"  sink:     FAIL {sw.Elapsed.TotalSeconds:F1}s :: {ex.Message.Split('\n')[0]}"); return 1; }

    return 0;
}
finally { try { Directory.Delete(dbDir, true); } catch { } }
