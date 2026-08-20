# Stress harness (2026-08-15)

Scenario generator + process monitor used for the 2026-08-15 stress/memory investigation.
Not part of CI and not run by `dotnet test` — this is a manual harness, like `scripts/macro-bench.sh`.

It answers one question empirically: **does anything unnecessarily buffer a dataset in .NET, or
otherwise break the out-of-core execution model?** The discriminator is to pin
`engine.duckdb.memory_limit` low so DuckDB must spill, then compare measured peak RSS against the
budget `pz plan` prints (`https://pipelinez.dev/performance/`'s formula). RSS far above that budget means the .NET
side is holding data DuckDB was supposed to hold on disk.

## Running

```bash
export PZ_STRESS_ROOT=/var/tmp/pz-stress          # scratch: generated data + projects + samples
export PZ_CLI_DLL=src/Pz.Cli/bin/Release/net10.0/Pz.Cli.dll
dotnet build Pz.slnx -c Release

python3 scripts/stress/gen.py all                 # generate every scenario family
python3 scripts/stress/driver.py scale-20m uni-20m # plan + run + monitor, append results.jsonl
python3 scripts/stress/retention.py scale-1m 14    # .pz growth across repeated runs
python3 scripts/stress/mcp_client.py <proj> --allow-run pz_run   # drive `pz mcp` over stdio
```

`gen.py` takes a family name (`scale`, `uni`, `wide`, `bigrec`, `manybatch`, `manyfiles`,
`expensive`, `concurrent`) or `all`. Generated data is deterministic — every value is a pure
function of the row index and a fixed seed — so files are byte-identical across runs.

`driver.py` runs `pz plan` first to capture `plan.json`'s `memoryBudget`, then runs the scenario
under `stressmon.py` and records one JSON object per run in `$PZ_STRESS_ROOT/results.jsonl`
(`peak_rss_mb`, `peak_pz_mb` staging high-water mark, CPU seconds, `rss_over_budget_pct`).

`stressmon.py` samples the whole process tree at 200 ms and takes peak RSS from
`wait4(ru_maxrss)`, which covers the child tree. DuckDB runs in-process, so RSS covers both the
.NET heap and DuckDB's buffer pool — that is the point.

`wide-probe.cs` isolates the wide-table OOM (finding F1) by replaying pz's own staging statements
against the production `DuckSession`, parameterised by column count, memory limit,
`preserve_insertion_order` and DuckDB thread count:

```bash
dotnet scripts/stress/wide-probe.cs <wide.csv> <ncols> <memlimit> <preserve> [threads]
```

## Findings this harness produced

`results.jsonl` holds the raw records from the 2026-08-15 run (8 logical cores, 15 GiB RAM), taken
BEFORE the fixes below — re-running now produces different numbers for the F1/F2 scenarios, which is
the point.

- **F1** *(disclosed)* — a 20k×1000 table OOMs at `memory_limit: 1GiB` while `pz plan` reported a
  1.63 GB budget. The floor scales with **columns × DuckDB threads**, not rows; `engine.duckdb.threads: 1`
  fixes it at the same limit. `engine.duckdb.threads` is a *different key* from `engine.threads` and is
  unset by default, so DuckDB uses the machine core count. The formula cannot grow a column term (a
  contract-less csv/json dataset has no schema at plan time), so `pz plan` now prints a `note:` line and
  plan.json carries `duckdbThreadsDisclaimer`. The OOM itself is DuckDB's, and still happens — what
  changed is that the budget no longer implies it cannot.
- **F2** *(fixed)* — the universal (Arrow) csv read path failed on any row ≥ 16 KB, Sylvan's default
  `MaxBufferSize`, which pz did not expose. `CsvSource` now sets a 16 MiB ceiling, so the universal tier
  is the more permissive of the two rather than the more restrictive.
- **F3** *(fixed)* — node runtime errors reached `run_results.json`, NDJSON and the MCP envelope, but
  the text console renderers printed neither `errorCode` nor `errorMessage`. Both renderers now do.
  PZ0501 also appends a `pz:` guidance sentence naming pz keys instead of the underlying library's.
- **F4** *(fixed)* — the universal sink was ~7.7× slower than native COPY on the same data (CPU, not
  memory): the csv write sessions stringified every cell, built one `StringBuilder` per row, transcoded
  it UTF-16 → UTF-8 through a `StreamWriter`, and awaited once per row. The write path now formats
  straight into a pooled UTF-8 buffer from pinned Arrow column buffers (`CsvWriteCodec`, shared by the
  LocalFiles and AzureBlob sinks), the csv read path parses out of Sylvan's own char buffer instead of a
  string-plus-box per cell (`CsvArrowReader`), and the NDJSON writer reuses one `Utf8JsonWriter` per
  batch instead of constructing one per row. Output bytes are unchanged on every format — pinned by
  `UniversalWriteFormatTests`. Measured on this machine, 5M rows, csv → csv, `force_universal`, against
  the same run on the commit before the change:

  | | before | after | native (unchanged) |
  |---|---|---|---|
  | SourceLoad | 5003 ms | 4162 ms | 1249 ms |
  | SinkWrite | 5773 ms | 2763 ms | 873 ms |
  | wall | 12.2 s | 8.6 s | 4.7 s |

  So the sink node went from 6.6× native to 3.2×, and a whole universal run from 2.6× native to 1.9×.

  A follow-up pass took the read side further, by splitting a large csv into byte ranges the engine
  reads concurrently (`CsvSplitPlanner`) instead of optimizing the single reader any harder: SourceLoad
  2540 ms → 1864 ms against a 706 ms native control on the same (faster, see below) machine state, i.e.
  3.6× native → 2.6×, and the node's bottleneck hint flips to "ingest-bound — reader idle 65%". Past
  that point the reader is no longer the constraint at all, so what is left is the Arrow → DuckDB
  ingest. Note that machine state drifts a lot between sessions here — the native control alone moved
  1249 ms → 706 ms with no code change — so only ratios measured in the same session compare.
  `https://pipelinez.dev/performance/` carries the full table and the caveat that split reads land rows unordered.

  What is left of the reader is mostly irreducible at this design: `double` formatting ("R",
  ~340 ns/value) and parsing (~184 ns/value) dominate both directions. Arrow's own builder appends,
  estimated here at 30-60 ns/cell, turned out to be ~4 ns/cell when replaced and measured — see
  `https://pipelinez.dev/performance/`. The two probes below split those costs apart.

## Probes

`csv-write-probe.cs` and `csv-read-probe.cs` isolate each direction of the universal csv path from the
engine, DuckDB and the filesystem, so a change can be measured in seconds instead of through a full run:

```bash
dotnet scripts/stress/csv-write-probe.cs [rows] [batch-rows] [reps] [orders|long4|dbl4|str4]
dotnet scripts/stress/csv-read-probe.cs <orders.csv> [reps]
```

The write probe reports rows/sec and allocation for a shape of your choosing (the per-type shapes are
what show which column type dominates); the read probe reports Sylvan's own parse time separately from
pz's, so read-side work can be attributed to the right side.

Absolute numbers are machine-specific and are not gates; the ratios and the pass/fail boundaries
(8 KB vs 16 KB rows, threads=1 vs threads=2 at 1 GiB) are the portable results.
