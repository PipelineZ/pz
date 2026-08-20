// This suite drives the real CLI in-process, and the CLI writes to the process-global Console.
// Several classes swap Console.Out for a capturing StringWriter -- the "console-redirection"
// collection serializes THOSE against each other -- but any class that merely RUNS the CLI
// without redirecting (RestoreRunTests, which joins the "m2-local-feed" fixture collection
// instead, and a class can join only one collection) still races the swappers when xunit runs
// its collection in parallel: its plain console lines land inside another test's captured
// buffer, e.g. an "ok ..." renderer line inside JsonLogFormatTests' NDJSON capture, which then
// fails JsonDocument.Parse. One process-global means no
// safe parallelism in this assembly -- serialize it wholesale. The collections stay: they still
// document intent and carry the local-feed fixture.
[assembly: Xunit.CollectionBehavior(DisableTestParallelization = true)]
