using Pz.Connectors.Sdk;

namespace PcpFakeConnector;

/// <summary>A real out-of-process PCP peer served by the real SDK, delegating every call to a real
/// <c>LocalFilesConnector</c>. Configuration and credentials arrive only through the <c>Configure</c>
/// RPC. The argv switches below choose which failure to stage -- they are test switches, which is why
/// argv is the right surface for them and the wrong surface for config. Everything the SDK owns
/// (<c>--pz-socket</c>, and the <c>--pz-manifest</c> mode the packaging targets invoke) is passed
/// through to it untouched.</summary>
internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        FixtureOptions options;
        string[] passthrough;
        try
        {
            (options, passthrough) = FixtureOptions.Parse(args);
        }
        catch (ArgumentException ex)
        {
            await Console.Error.WriteLineAsync($"PcpFakeConnector: {ex.Message}").ConfigureAwait(false);
            return 2;
        }

        if (options.DieImmediately)
        {
            await Console.Error.WriteLineAsync(
                "PcpFakeConnector: --die-immediately, exiting before the socket is served").ConfigureAwait(false);
            return 1;
        }

        var hooks = new PcpServerHooks
        {
            HangHandshake = options.HangHandshake
                ? ct => Task.Delay(Timeout.InfiniteTimeSpan, ct)
                : null,
            IgnoreCancel = options.IgnoreCancel,
            IgnoreShutdown = options.IgnoreShutdown,
        };

        return await PzConnectorHost.RunAsync(passthrough, _ => new StagedConnector(options), hooks).ConfigureAwait(false);
    }
}

/// <summary>Which failure this fixture stages. Nothing here is configuration.
///
/// <para><c>--sync-state</c> stages a feed-shaped connector: <c>SyncState</c> declared (and
/// <c>PartitionedRead</c> withdrawn -- one opaque token cannot span partitions), FEED for every
/// dataset, no native scan, and a deterministic token per drained partition.
/// <c>--declare-sync-state-only</c> declares the same capability set but implements none of it, the
/// shape a conformance vector must FAIL. <c>--stable-ids</c> declares <c>StablePartitionIds</c> and
/// gives every partition the id <c>&lt;dataset&gt;:&lt;ordinal&gt;</c>.</para></summary>
internal sealed record FixtureOptions(
    bool HangHandshake,
    bool DieImmediately,
    bool WrongProtocolMajor,
    bool MisreportCapabilities,
    bool MisreportName,
    bool FailCheckTransient,
    bool ReportAbortSemanticsNone,
    bool UseGate,
    bool EndlessRead,
    bool IgnoreCancel,
    bool IgnoreShutdown,
    bool DeclareCheckpointableReads,
    bool SyncState,
    bool DeclareSyncStateOnly,
    bool StableIds)
{
    /// <summary>Splits argv into the fixture's own switches and what the SDK owns. The SDK's argv
    /// (<c>--pz-socket &lt;path&gt;</c>, and <c>--pz-manifest --out &lt;file&gt; --entrypoint
    /// &lt;rid&gt;=&lt;path&gt;</c>) is passed through verbatim so its own parser stays the one
    /// authority on it.</summary>
    public static (FixtureOptions Options, string[] Passthrough) Parse(string[] args)
    {
        var passthrough = new List<string>();
        bool hangHandshake = false, dieImmediately = false, wrongProtocolMajor = false;
        bool misreportCapabilities = false, misreportName = false;
        bool failCheckTransient = false, reportAbortSemanticsNone = false;
        bool useGate = false, endlessRead = false, ignoreCancel = false, ignoreShutdown = false;
        bool declareCheckpointableReads = false;
        bool syncState = false, declareSyncStateOnly = false, stableIds = false;

        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--pz-socket":
                    passthrough.Add(args[i]);
                    if (++i >= args.Length)
                    {
                        throw new ArgumentException("--pz-socket needs a socket path");
                    }

                    passthrough.Add(args[i]);
                    break;
                case "--pz-manifest":
                    passthrough.Add(args[i]);
                    break;
                case "--out":
                case "--entrypoint":
                    passthrough.Add(args[i]);
                    if (++i >= args.Length)
                    {
                        throw new ArgumentException($"{args[i - 1]} needs a value");
                    }

                    passthrough.Add(args[i]);
                    break;
                case "--hang-handshake":
                    hangHandshake = true;
                    break;
                case "--die-immediately":
                    dieImmediately = true;
                    break;
                case "--wrong-protocol-major":
                    wrongProtocolMajor = true;
                    break;
                case "--misreport-capabilities":
                    misreportCapabilities = true;
                    break;
                case "--misreport-name":
                    misreportName = true;
                    break;
                case "--fail-check-transient":
                    failCheckTransient = true;
                    break;
                case "--report-abort-semantics-none":
                    reportAbortSemanticsNone = true;
                    break;
                case "--use-gate":
                    useGate = true;
                    break;
                case "--endless-read":
                    endlessRead = true;
                    break;
                case "--ignore-cancel":
                    ignoreCancel = true;
                    break;
                case "--ignore-shutdown":
                    ignoreShutdown = true;
                    break;
                case "--declare-checkpointable-reads":
                    declareCheckpointableReads = true;
                    break;
                case "--sync-state":
                    syncState = true;
                    break;
                case "--declare-sync-state-only":
                    declareSyncStateOnly = true;
                    break;
                case "--stable-ids":
                    stableIds = true;
                    break;
                default:
                    throw new ArgumentException($"unrecognized argument '{args[i]}'");
            }
        }

        if (passthrough.Count == 0)
        {
            throw new ArgumentException("--pz-socket <path> or --pz-manifest is required");
        }

        return (new FixtureOptions(
            hangHandshake, dieImmediately, wrongProtocolMajor, misreportCapabilities, misreportName,
            failCheckTransient, reportAbortSemanticsNone, useGate, endlessRead, ignoreCancel, ignoreShutdown,
            declareCheckpointableReads, syncState, declareSyncStateOnly, stableIds), passthrough.ToArray());
    }
}
