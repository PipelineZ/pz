using System.Security.Cryptography;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol;

namespace PcpFakeConnector;

/// <summary>What a minted data-plane ticket authorizes. A ticket names one direction and one
/// already-planned unit of work, so the data plane never has to interpret configuration.</summary>
internal abstract record TicketEntry;

/// <summary>Connector -> host: the data plane writes <paramref name="Schema"/> then every batch
/// <paramref name="Partition"/> yields. The schema is captured at OpenReadStream time so an empty
/// partition still produces a well-formed stream; the ABI requires it to equal the batches' schema
/// exactly.</summary>
internal sealed record ReadTicket(
    Schema Schema,
    IDatasetPartition Partition,
    BatchOptions Options,
    CancellationToken OpToken) : TicketEntry;

/// <summary>Host -> connector: the data plane reads batches off the stream into
/// <paramref name="Session"/> until end-of-stream, then releases CommitWrite.</summary>
internal sealed record WriteTicket(WriteSessionState Session) : TicketEntry;

/// <summary>One open sink write session, reachable from the control plane by session id and from the
/// data plane by ticket.
///
/// <para><see cref="Drained"/> is one half of the point: CommitWrite must not run until the write
/// stream has been read to end-of-stream, so the control plane awaits it and the data plane completes
/// it (or faults it, so a torn stream surfaces as a failed commit rather than a lost suffix).</para>
///
/// <para><see cref="TryBeginPump"/>/<see cref="Close"/> are the other half: the data plane writes
/// batches into <see cref="Session"/> from its own task, so the control plane must not commit, abort,
/// or dispose the session while a write is in flight. Close shuts the door on a pump that has not
/// started and hands back the one that has, and <see cref="Cancellation"/> is what stops it — which is
/// what makes AbortWrite and Cancel able to end a write at all.</para></summary>
internal sealed class WriteSessionState(string sessionId, string opId, ISinkWriteSession session)
{
    private readonly Lock _gate = new();
    private Task? _pump;
    private bool _closed;

    public string SessionId { get; } = sessionId;

    /// <summary>The op this session belongs to, so <c>Cancel {opId}</c> can reach its write pump.</summary>
    public string OpId { get; } = opId;

    public ISinkWriteSession Session { get; } = session;

    public CancellationTokenSource Cancellation { get; } = new();

    public TaskCompletionSource Drained { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Data plane: claims the right to write into the session, handing over a task that
    /// completes when it stops writing (successfully or not). Returns false once the control plane has
    /// closed the session — which is what keeps a batch out of a session that is being aborted.</summary>
    public bool TryBeginPump(Task pump)
    {
        lock (_gate)
        {
            if (_closed)
            {
                return false;
            }

            _pump = pump;
            return true;
        }
    }

    /// <summary>Control plane: closes the session to any further pumping and returns the pump already
    /// running, or null if none ever claimed it. The caller must await what it gets back before
    /// touching the session — the lock is what makes "either it started before I closed, or it can
    /// never start" true.</summary>
    public Task? Close()
    {
        lock (_gate)
        {
            _closed = true;
            return _pump;
        }
    }
}

/// <summary>Mints and burns the single-use data-plane tickets.
///
/// <para>A ticket is 16 cryptographically random bytes and is valid for exactly one connection: the
/// registry removes it the moment it is presented. An unknown ticket and a replayed one are therefore
/// the same case, and both leave the caller with nothing to serve — the data plane closes the
/// connection without writing a byte.</para></summary>
internal sealed class TicketRegistry
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, TicketEntry> _entries = new(StringComparer.Ordinal);

    public byte[] Mint(TicketEntry entry)
    {
        var ticket = RandomNumberGenerator.GetBytes(ProtocolConstants.TicketLength);
        lock (_gate)
        {
            _entries.Add(Key(ticket), entry);
        }

        return ticket;
    }

    /// <summary>Resolves a presented ticket and removes it in the same critical section, so two
    /// connections racing on one ticket can never both be served.</summary>
    public bool TryBurn(ReadOnlySpan<byte> ticket, out TicketEntry entry)
    {
        if (ticket.Length != ProtocolConstants.TicketLength)
        {
            entry = null!;
            return false;
        }

        lock (_gate)
        {
            return _entries.Remove(Key(ticket), out entry!);
        }
    }

    private static string Key(ReadOnlySpan<byte> ticket) => Convert.ToHexString(ticket);
}
