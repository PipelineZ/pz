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
/// data plane by ticket. <see cref="Drained"/> is the whole point: CommitWrite must not run until the
/// write stream has been read to end-of-stream, so the control plane awaits it and the data plane
/// completes it (or faults it, so a torn stream surfaces as a failed commit rather than a lost
/// suffix).</summary>
internal sealed class WriteSessionState(string sessionId, ISinkWriteSession session)
{
    public string SessionId { get; } = sessionId;
    public ISinkWriteSession Session { get; } = session;
    public TaskCompletionSource Drained { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
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
