using System.Globalization;
using Pz.Connectors.Abstractions;
using Pz.Core.Model;

namespace Pz.Engine.Planning;

/// <summary>Static (planning-time only, never touches the OS) memory budget:
/// <c>duckdb.memory_limit</c> (parsed to bytes) + <c>engine.threads * 6 * batch_bytes</c> (6 = the
/// bounded-channel capacity of 4 + 1 batch being produced + 1 being ingested) + a fixed 256MB overhead.
///
/// <see cref="DuckDbBytes"/> is null — with <see cref="DuckDbDisclaimer"/> explaining why — whenever
/// the byte count can't be known deterministically: <c>duckdb.memory_limit</c> unset (DuckDB's own
/// default is "80% of system RAM", which is machine-dependent and would make plan.json's byte-stability
/// guarantee depend on the machine that generated it) or set to something that isn't a fixed byte size
/// (e.g. DuckDB also accepts a bare percentage like "80%"). In either case the disclaimed amount is
/// treated as 0 in <see cref="TotalBytes"/> — the total is a documented lower bound, not a guess.</summary>
public sealed record MemoryBudget(
    long? DuckDbBytes, string? DuckDbDisclaimer, long ChannelBytes, long FixedOverheadBytes, long TotalBytes,
    string? DuckDbThreadsDisclaimer = null)
{
    /// <summary>The total above bounds how much memory pz and DuckDB may
    /// hold; it does NOT promise the workload fits inside <c>duckdb.memory_limit</c>. DuckDB's floor for
    /// materialising a table scales with that table's COLUMN COUNT times its thread count, neither of
    /// which appears in the formula — a 20,000-row x 1,000-column table can exhaust a 1GiB limit while
    /// this budget reports 1.63 GB of headroom, and lowering DuckDB's thread count alone is enough to
    /// make the same project succeed.
    ///
    /// The formula deliberately does not grow a column term. Column counts are not knowable here for the
    /// case that actually fails: a contract-less csv/json dataset has no declared schema at plan time,
    /// and planning is required to stay side-effect-free, so the
    /// planner cannot open the file to find out. A term present for declared contracts and silently
    /// absent for inferred ones would be worse than none — it would look authoritative exactly where it
    /// is blind. So this states the caveat instead, mirroring <see cref="DuckDbDisclaimer"/>'s existing
    /// "say what cannot be known" precedent.
    ///
    /// Non-null whenever <c>engine.duckdb.threads</c> is unset — note that is a DIFFERENT key from
    /// <c>engine.threads</c>, which drives the channel term but has no effect on DuckDB — because DuckDB
    /// then picks the machine's core count, making the real floor machine-dependent even though every
    /// byte printed above is reproducible.</summary>
    public const string ThreadsDisclaimer =
        "engine.duckdb.threads is not set (it is a different key from engine.threads, which sizes the " +
        "channel term above and does not reach DuckDB); DuckDB therefore uses the machine's core count. " +
        "DuckDB's memory floor scales with a table's column count times its thread count, so a wide " +
        "schema can exhaust duckdb.memory_limit even though this total says otherwise -- set " +
        "engine.duckdb.threads to make that floor deterministic.";

    /// <summary>4 channel capacity + 1 producing + 1 ingesting batch in flight.</summary>
    public const long ChannelSlotsPerThread = 6;

    public const long FixedOverhead = 256L * 1024 * 1024;

    public static MemoryBudget Compute(EngineConfig engine)
    {
        var batchBytes = engine.BatchBytes ?? BatchOptions.Default.TargetBatchBytes;
        var channelBytes = (long)engine.Threads * ChannelSlotsPerThread * batchBytes;
        var (duckDbBytes, disclaimer) = ParseDuckDbMemoryLimit(engine.DuckDb?.MemoryLimit);
        var totalBytes = (duckDbBytes ?? 0L) + channelBytes + FixedOverhead;
        return new MemoryBudget(duckDbBytes, disclaimer, channelBytes, FixedOverhead, totalBytes,
            engine.DuckDb?.Threads is null ? ThreadsDisclaimer : null);
    }

    private static (long? Bytes, string? Disclaimer) ParseDuckDbMemoryLimit(string? memoryLimit)
    {
        if (string.IsNullOrWhiteSpace(memoryLimit))
        {
            return (null,
                "duckdb.memory_limit is not set; DuckDB itself defaults to 80% of the machine's RAM at run " +
                "time, which cannot be computed deterministically here, so it is treated as 0 in the total below.");
        }

        if (TryParseByteSize(memoryLimit, out var bytes))
        {
            return (bytes, null);
        }

        return (null,
            $"duckdb.memory_limit '{memoryLimit}' is not a fixed byte size (e.g. DuckDB also accepts a bare " +
            "percentage such as '80%'); treated as 0 in the total below.");
    }

    /// <summary>Accepts DuckDB-style byte-size literals: a decimal magnitude followed by an optional
    /// unit -- <c>B</c>/<c>KB</c>/<c>MB</c>/<c>GB</c>/<c>TB</c> (1000-based) or
    /// <c>KiB</c>/<c>MiB</c>/<c>GiB</c>/<c>TiB</c> (1024-based) -- case-insensitive, optional whitespace
    /// between the number and the unit. A bare number with no unit is treated as bytes. Anything that
    /// isn't this shape (e.g. DuckDB's own percentage form) returns false.</summary>
    internal static bool TryParseByteSize(string text, out long bytes)
    {
        bytes = 0;
        var trimmed = text.Trim();

        var digits = 0;
        while (digits < trimmed.Length && (char.IsAsciiDigit(trimmed[digits]) || trimmed[digits] == '.'))
        {
            digits++;
        }

        if (digits == 0)
        {
            return false;
        }

        if (!double.TryParse(trimmed[..digits], NumberStyles.Float, CultureInfo.InvariantCulture, out var magnitude))
        {
            return false;
        }

        var unit = trimmed[digits..].Trim();
        var multiplier = unit.ToUpperInvariant() switch
        {
            "" or "B" => 1L,
            "KB" => 1_000L,
            "MB" => 1_000_000L,
            "GB" => 1_000_000_000L,
            "TB" => 1_000_000_000_000L,
            "KIB" => 1024L,
            "MIB" => 1024L * 1024,
            "GIB" => 1024L * 1024 * 1024,
            "TIB" => 1024L * 1024 * 1024 * 1024,
            _ => -1L,
        };

        if (multiplier < 0)
        {
            return false;
        }

        bytes = (long)Math.Round(magnitude * multiplier, MidpointRounding.AwayFromZero);
        return true;
    }
}
