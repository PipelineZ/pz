using Apache.Arrow;
using Pz.Connectors.Abstractions.Formats;

namespace Pz.Connectors.Toolkit.Formats;

/// <summary>The toolkit's NDJSON write surface — the go-forward home for connector NDJSON writing.
/// Delegates to the frozen <see cref="NdjsonCodec"/> so output stays
/// byte-identical while callers migrate off the Abstractions type (which stays forever,
/// additive-only, but stops gaining callers).</summary>
public static class NdjsonWriteCodec
{
    public static Task WriteAsync(RecordBatch batch, Stream ndjson, CancellationToken ct)
        => NdjsonCodec.WriteAsync(batch, ndjson, ct);
}
