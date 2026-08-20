namespace Pz.Engine.State;

/// <summary>A per-dataset incremental-extraction cursor position.
/// <see cref="Value"/> and <see cref="TypeName"/> are opaque strings as far as this record and
/// <see cref="WatermarkStore"/> are concerned -- canonicalization of cursor values (int/bigint/decimal/
/// date/timestamp) into these string forms is the caller's responsibility.</summary>
public sealed record Watermark(string Cursor, string TypeName, string Value, string RunId);
