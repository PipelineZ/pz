namespace Pz.Connectors.Abstractions;

/// <summary>How a dataset with no declared sync mode
/// (mode: auto) naturally reads. Full = stateless re-read; Feed = the connector manages an
/// opaque resume token (sync-state kind).</summary>
public enum NaturalReadShape { Full, Feed }

/// <summary>Optional additive interface on <see cref="ISource"/> implementations. Sources that
/// never manage their own resume state don't implement it; the engine then assumes
/// <see cref="NaturalReadShape.Full"/>. Must be side-effect-free and offline (plan-time call).</summary>
public interface INaturalReadShapeSource
{
    NaturalReadShape GetNaturalReadShape(DatasetSpec spec);
}
