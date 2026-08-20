using System.Diagnostics;

namespace Pz.Diagnostics.Otel;

/// <summary>The single <see cref="ActivitySource"/> every span in the engine is created from —
/// BCL-only (<c>System.Diagnostics.DiagnosticSource</c> is part of the shared framework, not a NuGet
/// package), so this lives in Pz.Diagnostics without breaking its BCL-only rule.
/// <see cref="ActivitySource.StartActivity(string)"/> is a documented BCL no-op (returns
/// <c>null</c>, allocates nothing material) when no <see cref="ActivityListener"/> is registered
/// anywhere in the process — every emission site in the engine relies on exactly this to make span
/// emission zero-cost when OTel export isn't configured (<c>Pz.Cli</c>'s <c>OtelProviders</c> is the
/// only place that ever registers a listener/exporter).</summary>
public static class PzActivitySource
{
    /// <summary>The public span-source name; also used as the
    /// <see cref="ActivityListener.ShouldListenTo"/> match key wherever a listener is configured.</summary>
    public const string Name = "Pz.Engine";

    public static readonly ActivitySource Instance = new(Name);
}
