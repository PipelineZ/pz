using Pz.Diagnostics.Events;

namespace Pz.Cli.Rendering;

/// <summary>One rendering strategy over the typed run-event stream: implementations receive every
/// <see cref="RunEvent"/> published to the <see cref="RunEventBus"/>, in publish order,
/// via <see cref="RendererPump"/>. Rendering is presentation-only and best-effort — the crash-safe
/// <c>run_results.json</c> snapshot path (<see cref="Pz.Cli.Commands.SnapshotRunEvents"/>) never depends
/// on any renderer running or succeeding.</summary>
public interface IEventRenderer
{
    void Render(RunEvent evt);
}
