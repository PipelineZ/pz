using Pz.Diagnostics.Events;
using Pz.State.SqlServer;

namespace Pz.Cli.Rendering;

/// <summary><see cref="SqlEventSink"/> cannot implement <see cref="IEventRenderer"/> itself (layering:
/// <c>Pz.State.SqlServer</c> may not reference <c>Pz.Cli</c>), so this wraps it on the <c>Pz.Cli</c>
/// side of that boundary.
///
/// <see cref="Dropped"/> and <see cref="DisposeAsync"/> forward straight through so the composition site
/// (<c>RunCommand.ExecuteRun</c>) can dispose the sink and read its final drop count without knowing it
/// is looking at a <see cref="SqlEventSink"/> underneath a <see cref="CompositeEventRenderer"/>.</summary>
internal sealed class SqlEventRenderer(SqlEventSink sink) : IEventRenderer, IAsyncDisposable
{
    public void Render(RunEvent evt) => sink.Write(evt);

    public long Dropped => sink.Dropped;

    public ValueTask DisposeAsync() => sink.DisposeAsync();
}
