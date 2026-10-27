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
    /// <summary><see cref="NodeSqlEvent"/> is never persisted (it can carry a value <c>env()</c> inlined); the sink
    /// also refuses it, so this holds for any other caller of the sink.</summary>
    public void Render(RunEvent evt)
    {
        if (evt is NodeSqlEvent)
        {
            return;
        }

        sink.Write(evt);
    }

    public long Dropped => sink.Dropped;

    public ValueTask DisposeAsync() => sink.DisposeAsync();
}
