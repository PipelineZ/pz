using System.Text.Json;
using Pz.Engine.State;
using Pz.TestSupport.State;

namespace Pz.Engine.Tests.State;

public sealed class LocalKeyedStateStoreContractTests : KeyedStateStoreContract
{
    /// <summary>Each store's backing file, so <see cref="CorruptStoredState"/> can find it without
    /// <see cref="NewStore"/> handing back anything but a fresh, independent store.</summary>
    private readonly Dictionary<IKeyedStateStore<TestEntry>, string> _files = [];

    protected override IKeyedStateStore<TestEntry> NewStore()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"pz-kv-{Guid.NewGuid():N}");
        var store = new KeyedJsonStateStore<TestEntry>(dir, "contract.json", "entries", "Contract file",
            readEntry: static entry =>
            {
                var value = entry.GetProperty("value").GetString();
                var runId = entry.GetProperty("runId").GetString();
                return value is null || runId is null ? null : new TestEntry(value, runId);
            },
            writeEntry: static (writer, e) =>
            {
                writer.WriteString("value", e.Value);
                writer.WriteString("runId", e.RunId);
            });

        _files[store] = Path.Combine(dir, "contract.json");
        return store;
    }

    /// <summary>Locally, "present but unreadable" is garbage bytes over the JSON file.</summary>
    protected override void CorruptStoredState(IKeyedStateStore<TestEntry> store)
        => File.WriteAllText(_files[store], "{ not json at all");
}
