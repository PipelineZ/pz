using Pz.Core.Dag;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.TestSupport.State;

namespace Pz.Engine.Tests.Artifacts;

public sealed class LocalRunArtifactStoreContractTests : RunArtifactStoreContract
{
    /// <summary>Each store's project directory, so <see cref="CorruptStoredRun"/> can find its
    /// run_results.json without <see cref="NewStore"/> handing back anything but a fresh, independent
    /// store.</summary>
    private readonly Dictionary<IRunArtifactStore, string> _projectDirs = [];

    protected override IRunArtifactStore NewStore()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"pz-run-artifacts-{Guid.NewGuid():N}");
        var store = new LocalRunArtifactStore(dir);
        _projectDirs[store] = dir;
        return store;
    }

    protected override NodeResult SucceededSourceLoad(string nodeId, string name) =>
        new(new NodeId(nodeId), NodeKind.SourceLoad, name, NodeStatus.Success, 0, TimeSpan.Zero, null);

    /// <summary>Locally, "present but unreadable" is garbage bytes over run_results.json — mirrors
    /// RunResultsReaderTests' own corrupt-file cases.</summary>
    protected override void CorruptStoredRun(IRunArtifactStore store, string runId) =>
        File.WriteAllText(
            Path.Combine(_projectDirs[store], ".pz", "runs", runId, "run_results.json"), "{ not json at all");
}
