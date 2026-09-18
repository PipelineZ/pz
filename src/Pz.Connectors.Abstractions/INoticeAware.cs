namespace Pz.Connectors.Abstractions;

/// <summary>Implemented by an ISource/ISink that wants to surface a non-fatal, human-readable notice
/// during a run without failing the node (e.g. sftp's unpinned host key). The engine calls UseNotice at
/// most once per opened ISource/ISink, after OpenAsync returns and before any plan/read/write call --
/// the same ordering guarantee <see cref="IOperationGateAware.UseOperationGate"/> documents, and the
/// reason a connector needs no dedup of its own even though a node may re-open its source/sink on
/// retry. A connector must still behave correctly with no subscriber (the callback given may be a
/// no-op). Additive: existing connectors that do not implement this are unaffected.</summary>
public interface INoticeAware
{
    void UseNotice(Action<string> notice);
}
