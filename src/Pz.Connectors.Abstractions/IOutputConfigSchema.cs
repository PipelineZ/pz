namespace Pz.Connectors.Abstractions;

/// <summary>Optional sink capability: a JSON Schema (draft 2020-12) for the connector-owned subset of
/// a <c>write:</c>/<c>sink()</c> call's options -- the write-side mirror of
/// <see cref="IConnector.DatasetConfigSchema"/>, which already gives a read-side <c>columns:</c>/dataset
/// option an <c>additionalProperties: false</c> typo check for free. A sink never sees the keys the
/// engine owns (<c>strategy</c>, <c>keys</c>, <c>duplicates</c>, <c>on_delete</c>, <c>schema_policy</c>,
/// <c>retry</c>) -- both the YAML <c>write:</c> loader and the <c>sink()</c> kwarg reader strip them
/// before a connector option dictionary is ever built -- so a schema declared here must describe only
/// what is left, and never needs to (or may usefully) list those names.
///
/// A sink that does not implement this interface is validated exactly as before this capability
/// existed: its output options reach the connector unchecked, and a typo among them is silently
/// ignored. Implementing it is additive-only -- a NEW capability interface, not a new member on
/// <see cref="ISinkConnector"/>, so every existing sink keeps compiling untouched.</summary>
public interface IOutputConfigSchema
{
    /// <summary>JSON Schema for this sink's own write() options.</summary>
    string OutputConfigSchema { get; }
}
