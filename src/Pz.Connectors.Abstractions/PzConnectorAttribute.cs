namespace Pz.Connectors.Abstractions;

/// <summary>Registers a connector implementation under its logical name (e.g. "postgres").
/// Assembly-level and repeatable: one package may ship several connectors.</summary>
[AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)]
public sealed class PzConnectorAttribute(string name, Type connectorType) : Attribute
{
    public string Name { get; } = name;
    public Type ConnectorType { get; } = connectorType;
}
