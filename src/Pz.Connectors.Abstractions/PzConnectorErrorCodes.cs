namespace Pz.Connectors.Abstractions;

/// <summary>Well-known values of <see cref="PzConnectorException.Code"/> that change how the engine reports a
/// failure. Any other value (a connector's own <c>PZ####</c>, or none) is reported as a connector failure.</summary>
public static class PzConnectorErrorCodes
{
    /// <summary>The connector refuses the config it was given: a missing or invalid option, or options that
    /// cannot be combined. Throw it non-transient, from <see cref="ISourceConnector.OpenAsync"/>,
    /// <see cref="ISinkConnector.OpenAsync"/>, <see cref="INaturalReadShapeSource.GetNaturalReadShape"/> or any
    /// other offline reading of the config. The engine then reports PZ0301 and tells the author to fix the
    /// options, naming the connection and the dataset or output itself, so the message should name the option
    /// but not the connection. Never set it on a network, auth or data failure: those are not the author's
    /// config to fix.</summary>
    public const string Config = "config";

    /// <summary>A connector's own unhandled exception, wrapped by the SDK that hosts it (for example a
    /// <c>NullReferenceException</c>): a defect in the connector, never a config error. Connectors do not set it
    /// themselves.</summary>
    public const string Unhandled = "unhandled";
}
