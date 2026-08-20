namespace Pz.Core.Validation;

public sealed class PzConfigException : Exception
{
    public PzError Error { get; }

    public PzConfigException(PzError error)
        : base(error.ToString())
    {
        Error = error;
    }
}
