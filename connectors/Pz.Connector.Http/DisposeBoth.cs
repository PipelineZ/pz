namespace Pz.Connector.Http;

/// <summary>Owns two disposables as one: a credential's loaded certificate and the transport built for it.</summary>
internal sealed class DisposeBoth(IDisposable first, IDisposable second) : IDisposable
{
    public void Dispose()
    {
        first.Dispose();
        second.Dispose();
    }
}
