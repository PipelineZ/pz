using System.Security.Cryptography;
using System.Text;

namespace Pz.Core.Dag;

public readonly record struct NodeId(string Value)
{
    public static NodeId Compute(string canonicalContent) =>
        new(Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(canonicalContent)).AsSpan(0, 8)));

    public override string ToString() => Value;
}
