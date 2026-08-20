namespace Pz.Core.Model;

/// <summary>The offline half of entity validation. An entity name is spelled exactly as its own system
/// spells it -- <c>dbo.orders</c>, <c>curated</c>, <c>/v2/events</c>, <c>repos/acme/pz/issues</c> -- so
/// pz checks STRUCTURE only and stays connector-agnostic. Existence, permissions, and schema remain
/// <c>--connect</c> work. A connector-declared entity grammar would catch more typos offline but needs
/// a new member on the connector contract, and leaving the ABI untouched is worth more than the extra
/// typo class.</summary>
public static class EntityName
{
    /// <summary>The reason <paramref name="name"/> cannot name anything, or null when it is well
    /// formed. The returned clause completes "entity '&lt;name&gt;' ..." in a PZ0344 message.</summary>
    public static string? Problem(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return "is empty";
        }

        if (name.Any(char.IsWhiteSpace))
        {
            return "contains whitespace";
        }

        return name.Split('.').Any(segment => segment.Length == 0)
            ? "has an empty dotted segment"
            : null;
    }
}
