using System.Text;

namespace Pz.Core.Validation;

public sealed record PzError(string Code, string Message, string? File, int? Line, string? Hint)
{
    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append(Code).Append(": ").Append(Message);

        if (File is not null)
        {
            sb.Append(" (").Append(File);
            if (Line is not null)
            {
                sb.Append(':').Append(Line);
            }

            sb.Append(')');
        }

        if (Hint is not null)
        {
            sb.Append(" — hint: ").Append(Hint);
        }

        return sb.ToString();
    }
}
