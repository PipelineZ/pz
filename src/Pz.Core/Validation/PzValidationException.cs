using System.Text;

namespace Pz.Core.Validation;

public sealed class PzValidationException : Exception
{
    public IReadOnlyList<PzError> Errors { get; }

    public PzValidationException(IReadOnlyList<PzError> errors)
        : base(BuildMessage(errors))
    {
        if (errors.Count == 0)
        {
            throw new ArgumentException("PzValidationException requires at least one error.", nameof(errors));
        }

        Errors = errors;
    }

    private static string BuildMessage(IReadOnlyList<PzError> errors)
    {
        var sb = new StringBuilder();
        sb.Append(errors.Count).Append(" validation error(s):");
        foreach (var error in errors)
        {
            sb.Append('\n').Append("  ").Append(error);
        }

        return sb.ToString();
    }
}
