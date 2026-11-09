using System.Text;

namespace Pz.Core.Validation;

public sealed class PzValidationException : Exception
{
    public IReadOnlyList<PzError> Errors { get; }

    /// <summary>True when at least one error is not the author's to fix: something pz depends on failed rather than
    /// refused (a connector that failed while the plan probed it). A verb then exits fatal (3) rather than config
    /// (2). False for every config and validation error, which is the default.</summary>
    public bool IsFatal { get; init; }

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
