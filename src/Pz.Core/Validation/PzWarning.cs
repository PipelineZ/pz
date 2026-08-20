namespace Pz.Core.Validation;

/// <summary>A non-blocking validation warning. Same shape and PZ#### discipline as
/// <see cref="PzError"/> (code, file, cause, next-step hint), but flows back through
/// <c>CompiledDag.Warnings</c> instead of throwing — warnings never change the exit code
/// and never block a run.</summary>
public sealed record PzWarning(string Code, string Message, string? File, int? Line, string? Hint);
