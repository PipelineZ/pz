namespace Pz.Engine.Preview;

/// <summary>How a preview cell is written to JSON: as a number, a boolean, null, or a string.</summary>
public enum PreviewCellKind { Null, Number, Boolean, Text }

/// <summary>One sampled value. <see cref="PreviewCellKind.Number"/> text is a valid JSON number literal and
/// <see cref="PreviewCellKind.Boolean"/> text is <c>true</c>/<c>false</c>; <see cref="Text"/> is null only for
/// <see cref="PreviewCellKind.Null"/>.</summary>
public readonly record struct PreviewCell(PreviewCellKind Kind, string? Text);

/// <summary>A sampled column: its DuckDB type name and whether its values are redacted.</summary>
public sealed record PreviewColumn(string Name, string Type, bool Redacted);

/// <summary>What <c>pz preview</c> prints, in the field order of the JSON contract with PipelineX.
/// <see cref="Mode"/> is <c>run</c> or <c>fresh</c>; <see cref="TotalRows"/> is null for a fresh preview,
/// which is computed from source slices and so has no meaningful total.</summary>
public sealed record PreviewResult(string Node, string Mode, string? RunId, IReadOnlyList<PreviewColumn> Columns,
    IReadOnlyList<IReadOnlyList<PreviewCell>> Rows, long? TotalRows, int Limit, bool Sliced);
