namespace Pz.Core.Model;

/// <summary>Shared resolution of a dataset's incremental-cursor declared type, used by both
/// compile-time window validation (DagCompiler PZ0213) and the engine's bound computation
/// (SourceLoadExecutor). Two declaration routes exist: a <c>columns:</c> contract typing the
/// cursor like any other column (contract mode), or — for raw-envelope connectors (http) whose
/// records land as untyped payload text — the connector's <c>cursor</c>/<c>cursor_type</c>
/// dataset options (the raw-mode cursor convention; the option must name the SAME column as
/// <c>incremental.cursor</c>, which the connector itself also enforces). When a non-empty
/// contract is declared it owns cursor typing outright — raw-mode options are never consulted
/// (the http connector rejects that combination as a config error anyway).</summary>
public static class CursorContract
{
    public static readonly string[] AllowedTypes = ["int", "bigint", "decimal", "date", "timestamp"];

    public static string? ResolveDeclaredType(DatasetDef dataset)
    {
        if (dataset.SyncMode?.Incremental is not { } incremental)
        {
            return null;
        }

        if (dataset.Columns is { Count: > 0 } columns)
        {
            return columns.TryGetValue(incremental.Cursor, out var contractType)
                && Array.IndexOf(AllowedTypes, contractType) >= 0
                    ? contractType
                    : null;
        }

        return dataset.Options.TryGetValue("cursor", out var rawCursor)
            && rawCursor?.ToString() == incremental.Cursor
            && dataset.Options.TryGetValue("cursor_type", out var rawType)
            && rawType?.ToString() is { } type
            && Array.IndexOf(AllowedTypes, type) >= 0
                ? type
                : null;
    }
}
