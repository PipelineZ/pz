using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Pz.Connectors.Abstractions;

/// <summary>A native-scan source that can narrow its own scan to the named columns. The list is a subset of the
/// dataset's columns, unqualified; no predicate is ever passed, because the engine's predicates are DuckDB
/// dialect and the connector's inner query may not be.</summary>
public interface IProjectingNativeScan
{
    bool TryGetNativeScan(DatasetSpec spec, IReadOnlyList<string> columns,
        [NotNullWhen(true)] out NativeScan? scan);
}
