using System.Diagnostics.CodeAnalysis;

namespace Pz.Connectors.Abstractions;

/// <summary>A sink whose destination DuckDB cannot write to directly (an SFTP server) but which can take a
/// finished file: the engine COPYs the staged output to local files in the format the sink names, with
/// DuckDB's own writer (every format, nested types included), then hands those files to
/// <see cref="CommitSpoolAsync"/>. Implemented by the <see cref="ISink"/> a connector opens; over PCP a sink
/// declares <see cref="ConnectorCapabilities.SpoolWrites"/>. Tried after <see cref="ISink.TryGetNativeCopy"/>
/// declines and never under <c>engine.force_universal</c>, which keeps the Arrow path.</summary>
public interface ISpoolingSink
{
    /// <summary>The spool plan for <paramref name="spec"/>, or false to take the Arrow path. Cheap and offline,
    /// like <see cref="ISink.TryGetNativeCopy"/>: the planner probes it.</summary>
    bool TryGetSpool(OutputSpec spec, [NotNullWhen(true)] out SpoolPlan? plan);

    /// <summary>Delivers the spooled files, then returns what was written. Each file is a complete file of the
    /// planned format; <see cref="SpooledFile.Folder"/> is the rendered partition folder (empty without
    /// <see cref="SpoolPlan.PartitionColumn"/>). The engine deletes the files after this returns or throws, so a
    /// sink must finish reading them first; a sink should make each delivery atomic (write a temporary name,
    /// then rename) so a failure leaves the destination as it was.</summary>
    ValueTask<WriteResult> CommitSpoolAsync(OutputSpec spec, IReadOnlyList<SpooledFile> files, CancellationToken ct);
}

/// <summary>How the engine writes the spool: <see cref="CopyOptions"/> is the body of DuckDB's
/// <c>COPY … TO '&lt;file&gt;' (…)</c> clause (<c>FORMAT parquet</c>), <see cref="Extension"/> the file
/// extension.</summary>
public sealed record SpoolPlan(string CopyOptions, string Extension)
{
    /// <summary>Short, user-facing description (e.g. "parquet via sftp upload"), for planner reasons only.</summary>
    public string? Mechanism { get; init; }

    /// <summary>Statements to run before the COPY (<c>INSTALL excel; LOAD excel</c>).</summary>
    public IReadOnlyList<string> SetupStatements { get; init; } = [];

    /// <summary>As <see cref="NativeCopy.NestedAsJson"/>: the format has no nested types (csv, xlsx), so STRUCT,
    /// LIST and MAP columns are written as JSON text.</summary>
    public bool NestedAsJson { get; init; }

    /// <summary>A timestamp or date column whose value picks each row's folder through
    /// <see cref="Paths.PathTemplate"/> (the output's <c>partition_by</c>); null for one file.</summary>
    public string? PartitionColumn { get; init; }

    /// <summary>The calendar-token template (<c>{yyyy}/{MM}</c>) rendered from <see cref="PartitionColumn"/>.</summary>
    public string? PathTemplate { get; init; }
}

/// <summary>One spooled file: the partition <paramref name="Folder"/> it belongs in (empty when unpartitioned),
/// its local path and its row count.</summary>
public sealed record SpooledFile(string Folder, string LocalPath, long Rows);
