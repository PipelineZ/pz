using Pz.Connectors.Abstractions;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;

namespace Pz.Engine.Validation;

/// <summary>Tier 5 of `pz validate --connect`: concurrent online connectivity probes for
/// every source/sink connection, plus schema drift detection for every declared source dataset. Fetched
/// schemas for datasets WITHOUT a declared `columns:` contract are returned so the caller
/// (<c>ValidateCommand</c>) can persist them via <see cref="Pz.Engine.Artifacts.SchemaCacheWriter"/>.</summary>
/// <param name="ExtraColumnsByDataset">Additive: for a dataset WITH a declared `columns:` contract,
/// the fetched field names that exist live but are not in the contract -- "source.dataset" keyed, same
/// as <paramref name="FetchedSchemas"/>. A contract prunes on read (extra fetched columns are never a
/// drift error -- see <see cref="ConnectivityValidator"/>'s own doc), so this is the only signal that a
/// contract-bearing dataset's live shape has grown since the contract was written. Absent (no key) for
/// a dataset with no extra columns, and never populated for a contract-less dataset (its whole fetched
/// shape already rides <paramref name="FetchedSchemas"/>).</param>
public sealed record ConnectivityResult(
    IReadOnlyList<PzError> Errors,
    IReadOnlyDictionary<string, string> FetchedSchemas,
    IReadOnlyDictionary<string, IReadOnlyList<string>>? ExtraColumnsByDataset = null)
{
    /// <summary>What a SUCCESSFUL connection check had to say (sftp's presented host-key fingerprint, a
    /// connector stating that it has no offline probe), one line per connection that said anything.</summary>
    public IReadOnlyList<string> Notes { get; init; } = [];
}

public static class ConnectivityValidator
{
    private const string ConnectHint = "check the connection settings and that the service is reachable";
    private const string DriftHint = "fix the declared columns: contract or the underlying schema";

    /// <summary>Per-probe timeout: a firewalled/unreachable host must never hang
    /// `pz validate --connect` forever. Applied individually to every <see cref="IConnector.CheckConnectionAsync"/>
    /// call AND every drift-phase <see cref="ISourceConnector.OpenAsync"/>/<see cref="ISource.GetSchemaAsync"/>
    /// call below -- each one gets its own fresh window, not one shared budget for the whole run. Internal
    /// rather than a `RunAsync` parameter, with a settable seam so tests can inject a near-zero
    /// timeout instead of waiting out 30 real seconds; see <c>Pz.Engine.csproj</c>'s
    /// <c>InternalsVisibleTo("Pz.Engine.Tests")</c>.</summary>
    internal static TimeSpan ProbeTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Runs every source/sink's <see cref="IConnector.CheckConnectionAsync"/> concurrently
    /// (`Task.WhenAll` over one lambda per def -- a failure or a thrown exception from one probe never
    /// prevents another from completing or being reported), then -- independently -- opens
    /// each source once and fetches every declared dataset's schema to compare against its `columns:`
    /// contract. A connector name absent from <paramref name="registry"/> is skipped here (mirrors
    /// <see cref="ConnectorConfigValidator"/>: an unresolvable connector already failed upstream).</summary>
    public static async Task<ConnectivityResult> RunAsync(
        PzProject project, ConnectorRegistry registry, CancellationToken ct)
    {
        var errors = new List<PzError>();

        // One probe per connection, not one per direction. A connector
        // that implements both halves (postgres) would otherwise open the same database twice, and
        // report one unreachable host as two errors.
        var connectionProbes = new List<Task<(PzError? Error, string? Note)>>();
        foreach (var connection in project.Connections)
        {
            IConnector? connector = registry.TryGetSource(connection.Connector, out var source) ? source
                : registry.TryGetSink(connection.Connector, out var sink) ? sink
                : null;
            if (connector is not null)
            {
                connectionProbes.Add(ProbeConnectionAsync(connector, registry.ConfigFor(connection), "connection",
                    connection.Name, connection.FilePath, ct));
            }
        }

        // All probes are already running (added to the list as Tasks, started by ProbeConnectionAsync's
        // first await) before WhenAll is reached -- this is what makes them concurrent, not sequential.
        var connectionResults = await Task.WhenAll(connectionProbes).ConfigureAwait(false);
        errors.AddRange(connectionResults.Select(r => r.Error).Where(e => e is not null)!);
        var notes = connectionResults.Select(r => r.Note).Where(n => n is not null).Cast<string>().ToList();

        var fetchedSchemas = new Dictionary<string, string>(StringComparer.Ordinal);
        var extraColumns = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
        foreach (var source in project.Connections)
        {
            if (!registry.TryGetSource(source.Connector, out var connector) || source.Datasets.Count == 0)
            {
                continue;
            }

            await ProbeSourceSchemasAsync(connector, registry.ConfigFor(source), source, errors, fetchedSchemas,
                extraColumns, ct).ConfigureAwait(false);
        }

        return new ConnectivityResult(errors, fetchedSchemas, extraColumns) { Notes = notes };
    }

    /// <summary>A successful check's own <see cref="ConnectionCheck.Message"/> is surfaced as an
    /// informational note (e.g. sftp's unpinned-host-key fingerprint) -- distinct from a FAILED
    /// check's message, which becomes the detail of a <see cref="PzErrorCode.ConnectionCheckFailed"/>
    /// error below.</summary>
    private static async Task<(PzError? Error, string? Note)> ProbeConnectionAsync(IConnector connector,
        ConnectorConfig config, string kind, string name, string filePath, CancellationToken ct)
    {
        try
        {
            var check = await WithTimeoutAsync(
                t => connector.CheckConnectionAsync(config, t), ct).ConfigureAwait(false);
            if (check.Ok)
            {
                var note = string.IsNullOrEmpty(check.Message) ? null : $"{kind} '{name}': {check.Message}";
                return (null, note);
            }

            var detail = string.IsNullOrEmpty(check.Message) ? "" : $": {check.Message}";
            return (new PzError(PzErrorCode.ConnectionCheckFailed,
                $"{kind} '{name}' connection check failed{detail}", filePath, null, ConnectHint), null);
        }
        catch (ProbeTimedOutException)
        {
            return (TimeoutError($"{kind} '{name}' connection", filePath), null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A connector's thrown foreign exception may echo a raw engine/driver error verbatim; redact
            // before it reaches this probe's PzError message. A Pz-family exception passes through
            // unredacted (see
            // MessageRedaction.Redact(Exception)'s trust boundary doc).
            return (new PzError(PzErrorCode.ConnectionCheckFailed,
                $"{kind} '{name}' connection check threw: {MessageRedaction.Redact(ex)}", filePath, null, ConnectHint), null);
        }
    }

    /// <summary>Opens <paramref name="source"/> exactly once and fetches every declared
    /// dataset's schema through it; any failure while opening or fetching is caught and reported as one
    /// PZ0330 naming the source, without aborting probing of any OTHER source.</summary>
    private static async Task ProbeSourceSchemasAsync(ISourceConnector connector, ConnectorConfig config,
        ConnectionDef source, List<PzError> errors, Dictionary<string, string> fetchedSchemas,
        Dictionary<string, IReadOnlyList<string>> extraColumns, CancellationToken ct)
    {
        ISource? opened = null;
        try
        {
            opened = await WithTimeoutAsync(
                t => connector.OpenAsync(config, t), ct).ConfigureAwait(false);
            foreach (var dataset in source.Datasets)
            {
                await ProbeDatasetSchemaAsync(opened, source, dataset, errors, fetchedSchemas, extraColumns, ct)
                    .ConfigureAwait(false);
            }
        }
        catch (ProbeTimedOutException)
        {
            errors.Add(TimeoutError($"source '{source.Name}' connection", source.FilePath));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            errors.Add(new PzError(PzErrorCode.ConnectionCheckFailed,
                $"source '{source.Name}' connection check threw: {ex.Message}", source.FilePath, null, ConnectHint));
        }
        finally
        {
            if (opened is not null)
            {
                await opened.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private static async Task ProbeDatasetSchemaAsync(ISource source, ConnectionDef sourceDef, DatasetDef dataset,
        List<PzError> errors, Dictionary<string, string> fetchedSchemas,
        Dictionary<string, IReadOnlyList<string>> extraColumns, CancellationToken ct)
    {
        // Same options+columns merge SpecBuilder.ForSourceLoad performs for a real read -- connectors
        // that require the declared contract to be present in DatasetSpec.Options (e.g. CsvSource) must
        // see the exact same shape here that a real run would give them.
        var options = new Dictionary<string, object?>(dataset.Options);
        if (dataset.Columns is not null)
        {
            options["columns"] = dataset.Columns;
        }

        DatasetSchema schema;
        try
        {
            schema = await WithTimeoutAsync(
                t => source.GetSchemaAsync(new DatasetSpec(sourceDef.Name, dataset.Name, options), t), ct)
                .ConfigureAwait(false);
        }
        catch (ProbeTimedOutException)
        {
            errors.Add(TimeoutError($"source '{sourceDef.Name}' dataset '{dataset.Name}' schema fetch", sourceDef.FilePath));
            return;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            errors.Add(new PzError(PzErrorCode.ConnectionCheckFailed,
                $"source '{sourceDef.Name}' dataset '{dataset.Name}' schema fetch failed: {ex.Message}",
                sourceDef.FilePath, null, ConnectHint));
            return;
        }

        if (dataset.Columns is not { Count: > 0 } contract)
        {
            var key = $"{sourceDef.Name}.{dataset.Name}";
            var rendered = string.Join(", ", schema.Schema.FieldsList
                .OrderBy(f => f.Name, StringComparer.Ordinal)
                .Select(f => $"{f.Name}: {ContractTypes.Describe(f.DataType)}"));
            fetchedSchemas[key] = rendered;
            return;
        }

        var fetchedFields = schema.Schema.FieldsList.ToDictionary(f => f.Name, f => f.DataType, StringComparer.Ordinal);
        foreach (var columnName in contract.Keys.OrderBy(k => k, StringComparer.Ordinal))
        {
            var contractType = contract[columnName];
            if (!fetchedFields.TryGetValue(columnName, out var actualType))
            {
                errors.Add(new PzError(PzErrorCode.SchemaDrift,
                    $"source '{sourceDef.Name}' dataset '{dataset.Name}': declared column '{columnName}' " +
                    $"({contractType}) is missing from the fetched schema",
                    sourceDef.FilePath, null, DriftHint));
                continue;
            }

            var expected = ContractTypes.ToArrowExpectation(contractType);
            if (!ContractTypes.ArrowTypesEqual(expected, actualType))
            {
                errors.Add(new PzError(PzErrorCode.SchemaDrift,
                    $"source '{sourceDef.Name}' dataset '{dataset.Name}': declared column '{columnName}' is " +
                    $"'{contractType}' but the fetched schema reports {ContractTypes.Describe(actualType)}",
                    sourceDef.FilePath, null, DriftHint));
            }

            // Extra fetched columns (present in fetchedFields but not in contract) are tolerated by
            // design: contracts prune on read, so this loop never iterates them -- recorded separately
            // below instead, for a caller that wants to know the live shape grew.
        }

        var extra = fetchedFields.Keys.Except(contract.Keys, StringComparer.Ordinal)
            .OrderBy(n => n, StringComparer.Ordinal).ToList();
        if (extra.Count > 0)
        {
            extraColumns[$"{sourceDef.Name}.{dataset.Name}"] = extra;
        }
    }

    /// <summary>Runs <paramref name="operation"/> under a <see cref="ProbeTimeout"/> window linked to
    /// (but independent of) <paramref name="ct"/>: if the timeout elapses first, <paramref
    /// name="operation"/>'s own cancellation observes it exactly like any other <see
    /// cref="OperationCanceledException"/> source (a well-behaved connector unwinds promptly), and this
    /// method translates that specific case into <see cref="ProbeTimedOutException"/> so callers can
    /// distinguish "this probe took too long" from "the caller's own ct was cancelled" (the latter must
    /// keep propagating as a plain <see cref="OperationCanceledException"/>, unwrapped).</summary>
    private static async Task<T> WithTimeoutAsync<T>(Func<CancellationToken, ValueTask<T>> operation, CancellationToken ct)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(ProbeTimeout);
        try
        {
            return await operation(timeoutCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new ProbeTimedOutException();
        }
    }

    private static PzError TimeoutError(string label, string filePath) =>
        new(PzErrorCode.ConnectionCheckFailed,
            $"{label} timed out after {ProbeTimeout.TotalSeconds.ToString("0.##", System.Globalization.CultureInfo.InvariantCulture)}s",
            filePath, null, ConnectHint);

    /// <summary>Internal signal distinguishing a <see cref="ProbeTimeout"/> elapsing from any other
    /// <see cref="OperationCanceledException"/> source (never surfaced past this file).</summary>
    private sealed class ProbeTimedOutException : Exception;
}
