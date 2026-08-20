using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit.Reference;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.Validation;

namespace Pz.Engine.Tests.Validation;

/// <summary>Tier 5 (`ConnectivityValidator`): concurrent `CheckConnectionAsync` probes (PZ0330) plus
/// schema drift detection against declared `columns:` contracts (PZ0331). Drift-focused tests reuse the
/// real <see cref="InMemoryConnector"/>/<see cref="InMemorySource"/> (fixed schema
/// id:int64,name:utf8,amount:double,flag:bool,ts:timestamp) since its `GetSchemaAsync` is independent of
/// any declared contract -- exactly what a genuine drift signal needs. Connection-check-focused tests use
/// <see cref="FaultyConnector"/>, which lets each test control <c>CheckConnectionAsync</c> directly.</summary>
public sealed class ConnectivityValidatorTests
{
    private static PzProject Project(IReadOnlyList<ConnectionDef>? sources = null, IReadOnlyList<ConnectionDef>? sinks = null) =>
        new("proj", "0.1.0", new EngineConfig(), new Dictionary<string, object?>(), [],
            [.. sources ?? [], .. sinks ?? []], []);

    [Fact]
    public async Task Failed_connection_check_is_PZ0330_naming_the_source()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("flaky", new FaultyConnector
        {
            CheckConnectionFunc = _ => new ValueTask<ConnectionCheck>(new ConnectionCheck(false, "no route to host")),
        });

        var source = new ConnectionDef("db", "flaky", new Dictionary<string, object?>(), [], "sources/db.yml");

        var result = await ConnectivityValidator.RunAsync(Project([source]), registry, default);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.ConnectionCheckFailed, error.Code);
        Assert.Contains("db", error.Message, StringComparison.Ordinal);
        Assert.Contains("no route to host", error.Message, StringComparison.Ordinal);
        Assert.Equal("sources/db.yml", error.File);
    }

    [Fact]
    public async Task Throwing_connection_check_is_PZ0330_not_a_crash()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("flaky", new FaultyConnector
        {
            CheckConnectionFunc = _ => throw new InvalidOperationException("dns lookup failed"),
        });

        var source = new ConnectionDef("db", "flaky", new Dictionary<string, object?>(), [], "sources/db.yml");

        var result = await ConnectivityValidator.RunAsync(Project([source]), registry, default);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.ConnectionCheckFailed, error.Code);
        Assert.Contains("db", error.Message, StringComparison.Ordinal);
        Assert.Contains("dns lookup failed", error.Message, StringComparison.Ordinal);
    }

    /// <summary>ProbeConnectionAsync's own thrown-exception path must
    /// route ex.Message through MessageRedaction before embedding it -- a connector could throw an
    /// exception wrapping a raw engine error (a DuckDB-shaped "LINE n: ..." statement echo).</summary>
    [Fact]
    public async Task Connectivity_probe_message_is_redacted()
    {
        const string engineEcho =
            "Binder Error: syntax error at or near \"CREATE\"\n" +
            "LINE 1: CREATE SECRET s (TYPE s3, KEY_ID 'AKID', SECRET 'SECRET_VALUE')\n" +
            "                                                        ^";
        var registry = new ConnectorRegistry();
        registry.AddSource("flaky", new FaultyConnector
        {
            CheckConnectionFunc = _ => throw new InvalidOperationException(engineEcho),
        });

        var source = new ConnectionDef("db", "flaky", new Dictionary<string, object?>(), [], "sources/db.yml");

        var result = await ConnectivityValidator.RunAsync(Project([source]), registry, default);

        var error = Assert.Single(result.Errors);
        Assert.DoesNotContain("SECRET_VALUE", error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("LINE 1", error.Message, StringComparison.Ordinal);
        Assert.Contains("Binder Error", error.Message, StringComparison.Ordinal);
    }

    /// <summary>The established gate pattern: each stub's `CheckConnectionAsync` signals it has entered,
    /// then blocks on its own gate until released. Both `entered` signals must complete BEFORE either
    /// gate is released -- if probes ran sequentially, the second stub's `CheckConnectionAsync` would
    /// never even be invoked until the first one's gate had already been released, so `entered2` would
    /// still be pending at that point.</summary>
    [Fact]
    public async Task All_connections_probed_concurrently()
    {
        var gate1 = new TaskCompletionSource();
        var gate2 = new TaskCompletionSource();
        var entered1 = new TaskCompletionSource();
        var entered2 = new TaskCompletionSource();

        var registry = new ConnectorRegistry();
        registry.AddSource("a", new FaultyConnector
        {
            CheckConnectionFunc = async _ =>
            {
                entered1.SetResult();
                await gate1.Task;
                return new ConnectionCheck(true);
            },
        });
        registry.AddSource("b", new FaultyConnector
        {
            CheckConnectionFunc = async _ =>
            {
                entered2.SetResult();
                await gate2.Task;
                return new ConnectionCheck(true);
            },
        });

        var sourceA = new ConnectionDef("a", "a", new Dictionary<string, object?>(), [], "sources/a.yml");
        var sourceB = new ConnectionDef("b", "b", new Dictionary<string, object?>(), [], "sources/b.yml");

        var runTask = ConnectivityValidator.RunAsync(Project([sourceA, sourceB]), registry, default);

        await Task.WhenAll(entered1.Task, entered2.Task).WaitAsync(TimeSpan.FromSeconds(5));

        gate1.SetResult();
        gate2.SetResult();

        var result = await runTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Empty(result.Errors);
    }

    [Fact]
    public async Task A_failing_probe_does_not_abort_other_probes()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("bad", new FaultyConnector
        {
            CheckConnectionFunc = _ => throw new InvalidOperationException("boom"),
        });
        registry.AddSource("good", new FaultyConnector());

        var badSource = new ConnectionDef("bad", "bad", new Dictionary<string, object?>(), [], "sources/bad.yml");
        var goodSource = new ConnectionDef("good", "good", new Dictionary<string, object?>(), [], "sources/good.yml");

        var result = await ConnectivityValidator.RunAsync(Project([badSource, goodSource]), registry, default);

        var error = Assert.Single(result.Errors);
        Assert.Contains("bad", error.Message, StringComparison.Ordinal);
    }

    // A probe that never completes (e.g. a firewalled host) must not hang
    // `pz validate --connect` forever -- ConnectivityValidator.ProbeTimeout is an internal, settable seam
    // (InternalsVisibleTo("Pz.Engine.Tests")) so this test injects a near-zero timeout instead of waiting
    // out the real 30s default; both tests below restore the original value in a `finally`, since the
    // field is static and xunit runs tests within one class sequentially by default.
    [Fact]
    public async Task Never_completing_connection_check_times_out_as_PZ0330_without_hanging()
    {
        var original = ConnectivityValidator.ProbeTimeout;
        ConnectivityValidator.ProbeTimeout = TimeSpan.FromMilliseconds(50);
        try
        {
            var registry = new ConnectorRegistry();
            registry.AddSource("hangs", new FaultyConnector
            {
                CheckConnectionFunc = async ct =>
                {
                    await Task.Delay(Timeout.Infinite, ct);
                    return new ConnectionCheck(true); // unreachable
                },
            });
            registry.AddSource("good", new FaultyConnector());

            var hangsSource = new ConnectionDef("hangs", "hangs", new Dictionary<string, object?>(), [], "sources/hangs.yml");
            var goodSource = new ConnectionDef("good", "good", new Dictionary<string, object?>(), [], "sources/good.yml");

            var result = await ConnectivityValidator.RunAsync(Project([hangsSource, goodSource]), registry, default)
                .WaitAsync(TimeSpan.FromSeconds(5));

            var error = Assert.Single(result.Errors);
            Assert.Equal(PzErrorCode.ConnectionCheckFailed, error.Code);
            Assert.Contains("hangs", error.Message, StringComparison.Ordinal);
            Assert.Contains("timed out after", error.Message, StringComparison.Ordinal);
        }
        finally
        {
            ConnectivityValidator.ProbeTimeout = original;
        }
    }

    /// <summary>Same protection, drift phase: a source that opens fine but whose `GetSchemaAsync` never
    /// completes must also time out rather than hang -- the timeout covers both probe call sites.</summary>
    [Fact]
    public async Task Never_completing_schema_fetch_times_out_as_PZ0330_without_hanging()
    {
        var original = ConnectivityValidator.ProbeTimeout;
        ConnectivityValidator.ProbeTimeout = TimeSpan.FromMilliseconds(50);
        try
        {
            var registry = new ConnectorRegistry();
            registry.AddSource("hangs", new HangingSchemaConnector());

            var source = new ConnectionDef("mem", "hangs", new Dictionary<string, object?>(),
                [new DatasetDef("numbers", new Dictionary<string, object?>(), null)], "sources/mem.yml");

            var result = await ConnectivityValidator.RunAsync(Project([source]), registry, default)
                .WaitAsync(TimeSpan.FromSeconds(5));

            var error = Assert.Single(result.Errors);
            Assert.Equal(PzErrorCode.ConnectionCheckFailed, error.Code);
            Assert.Contains("mem", error.Message, StringComparison.Ordinal);
            Assert.Contains("timed out after", error.Message, StringComparison.Ordinal);
        }
        finally
        {
            ConnectivityValidator.ProbeTimeout = original;
        }
    }

    [Fact]
    public async Task Declared_contract_drift_is_PZ0331_listing_columns()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("inmemory", new InMemoryConnector());

        // InMemorySource.FixedSchema declares "id" as Int64 -- "varchar" is a deliberate mismatch.
        var source = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", new Dictionary<string, object?> { ["rows"] = 10L },
                new Dictionary<string, string> { ["id"] = "varchar" })],
            "sources/mem.yml");

        var result = await ConnectivityValidator.RunAsync(Project([source]), registry, default);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.SchemaDrift, error.Code);
        Assert.Contains("id", error.Message, StringComparison.Ordinal);
        Assert.Contains("varchar", error.Message, StringComparison.Ordinal);
        Assert.Contains("Int64", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Missing_declared_column_is_PZ0331()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("inmemory", new InMemoryConnector());

        var source = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", new Dictionary<string, object?> { ["rows"] = 10L },
                new Dictionary<string, string> { ["id"] = "bigint", ["nonexistent"] = "varchar" })],
            "sources/mem.yml");

        var result = await ConnectivityValidator.RunAsync(Project([source]), registry, default);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.SchemaDrift, error.Code);
        Assert.Contains("nonexistent", error.Message, StringComparison.Ordinal);
        Assert.Contains("missing", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Extra_fetched_columns_are_tolerated()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("inmemory", new InMemoryConnector());

        // Only "id" is declared; InMemorySource.FixedSchema also carries name/amount/flag/ts, which
        // must be tolerated (contracts prune on read) rather than reported as drift.
        var source = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", new Dictionary<string, object?> { ["rows"] = 10L },
                new Dictionary<string, string> { ["id"] = "bigint" })],
            "sources/mem.yml");

        var result = await ConnectivityValidator.RunAsync(Project([source]), registry, default);

        Assert.Empty(result.Errors);
    }

    [Fact]
    public async Task Uncontracted_dataset_schema_is_cached()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("inmemory", new InMemoryConnector());

        var source = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", new Dictionary<string, object?> { ["rows"] = 10L }, null)],
            "sources/mem.yml");

        var result = await ConnectivityValidator.RunAsync(Project([source]), registry, default);

        Assert.Empty(result.Errors);
        var entry = Assert.Single(result.FetchedSchemas);
        Assert.Equal("mem.numbers", entry.Key);
        Assert.Contains("id: Int64", entry.Value, StringComparison.Ordinal);
        Assert.Contains("name: Utf8", entry.Value, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Contracted_dataset_schema_is_not_cached()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("inmemory", new InMemoryConnector());

        var source = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", new Dictionary<string, object?> { ["rows"] = 10L },
                new Dictionary<string, string> { ["id"] = "bigint" })],
            "sources/mem.yml");

        var result = await ConnectivityValidator.RunAsync(Project([source]), registry, default);

        Assert.Empty(result.FetchedSchemas);
    }

    [Fact]
    public async Task Unknown_connector_name_is_skipped_here()
    {
        var registry = new ConnectorRegistry(); // "doesnotexist" never registered

        var source = new ConnectionDef("weird", "doesnotexist", new Dictionary<string, object?>(), [], "sources/weird.yml");

        var result = await ConnectivityValidator.RunAsync(Project([source]), registry, default);

        Assert.Empty(result.Errors);
        Assert.Empty(result.FetchedSchemas);
    }

    /// <summary>Minimal connector double giving each test direct control over `CheckConnectionAsync`
    /// (<see cref="InMemoryConnector"/>'s always reports Ok, with no fault-injection hook for it). Opening
    /// a source is never exercised by the connection-check-focused tests that use this, so it throws.</summary>
    private sealed class FaultyConnector : ISourceConnector, ISinkConnector
    {
        public Func<CancellationToken, ValueTask<ConnectionCheck>>? CheckConnectionFunc { get; init; }

        public ConnectorInfo Info => new("faulty", "0.1.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            new(ValidationResult.Success);

        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            CheckConnectionFunc?.Invoke(ct) ?? new ValueTask<ConnectionCheck>(new ConnectionCheck(true));

        ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
            throw new NotSupportedException("FaultyConnector never opens a source in these connection-check tests");

        ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
            throw new NotSupportedException("FaultyConnector never opens a sink in these connection-check tests");
    }

    /// <summary>Opens instantly (so the drift phase reaches `GetSchemaAsync`) but that call never
    /// completes -- used to prove the drift-phase probe timeout covers `GetSchemaAsync`, not just
    /// `CheckConnectionAsync`.</summary>
    private sealed class HangingSchemaConnector : ISourceConnector, ISource
    {
        public ConnectorInfo Info => new("hangs", "0.1.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            new(ValidationResult.Success);

        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            new(new ConnectionCheck(true));

        public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

        public async ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
        {
            await Task.Delay(Timeout.Infinite, ct);
            throw new System.Diagnostics.UnreachableException(); // Task.Delay(Infinite, ct) never returns normally
        }

        public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
        {
            scan = null;
            return false;
        }

        public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
            throw new NotSupportedException("never reads in this schema-fetch-timeout test");

        public ValueTask DisposeAsync() => default;
    }
}
