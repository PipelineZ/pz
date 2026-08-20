using Npgsql;
using NpgsqlTypes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.TestKit;
using Pz.TestSupport;

namespace Pz.Connector.Postgres.Tests;

/// <summary>Runs the TestKit's source acceptance suite against the real <see cref="PostgresConnector"/>
/// against a Testcontainers postgres instance. <see cref="SmallDataset"/> declares a real
/// <c>partition_column</c>/<c>partitions</c> pair (2 partitions over "orders"' integer primary key), so
/// <c>Partitions_union_equals_single_partition_read</c> exercises its GROUND-TRUTH path (not the weak
/// re-plan-idempotency fallback) for postgres too, mirroring <c>InMemorySourceAcceptance</c>. This
/// class lives on <see cref="PostgresCdcContainerFixture"/>'s "postgres-cdc" collection
/// (wal_level=logical, "orders" seeded there too) so its change-capture facts have a
/// replication-capable server -- as <c>SqlServerSourceAcceptance</c> shares its one container with the
/// mssql cdc suite.</summary>
[Collection("postgres-cdc")]
public sealed class PostgresSourceAcceptance(PostgresCdcContainerFixture fixture) : SourceConnectorAcceptanceTests
{
    // The fixture's own constructor already calls DockerFacts.SkipUnlessDocker (see
    // PostgresContainerFixture's doc comment for the mechanism); overriding this GateFact() hook makes
    // every INHERITED fact call it before doing any work, so a docker-less run SKIPs cleanly. The extra
    // facts declared directly below (not inherited from the base) are [SkippableFact] for the same reason.
    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISourceConnector CreateSource() => new PostgresConnector();

    protected override ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["database"] = fixture.Database,
        ["user"] = fixture.User,
        ["password"] = fixture.Password,
    });

    protected override DatasetSpec SmallDataset => new("pg", "orders", new Dictionary<string, object?>
    {
        ["partition_column"] = "id",
        ["partitions"] = 2,
    });

    // 500k rows via generate_series -- large enough that Cancellation_honored_within_5s (which no-ops
    // when LargeDataset is null) genuinely exercises cancelling a real postgres read mid-stream.
    protected override DatasetSpec? LargeDataset => new("pg", "large", new Dictionary<string, object?>
    {
        ["query"] = "select i as n from generate_series(1, 500000) i",
    });

    // A read that fails MID-STREAM (after the reader opens successfully) with a genuinely transient
    // NpgsqlException: the query terminates its own backend partway through (self pg_terminate_backend),
    // which Npgsql classifies IsTransient=true (a broken connection is retriable, unlike a query
    // error). This exercises the manual-enumerator mid-stream classification path,
    // not the connection-open path -- see Bad_host_partition_read_reports_transient_at_open below for the
    // partition's own connect-time proof (Bad_host_read_reports_transient_at_open instead proves the
    // PROBE's connect-time classification, since SmallDataset carries a partition_column), and
    // Midstream_permanent_failure_is_classified_not_transient for a mid-stream failure that is NOT
    // transient (proving the classification carries the real SqlState-derived value, not a constant).
    protected override DatasetSpec? TransientFailureDataset => new("pg", "self-terminate", new Dictionary<string, object?>
    {
        ["query"] = "select case when x = 3 then pg_terminate_backend(pg_backend_pid()) else false end as v " +
            "from generate_series(1, 1000) x",
    });

    protected override DatasetSpec? GetSpecWithPartitionOverride(int partitions) => SmallDataset with
    {
        Options = new Dictionary<string, object?>(SmallDataset.Options) { ["partitions"] = partitions },
    };

    // A fresh fixture (own uniquely-named table + publication) per fact
    // access -- mirrors PostgresCdcSnapshotTests/PostgresCdcPollTests' per-test table convention so the
    // TestKit's ChangeCapture_* facts never collide with each other or with the CDC suites sharing this
    // container.
    protected override IChangeCaptureFixture? ChangeCaptureFixture => new PostgresChangeCaptureFixture(fixture);

    // Fact 5's default (mssql-shaped) implementation feeds a synthetically stale PriorSyncState and
    // expects the read to throw. That proof does not transfer to postgres: PostgresCdcPartition's poll
    // resumes from the replication SLOT's own server-side position (see PostgresCdcPartition.PollReadAsync
    // -- LastAppliedLsn/LastFlushedLsn only drive the CONFIRM, never the read start), so a stale-but
    // otherwise-valid-looking token wouldn't provably throw for the reason the fact wants to prove.
    // Postgres's honest analog is teardown: an operator/DBA (or `pz cdc drop`) removing the slot out from
    // under a scheduled poll must fail loudly, never silently -- proven here via
    // IChangeCaptureAdmin.DropChangeCaptureStateAsync (the same call `pz cdc drop` makes).
    [SkippableFact]
    public override async Task ChangeCapture_position_before_retained_minimum_throws()
    {
        GateFact();
        Skip.If(ChangeCaptureFixture is null, "no ChangeCaptureFixture provided");
        var cdcFixture = ChangeCaptureFixture!;
        var connector = CreateSource();
        Skip.If(!connector.Capabilities.HasFlag(ConnectorCapabilities.ChangeCapture),
            "connector does not declare ChangeCapture");

        var spec = await cdcFixture.CdcSpecAsync();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        // First-run snapshot: creates the pz-owned slot and yields a genuinely valid resume token.
        var snapshotPartition = Assert.Single(
            await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));
        await foreach (var batch in snapshotPartition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            batch.Dispose();
        }

        Assert.True(((ISyncStatePartition)snapshotPartition).TryGetSyncStateCandidate(out var token));
        Assert.NotNull(token);

        // Teardown: the slot is dropped server-side out from under the next poll -- exactly what `pz cdc
        // drop` (or a DBA) does.
        var admin = Assert.IsAssignableFrom<IChangeCaptureAdmin>(source);
        await admin.DropChangeCaptureStateAsync(spec, CancellationToken.None);

        var pollPartition = Assert.Single(await source.PlanReadAsync(
            spec with { PriorSyncState = token }, ReadHints.None, CancellationToken.None));
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in pollPartition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }
        });
        Assert.False(ex.IsTransient);
    }

    // The IChangeCaptureFixture implementation backing the TestKit's ChangeCapture_* facts.
    // Owns one throwaway table (own publication, dropped/recreated per instance) with 20 seed rows split
    // into disjoint update/delete pools so MutateAsync never re-touches a key a prior call on the SAME
    // instance already mutated -- every acceptance fact gets its own fresh instance via the
    // ChangeCaptureFixture property above, so no state leaks between facts.
    private sealed class PostgresChangeCaptureFixture(PostgresCdcContainerFixture fixture) : IChangeCaptureFixture
    {
        private const int SeedRows = 20;
        private const int UpdatePoolStart = 1;
        private const int UpdatePoolEnd = 10; // inclusive
        private const int DeletePoolStart = 11;
        private const int DeletePoolEnd = 20; // inclusive

        private readonly string _table = $"cdc_acceptance_{Guid.NewGuid():N}"[..24];
        private readonly string _dataset = $"cdc_acceptance_ds_{Guid.NewGuid():N}"[..28];
        private Task<DatasetSpec>? _setup;
        private int _nextInsertId = 10_000;
        private int _nextUpdateId = UpdatePoolStart;
        private int _nextDeleteId = DeletePoolStart;

        public Task<DatasetSpec> CdcSpecAsync() => _setup ??= SetupAsync();

        public async Task MutateAsync(int inserts, int updates, int deletes)
        {
            var spec = await CdcSpecAsync().ConfigureAwait(false);
            var table = spec.Dataset;

            for (var i = 0; i < inserts; i++)
            {
                var id = _nextInsertId++;
                await ExecuteAsync($"insert into public.{table} (id, name) values ({id}, 'new-{id}')")
                    .ConfigureAwait(false);
            }

            for (var i = 0; i < updates; i++)
            {
                if (_nextUpdateId > UpdatePoolEnd)
                {
                    throw new InvalidOperationException("PostgresChangeCaptureFixture: update pool exhausted");
                }

                var id = _nextUpdateId++;
                await ExecuteAsync($"update public.{table} set name = 'updated-{id}' where id = {id}")
                    .ConfigureAwait(false);
            }

            for (var i = 0; i < deletes; i++)
            {
                if (_nextDeleteId > DeletePoolEnd)
                {
                    throw new InvalidOperationException("PostgresChangeCaptureFixture: delete pool exhausted");
                }

                var id = _nextDeleteId++;
                await ExecuteAsync($"delete from public.{table} where id = {id}").ConfigureAwait(false);
            }
        }

        public async Task<string?> ServerPositionAsync()
        {
            await using var conn = await OpenAsync().ConfigureAwait(false);
            await using var cmd = new NpgsqlCommand("select pg_current_wal_lsn()", conn);
            var lsn = (NpgsqlLogSequenceNumber)(await cmd.ExecuteScalarAsync().ConfigureAwait(false))!;
            return PostgresCdc.FormatLsn(lsn);
        }

        private async Task<DatasetSpec> SetupAsync()
        {
            await ExecuteAsync($"drop table if exists public.{_table} cascade").ConfigureAwait(false);
            await ExecuteAsync($"create table public.{_table} (id integer primary key, name text not null)")
                .ConfigureAwait(false);
            await ExecuteAsync(
                $"insert into public.{_table} (id, name) select i, 'row-' || i " +
                $"from generate_series(1, {SeedRows}) i").ConfigureAwait(false);

            await ExecuteAsync("drop publication if exists pz_pg").ConfigureAwait(false);
            await ExecuteAsync($"create publication pz_pg for table public.{_table}").ConfigureAwait(false);

            var spec = new DatasetSpec("pg", _table, new Dictionary<string, object?>())
            {
                ChangeCapture = true,
                PriorSyncState = null,
            };
            await DropSlotIfExistsAsync(PostgresCdc.SlotName(spec)).ConfigureAwait(false);
            return spec;
        }

        private async Task<NpgsqlConnection> OpenAsync()
        {
            var conn = new NpgsqlConnection(fixture.ConnectionString);
            await conn.OpenAsync().ConfigureAwait(false);
            return conn;
        }

        private async Task ExecuteAsync(string sql)
        {
            await using var conn = await OpenAsync().ConfigureAwait(false);
            await using var cmd = new NpgsqlCommand(sql, conn);
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        private async Task DropSlotIfExistsAsync(string slot) =>
            await ExecuteAsync(
                $"select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = '{slot}'")
                .ConfigureAwait(false);
    }

    // Mid-stream classification-surface coverage: division by zero is a genuine mid-stream postgres error
    // (SQLSTATE 22012) that Npgsql classifies IsTransient=false. This cannot be wired through
    // TransientFailureDataset (whose contract requires IsTransient=true for
    // Transient_failures_carry_is_transient to pass), so it gets its own direct fact proving the
    // mid-stream manual-enumerator path surfaces a *correctly-classified permanent* failure as
    // PzConnectorException, not just a transient one and not a raw NpgsqlException.
    [SkippableFact]
    public async Task Midstream_permanent_failure_is_classified_not_transient()
    {
        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new DatasetSpec("pg", "div-by-zero", new Dictionary<string, object?>
        {
            ["query"] = "select 1 / (x - 50) as v from generate_series(1, 100) x",
        });
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }
        });

        Assert.False(ex.IsTransient);
    }

    // Connect-time transient classification of the min/max PROBE connection: SmallDataset declares
    // partition_column, so PlanReadAsync itself opens a
    // probe connection (min/max round trip, see ProbeRangeAsync) before ever returning a partition list --
    // against an unreachable host, THAT connection attempt fails first, proving the probe connection's own
    // try/catch carries the same IsTransient rule (ex.IsTransient off the NpgsqlException) as every other
    // connect-time catch in this file. This does NOT reach PostgresPartition.ReadAsync's own connect-time
    // catch (PlanReadAsync throws before a partition is ever constructed) -- see
    // Bad_host_partition_read_reports_transient_at_open immediately below for that proof.
    [SkippableFact]
    public async Task Bad_host_read_reports_transient_at_open()
    {
        ISourceConnector connector = new PostgresConnector();
        var unreachable = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["host"] = "127.0.0.1",
            ["port"] = 1, // nothing listens on port 1
            ["database"] = fixture.Database,
            ["user"] = fixture.User,
            ["password"] = fixture.Password,
        });
        await using var source = await connector.OpenAsync(unreachable, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            var partitions = await source.PlanReadAsync(SmallDataset, ReadHints.None, CancellationToken.None);
            await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }
        });

        Assert.True(ex.IsTransient);
    }

    // The proof above only exercises the PROBE connection's connect-time catch -- PlanReadAsync throws
    // before PostgresPartition.ReadAsync is ever reached, leaving that type's own connect-time
    // try/catch (around OpenAsync/ExecuteReaderAsync, just above the mid-stream manual-enumerator
    // loop) unproven. A partition_column-FREE spec (query:-based, like
    // TransientFailureDataset above) makes PlanReadAsync return a single partition synchronously with NO
    // network access at all (see PostgresSource.PlanReadAsync's early return when partition_column is
    // absent) -- so against the same unreachable host, the only possible failure is
    // PostgresPartition.ReadAsync's own OpenAsync, proving it applies the identical IsTransient rule.
    [SkippableFact]
    public async Task Bad_host_partition_read_reports_transient_at_open()
    {
        ISourceConnector connector = new PostgresConnector();
        var unreachable = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["host"] = "127.0.0.1",
            ["port"] = 1, // nothing listens on port 1
            ["database"] = fixture.Database,
            ["user"] = fixture.User,
            ["password"] = fixture.Password,
        });
        await using var source = await connector.OpenAsync(unreachable, CancellationToken.None);

        var noPartitionColumnSpec = new DatasetSpec("pg", "no-partition-column", new Dictionary<string, object?>
        {
            ["query"] = "select 1 as n",
        });

        // Static: no partition_column means PlanReadAsync never touches the network (no probe connection).
        var partitions = await source.PlanReadAsync(noPartitionColumnSpec, ReadHints.None, CancellationToken.None);
        Assert.Single(partitions);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }
        });

        Assert.True(ex.IsTransient);
    }

    // A pre-cancelled token fires OperationCanceledException from inside NpgsqlConnection.OpenAsync --
    // NOT an NpgsqlException, so disposal tied to a `catch (NpgsqlException)` would leak the
    // connection. `await using var connection` (see PostgresPartition.ReadAsync) disposes on every exit
    // path regardless of exception type; this test
    // asserts the cancellation still surfaces cleanly as OperationCanceledException (not, say, an
    // ObjectDisposedException or a hang) -- the strongest black-box signal available that disposal didn't
    // go sideways, since the connection object itself isn't reachable from the test.
    [SkippableFact]
    public async Task ReadAsync_surfaces_clean_cancellation_when_cancelled_before_connect()
    {
        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var partitions = await source.PlanReadAsync(SmallDataset, ReadHints.None, CancellationToken.None);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, cts.Token))
            {
                batch.Dispose();
            }
        });
    }

    // A numeric value with 13 fractional digits (> the 9 decimal128(38,9) supports) must
    // surface as a PzConnectorException naming the column -- not a raw OverflowException.
    [SkippableFact]
    public async Task Numeric_overflow_is_reported_as_named_column_error()
    {
        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new DatasetSpec("pg", "overflow", new Dictionary<string, object?>
        {
            ["query"] = "select 12345.1234567890123::numeric as bignum",
        });
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }
        });

        Assert.False(ex.IsTransient);
        Assert.Contains("'bignum'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("decimal128(38,9)", ex.Message, StringComparison.Ordinal);
        Assert.IsType<OverflowException>(ex.InnerException);
    }

    // Beyond the inherited acceptance suite: CheckConnectionAsync's own contract isn't covered by any
    // SourceConnectorAcceptanceTests fact, so it gets two direct checks here -- the happy path against
    // the real running container, and the failure path against a definitely-unreachable port, which
    // proves the IsTransient tag actually reaches the ConnectionCheck message (the ConnectionCheck
    // shape carries no separate transience field).
    [SkippableFact]
    public async Task CheckConnectionAsync_reports_ok_against_running_container()
    {
        ISourceConnector connector = new PostgresConnector();

        var check = await connector.CheckConnectionAsync(ValidConfig, CancellationToken.None);

        Assert.True(check.Ok);
    }

    [SkippableFact]
    public async Task CheckConnectionAsync_reports_transient_or_permanent_tag_on_failure()
    {
        ISourceConnector connector = new PostgresConnector();
        var unreachable = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["host"] = "127.0.0.1",
            ["port"] = 1, // nothing listens on port 1
            ["database"] = fixture.Database,
            ["user"] = fixture.User,
            ["password"] = fixture.Password,
        });

        var check = await connector.CheckConnectionAsync(unreachable, CancellationToken.None);

        Assert.False(check.Ok);
        Assert.True(
            check.Message!.StartsWith("transient: ", StringComparison.Ordinal) ||
            check.Message!.StartsWith("permanent: ", StringComparison.Ordinal),
            $"expected a transient/permanent tagged message, got: {check.Message}");
    }
}
