using Microsoft.Data.SqlClient;
using Pz.TestSupport;
using Testcontainers.MsSql;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>Shared SQL Server container + seed data (one collection, one container -- startup is the
/// expensive part). Constructor calls DockerFacts.SkipUnlessDocker before any Testcontainers call, so
/// docker-less machines SKIP cleanly (same mechanism as the other database connector fixtures:
/// [SkippableFact] everywhere + GateFact overrides in acceptance subclasses).</summary>
public sealed class MsSqlContainerFixture : IAsyncLifetime
{
    private MsSqlContainer? _container;

    public MsSqlContainerFixture()
    {
        DockerFacts.SkipUnlessDocker();
    }

    public string Host { get; private set; } = "";
    public int Port { get; private set; }
    public string Database => "pz";
    public string User => "sa";
    public string Password { get; private set; } = "";
    public string ConnectionString { get; private set; } = "";

    public async Task InitializeAsync()
    {
        _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            // SQL Server Agent is off by default; cdc's capture/cleanup jobs need it running. Turning
            // it on is harmless to every other suite sharing this container -- it just runs in the
            // background -- so this reuses the one shared fixture instead of paying for a second heavy
            // mssql container.
            .WithEnvironment("MSSQL_AGENT_ENABLED", "true")
            .Build();
        await _container.StartAsync().ConfigureAwait(false);

        var master = new SqlConnectionStringBuilder(_container.GetConnectionString())
        {
            TrustServerCertificate = true,
        };
        Password = master.Password;
        var parts = master.DataSource.Split(',');
        Host = parts[0];
        Port = int.Parse(parts[1]);

        await using (var connection = new SqlConnection(master.ConnectionString))
        {
            await connection.OpenAsync().ConfigureAwait(false);
            await ExecuteAsync(connection, "create database pz").ConfigureAwait(false);
        }

        master.InitialCatalog = Database;
        ConnectionString = master.ConnectionString;
        await SeedAsync().ConfigureAwait(false);
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task SeedAsync()
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        // "orders": the acceptance suite's SmallDataset -- >= 100 rows (TestKit requires >= 100 and
        // >= 2 batches under a 4KB batch target).
        await ExecuteAsync(connection, """
            create table dbo.orders (
                id int primary key,
                name nvarchar(50) not null,
                amount float not null,
                flag bit not null,
                created datetimeoffset(6) not null
            )
            """).ConfigureAwait(false);
        await ExecuteAsync(connection, """
            insert into dbo.orders (id, name, amount, flag, created)
            select value, concat('row-', value), value * 1.5, value % 2,
                   dateadd(minute, value, cast('2026-01-01T00:00:00+00:00' as datetimeoffset(6)))
            from generate_series(0, 149)
            """).ConfigureAwait(false);

        // "matrix": canonical + widened types, one value row + one all-NULL row.
        await ExecuteAsync(connection, """
            create table dbo.matrix (
                id int primary key,
                c_int int, c_tinyint tinyint, c_smallint smallint, c_bigint bigint,
                c_float float, c_real real,
                c_decimal decimal(38,9), c_money money,
                c_nvarchar nvarchar(100), c_varchar varchar(100), c_char char(5),
                c_guid uniqueidentifier, c_bit bit,
                c_date date, c_datetime2 datetime2(6), c_datetime datetime,
                c_dto datetimeoffset(6)
            )
            """).ConfigureAwait(false);
        await ExecuteAsync(connection, """
            insert into dbo.matrix values (
                1, 42, 7, -3, 9000000000,
                1.5, 2.5,
                12345.123456789, 99.9900,
                N'hello', 'world', 'abc',
                '11111111-2222-3333-4444-555555555555', 1,
                '2026-03-27', '2026-03-27T10:30:00', '2026-03-27T10:30:00',
                '2026-03-27T12:30:00+02:00')
            """).ConfigureAwait(false);
        await ExecuteAsync(connection,
            "insert into dbo.matrix (id) values (2)").ConfigureAwait(false);

        // Stored-proc extraction proofs. GetSchemaAsync probes via CommandBehavior.SchemaOnly, which
        // SqlClient implements as SQL Server's legacy `SET FMTONLY ON` -- not
        // sp_describe_first_result_set. Verified against a live container:
        //  - a plain proc is statically describable (obviously).
        //  - a proc using dynamic SQL (sp_executesql or EXEC(string)) against a real table is ALSO
        //    statically describable under FMTONLY -- FMTONLY runs the dynamic batch too, it just
        //    suppresses row output. No WITH RESULT SETS hint is needed.
        //  - a proc that stages its result in a #temp table is NOT describable: FMTONLY skips DDL
        //    (the CREATE TABLE never runs), so the later reference to the temp object fails with
        //    "Invalid object name". WITH RESULT SETS on the EXEC does NOT rescue this -- that clause
        //    is documented for the sp_describe_first_result_set API, which this probe doesn't use --
        //    confirmed identical failure with and without the clause, both directly against the
        //    server (raw `SET FMTONLY ON`) and through the connector.
        await ExecuteAsync(connection, """
            create procedure dbo.orders_since @min_id int as
            begin
                set nocount on;
                select id, name, amount, flag, created from dbo.orders where id > @min_id;
            end
            """).ConfigureAwait(false);
        await ExecuteAsync(connection, """
            create procedure dbo.orders_dynamic as
            begin
                set nocount on;
                declare @sql nvarchar(max) = N'select id, name from dbo.orders';
                exec sp_executesql @sql;
            end
            """).ConfigureAwait(false);
        await ExecuteAsync(connection, """
            create procedure dbo.orders_temp as
            begin
                set nocount on;
                create table #tmp (id int, name nvarchar(50));
                insert into #tmp select id, name from dbo.orders;
                select id, name from #tmp;
            end
            """).ConfigureAwait(false);

        // `procedure:` dataset mode: a plain, FMTONLY-describable proc with two optional bound
        // parameters -- exercises both the bare full-read case and the $watermark/$watermark_upper
        // sentinel bindings against real ids 0..149.
        await ExecuteAsync(connection, """
            create procedure dbo.orders_page @min_id int = null, @max_id int = null as
            begin
                set nocount on;
                select id, name, amount, flag, created from dbo.orders
                where (@min_id is null or id > @min_id) and (@max_id is null or id <= @max_id);
            end
            """).ConfigureAwait(false);

        // "type_audit": mapped types + value shapes a real mart hits. One value row + one all-NULL
        // row, mirroring dbo.matrix's convention.
        await ExecuteAsync(connection, """
            create table dbo.type_audit (
                id int primary key,
                c_datetime datetime, c_smalldatetime smalldatetime,
                c_numeric numeric(18,2), c_smallmoney smallmoney,
                c_nchar nchar(5), c_text text, c_ntext ntext,
                c_nvarchar_max nvarchar(max),
                c_unicode nvarchar(50),
                c_cs nvarchar(20) collate Latin1_General_CS_AS,
                c_big_decimal decimal(29,9),
                c_date_min date, c_date_max date,
                c_dt2_max datetime2(6)
            )
            """).ConfigureAwait(false);
        await ExecuteAsync(connection, """
            insert into dbo.type_audit values (
                1,
                '2026-03-27T10:30:00', '2026-03-27T10:30:00',
                12345.67, 99.99,
                N'ab', 'legacy-text', N'legacy-ntext',
                replicate(cast('x' as nvarchar(max)), 10000),
                N'café 漢字 🚀',
                N'CaseSensitive',
                12345678901234567890.123456789,
                '0001-01-01', '9999-12-31',
                '9999-12-31T23:59:59.999999'
            )
            """).ConfigureAwait(false);
        await ExecuteAsync(connection,
            "insert into dbo.type_audit (id) values (2)").ConfigureAwait(false);

        // "unmapped": types outside the matrix -- the error contract + cast workaround.
        await ExecuteAsync(connection, """
            create table dbo.unmapped (
                id int primary key,
                c_bin varbinary(8), c_time time(0), c_xml xml
            )
            """).ConfigureAwait(false);
        await ExecuteAsync(connection,
            "insert into dbo.unmapped values (1, 0x0102, '10:30:00', '<a/>')").ConfigureAwait(false);
    }

    internal static async Task ExecuteAsync(SqlConnection connection, string sql)
    {
        await using var command = new SqlCommand(sql, connection);
        command.CommandTimeout = 120;
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }
}

[CollectionDefinition("sqlserver")]
public sealed class SqlServerCollection : ICollectionFixture<MsSqlContainerFixture>;
