using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Toolkit.Formats;
using Pz.DuckDb;

namespace Pz.Connector.LocalFiles.Tests;

public sealed class LocalFilesConnectorTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-localfiles-tests", Guid.NewGuid().ToString("N"));

    public LocalFilesConnectorTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private ConnectorConfig Config => new(new Dictionary<string, object?> { ["base_dir"] = _dir });

    [Fact]
    public async Task Csv_parse_error_names_file_line_column()
    {
        var path = Path.Combine(_dir, "bad.csv");
        await File.WriteAllTextAsync(path, "id,amount\n1,10.5\n2,not-a-number\n");

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "bad", new Dictionary<string, object?>
        {
            ["path"] = "bad.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["amount"] = "double" },
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
        Assert.Contains("bad.csv", ex.Message);
        Assert.Contains("line", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("amount", ex.Message);
    }

    [Fact]
    public async Task Empty_cells_and_quoted_empty_varchar_become_null()
    {
        // Row 1 is fully populated. Row 2 has empty (unquoted) cells for both the nullable int
        // column and the varchar column. Row 3 has an empty *quoted* string ("") for the varchar
        // column — per the implemented policy (CsvSource.ParseValue's IsNullOrEmpty check) this is
        // indistinguishable from an empty cell and also becomes NULL, so a varchar column can never
        // round-trip an actual empty string, only NULL.
        var path = Path.Combine(_dir, "nulls.csv");
        await File.WriteAllTextAsync(path, "id,qty,name\n1,5,Alice\n2,,\n3,7,\"\"\n");

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "nulls", new Dictionary<string, object?>
        {
            ["path"] = "nulls.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["qty"] = "int", ["name"] = "varchar" },
        });

        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);

        var ids = new List<long>();
        var qtys = new List<int?>();
        var names = new List<string?>();
        await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            var idCol = (Int64Array)batch.Column(0);
            var qtyCol = (Int32Array)batch.Column(1);
            var nameCol = (StringArray)batch.Column(2);
            for (var i = 0; i < batch.Length; i++)
            {
                ids.Add(idCol.GetValue(i)!.Value);
                qtys.Add(qtyCol.IsNull(i) ? null : qtyCol.GetValue(i));
                names.Add(nameCol.IsNull(i) ? null : nameCol.GetString(i));
            }

            batch.Dispose();
        }

        Assert.Equal(new long[] { 1, 2, 3 }, ids);

        Assert.Equal(5, qtys[0]);
        Assert.Null(qtys[1]);
        Assert.Equal(7, qtys[2]);

        Assert.Equal("Alice", names[0]);
        Assert.Null(names[1]); // empty unquoted cell -> null
        Assert.Null(names[2]); // quoted empty string "" -> also null, per the implemented policy
    }

    /// <summary>A row wider than Sylvan's default 16KiB read buffer must still read: the library would
    /// otherwise fail the whole node with its own "Row 1 was too large. Try increasing the MaxBufferSize
    /// setting." — advice naming a knob pz does not expose. The native tier (DuckDB read_csv) reads rows
    /// this size without complaint, so a universal-tier failure here would break the two tiers'
    /// behavioural-interchangeability contract. CsvSource sets an explicit MaxBufferSize.</summary>
    [Fact]
    public async Task Csv_row_wider_than_the_readers_default_buffer_round_trips()
    {
        var payload = new string('x', 64 * 1024);
        var path = Path.Combine(_dir, "wide-row.csv");
        await File.WriteAllTextAsync(path, $"id,payload\n1,{payload}\n");

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "wide_row", new Dictionary<string, object?>
        {
            ["path"] = "wide-row.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["payload"] = "varchar" },
        });

        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);

        var payloads = new List<string?>();
        await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            var payloadCol = (StringArray)batch.Column(1);
            for (var i = 0; i < batch.Length; i++)
            {
                payloads.Add(payloadCol.IsNull(i) ? null : payloadCol.GetString(i));
            }

            batch.Dispose();
        }

        Assert.Equal(payload, Assert.Single(payloads));
    }

    [Fact]
    public async Task Missing_header_column_is_permanent_error()
    {
        var path = Path.Combine(_dir, "missing_header.csv");
        await File.WriteAllTextAsync(path, "id\n1\n2\n");

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "t", new Dictionary<string, object?>
        {
            ["path"] = "missing_header.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["amount"] = "double" },
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
        Assert.Contains("amount", ex.Message);
        Assert.Contains("missing_header.csv", ex.Message);
    }

    [Fact]
    public async Task Missing_columns_contract_is_permanent_error()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "t", new Dictionary<string, object?>
        {
            ["path"] = "whatever.csv",
            ["format"] = "csv",
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.GetSchemaAsync(spec, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("columns:", ex.Message);
    }

    [Fact]
    public async Task Decimal_column_sink_is_permanent_error()
    {
        var connector = new LocalFilesConnector();
        await using var sink = await ((ISinkConnector)connector).OpenAsync(Config, CancellationToken.None);

        var schema = new Schema(
        [
            new Field("amount", new Decimal128Type(38, 9), nullable: true),
        ], null);

        var spec = new OutputSpec("lake", "out", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "out", ["format"] = "parquet" });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, schema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("amount", ex.Message);
    }

    [Fact]
    public async Task Replace_mode_commit_is_atomic_and_leaves_no_temp_dirs()
    {
        var connector = new LocalFilesConnector();
        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: false),
        ], null);

        var spec = new OutputSpec("lake", "out", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "out", ["format"] = "parquet" });

        await WriteOneAsync(connector, schema, spec, id: 1, name: "first");
        var outputDir = Path.Combine(_dir, "out");
        var finalPath = Path.Combine(outputDir, "out.parquet");

        Assert.True(File.Exists(finalPath));
        Assert.Empty(Directory.EnumerateDirectories(outputDir, ".pz-tmp-*"));

        await WriteOneAsync(connector, schema, spec, id: 2, name: "second");

        Assert.True(File.Exists(finalPath));
        Assert.Empty(Directory.EnumerateDirectories(outputDir, ".pz-tmp-*"));
    }

    [Fact]
    public async Task Native_scan_fragment_types_every_contract_column()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "matrix", new Dictionary<string, object?>
        {
            ["path"] = "matrix.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string>
            {
                ["c_int"] = "int",
                ["c_bigint"] = "bigint",
                ["c_double"] = "double",
                ["c_dec"] = "decimal",
                ["c_varchar"] = "varchar",
                ["c_bool"] = "boolean",
                ["c_date"] = "date",
                ["c_ts"] = "timestamp",
            },
        });

        var ok = source.TryGetNativeScan(spec, out var scan);

        Assert.True(ok);
        Assert.NotNull(scan);
        Assert.Contains("auto_detect = false", scan!.SqlFragment);
        Assert.Contains("columns = {", scan.SqlFragment);
        Assert.Contains("'c_int': 'INTEGER'", scan.SqlFragment);
        Assert.Contains("'c_bigint': 'BIGINT'", scan.SqlFragment);
        Assert.Contains("'c_double': 'DOUBLE'", scan.SqlFragment);
        Assert.Contains("'c_dec': 'DECIMAL(38,9)'", scan.SqlFragment);
        Assert.Contains("'c_varchar': 'VARCHAR'", scan.SqlFragment);
        Assert.Contains("'c_bool': 'BOOLEAN'", scan.SqlFragment);
        Assert.Contains("'c_date': 'DATE'", scan.SqlFragment);
        Assert.Contains("'c_ts': 'TIMESTAMP'", scan.SqlFragment);
    }

    // read_csv binds `columns=` to the file BY POSITION, ignoring the header names. If the contract's
    // declared order disagrees with the file header, the read silently loads each file column's values
    // under a DIFFERENT declared column's name -- silent cross-column corruption. The connector must
    // refuse when the file exists and its header contradicts the contract's positional binding.
    [Fact]
    public async Task Native_scan_rejects_a_contract_whose_order_contradicts_the_file_header()
    {
        var path = Path.Combine(_dir, "swapped.csv");
        await File.WriteAllTextAsync(path, "price,qty\n9.99,3\n");

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "swapped", new Dictionary<string, object?>
        {
            ["path"] = "swapped.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["qty"] = "double", ["price"] = "double" },
        });

        var ex = Assert.Throws<PzConnectorException>(() => source.TryGetNativeScan(spec, out _));

        Assert.False(ex.IsTransient);
        Assert.Contains("qty", ex.Message);
        Assert.Contains("price", ex.Message);
    }

    [Fact]
    public async Task Native_scan_accepts_a_contract_matching_the_file_header_order()
    {
        var path = Path.Combine(_dir, "aligned.csv");
        await File.WriteAllTextAsync(path, "qty,price\n3,9.99\n");

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "aligned", new Dictionary<string, object?>
        {
            ["path"] = "aligned.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["qty"] = "double", ["price"] = "double" },
        });

        Assert.True(source.TryGetNativeScan(spec, out var scan));
        Assert.NotNull(scan);
    }

    /// <summary>When a windowed dataset stamps BOTH
    /// <see cref="DatasetSpec.WatermarkCursor"/>+<see cref="DatasetSpec.WatermarkValue"/> (lower bound)
    /// AND <see cref="DatasetSpec.WatermarkUpperBound"/>, <see cref="CsvSource.TryGetNativeScan"/> wraps
    /// the read_csv(...) fragment in an outer SELECT with both bounds AND-chained, so DuckDB applies the
    /// filter as part of the native CTAS -- exactly the contract <see cref="LocalFilesConnector"/>'s
    /// <see cref="ConnectorCapabilities.BoundedWindow"/> declaration promises.</summary>
    [Fact]
    public async Task Native_scan_wraps_fragment_with_window_bounds()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
        {
            ["path"] = "orders.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        })
        {
            WatermarkCursor = "id",
            WatermarkValue = "10",
            WatermarkUpperBound = "20",
        };

        var ok = source.TryGetNativeScan(spec, out var scan);

        Assert.True(ok);
        Assert.NotNull(scan);
        Assert.StartsWith("(select * from read_csv(", scan!.SqlFragment);
        Assert.Contains("\"id\" > '10' and \"id\" <= '20'", scan.SqlFragment);
        Assert.EndsWith(")", scan.SqlFragment);
    }

    /// <summary>Quote-doubling defense-in-depth (mirrors Postgres's
    /// <c>Upper_bound_literal_is_quote_doubled</c>): the engine never actually produces a watermark value
    /// containing a quote (canonical forms are digits/ISO-8601 only), but the literal escaping must still
    /// be correct if it ever did.</summary>
    [Fact]
    public async Task Native_scan_window_bound_literal_is_quote_doubled()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
        {
            ["path"] = "orders.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "varchar" },
        })
        {
            WatermarkCursor = "id",
            WatermarkValue = "1",
            WatermarkUpperBound = "o'brien",
        };

        var ok = source.TryGetNativeScan(spec, out var scan);

        Assert.True(ok);
        Assert.Contains("'o''brien'", scan!.SqlFragment);
    }

    /// <summary>A spec with no watermark fields at all produces the bare fragment -- no wrapping SELECT,
    /// no WHERE. Plain (unwindowed) incremental -- <see cref="DatasetSpec.WatermarkCursor"/> set alone,
    /// no <see cref="DatasetSpec.WatermarkUpperBound"/> -- takes the same unwrapped path (ignoring the
    /// lower-bound watermark on the native tier is always correct; merge dedups).</summary>
    [Fact]
    public async Task Native_scan_without_upper_bound_is_byte_identical_to_unwindowed()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var baseSpec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
        {
            ["path"] = "orders.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        });

        Assert.True(source.TryGetNativeScan(baseSpec, out var unwindowedScan));

        var lowerBoundOnlySpec = baseSpec with { WatermarkCursor = "id", WatermarkValue = "10" };
        Assert.True(source.TryGetNativeScan(lowerBoundOnlySpec, out var lowerBoundOnlyScan));

        Assert.Equal(unwindowedScan!.SqlFragment, lowerBoundOnlyScan!.SqlFragment);
        Assert.DoesNotContain("where", unwindowedScan.SqlFragment, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Functional proof (not just string assertions): the wrapped fragment actually FILTERS when
    /// executed by DuckDB, mirroring <see cref="Native_scan_roundtrips_via_duckdb"/>'s pattern.</summary>
    [Fact]
    public async Task Native_scan_window_bounds_filter_rows_in_duckdb()
    {
        var path = Path.Combine(_dir, "orders.csv");
        await File.WriteAllTextAsync(path, "id\n" + string.Join('\n', Enumerable.Range(1, 30)));

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
        {
            ["path"] = "orders.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        })
        {
            WatermarkCursor = "id",
            WatermarkValue = "10",
            WatermarkUpperBound = "20",
        };

        var ok = source.TryGetNativeScan(spec, out var scan);
        Assert.True(ok);

        await using var duck = DuckSession.Open(Path.Combine(_dir, "window.duckdb"));
        await duck.ExecuteAsync("create schema if not exists staging");
        await duck.ExecuteAsync($"create table staging.t as select * from {scan!.SqlFragment}");

        Assert.Equal(10, await duck.ScalarAsync<long>("select count(*) from staging.t"));
        Assert.Equal(11L, await duck.ScalarAsync<long>("select min(id) from staging.t"));
        Assert.Equal(20L, await duck.ScalarAsync<long>("select max(id) from staging.t"));
    }

    [Fact]
    public async Task Connector_declares_bounded_window()
    {
        var connector = new LocalFilesConnector();
        Assert.True(connector.Capabilities.HasFlag(ConnectorCapabilities.BoundedWindow));
    }

    [Fact]
    public async Task Native_scan_auto_detects_without_columns_contract()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "t", new Dictionary<string, object?>
        {
            ["path"] = "whatever.csv",
            ["format"] = "csv",
        });

        var ok = source.TryGetNativeScan(spec, out var scan);

        Assert.True(ok);
        Assert.NotNull(scan);
        Assert.Contains("auto_detect = true", scan!.SqlFragment);
        Assert.DoesNotContain("columns = {", scan.SqlFragment);
        Assert.DoesNotContain("types = {", scan.SqlFragment);
    }

    // A zero-byte file has no header row to infer a schema from, yet read_csv(auto_detect = true)
    // fabricates a single `column0` VARCHAR column and the load succeeds with 0 rows -- a fabricated
    // schema that then propagates silently into every downstream sink. The connector must refuse
    // loudly instead, mirroring the header-contract guard: only when the file actually exists (an
    // absent file at plan time stays fine, the real read reports it).
    [Fact]
    public async Task Native_scan_rejects_a_zero_byte_file_without_columns_contract()
    {
        var path = Path.Combine(_dir, "empty.csv");
        await File.WriteAllTextAsync(path, "");

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "empty", new Dictionary<string, object?>
        {
            ["path"] = "empty.csv",
            ["format"] = "csv",
        });

        var ex = Assert.Throws<PzConnectorException>(() => source.TryGetNativeScan(spec, out _));

        Assert.False(ex.IsTransient);
        Assert.Contains("empty.csv", ex.Message);
        Assert.Contains("0 bytes", ex.Message);
    }

    [Fact]
    public async Task Native_scan_accepts_a_header_only_file_without_columns_contract()
    {
        var path = Path.Combine(_dir, "headeronly.csv");
        await File.WriteAllTextAsync(path, "id,name\n");

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "headeronly", new Dictionary<string, object?>
        {
            ["path"] = "headeronly.csv",
            ["format"] = "csv",
        });

        Assert.True(source.TryGetNativeScan(spec, out var scan));
        Assert.Contains("auto_detect = true", scan!.SqlFragment);
    }

    /// <summary>A partial contract behaves EXACTLY like a full one -- the same strict, pruning
    /// `columns = {...}` fragment, no `types=`/`auto_detect=true` middle case. There is deliberately no
    /// "declare some columns, infer the rest" (see <see cref="CsvSource.TryGetNativeScan"/>'s doc
    /// comment): a declared contract, partial or full, means "this is the schema, prune to
    /// it".</summary>
    [Fact]
    public async Task Native_scan_partial_contract_behaves_the_same_as_a_full_one()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "t", new Dictionary<string, object?>
        {
            ["path"] = "whatever.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        });

        var ok = source.TryGetNativeScan(spec, out var scan);

        Assert.True(ok);
        Assert.NotNull(scan);
        Assert.Contains("auto_detect = false", scan!.SqlFragment);
        Assert.Contains("columns = {", scan.SqlFragment);
        Assert.Contains("'id': 'BIGINT'", scan.SqlFragment);
        Assert.DoesNotContain("types = {", scan.SqlFragment);
    }

    [Fact]
    public async Task Native_scan_roundtrips_via_duckdb()
    {
        var path = Path.Combine(_dir, "nulls.csv");
        await File.WriteAllTextAsync(path, "id,qty,name\n1,5,Alice\n2,,\n3,7,\"\"\n");

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "nulls", new Dictionary<string, object?>
        {
            ["path"] = "nulls.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["qty"] = "int", ["name"] = "varchar" },
        });

        var ok = source.TryGetNativeScan(spec, out var scan);
        Assert.True(ok);

        await using var duck = DuckSession.Open(Path.Combine(_dir, "native.duckdb"));
        await duck.ExecuteAsync("create schema if not exists staging");
        await duck.ExecuteAsync($"create table staging.t as select * from {scan!.SqlFragment}");

        Assert.Equal(3, await duck.ScalarAsync<long>("select count(*) from staging.t"));
        Assert.Equal(5, await duck.ScalarAsync<long>("select qty from staging.t where id = 1"));
        Assert.Equal("Alice", await duck.ScalarAsync<string>("select name from staging.t where id = 1"));

        // Row 2: empty (unquoted) qty/name cells -> NULL, matching the universal path's policy.
        Assert.Equal(0L, await duck.ScalarAsync<long>("select count(*) from staging.t where id = 2 and qty is not null"));
        Assert.Equal(0L, await duck.ScalarAsync<long>("select count(*) from staging.t where id = 2 and name is not null"));

        // Row 3: quoted empty string name -> also NULL — a varchar column can never round-trip an
        // actual empty string, only NULL (same policy the universal CsvPartition.ParseValue enforces).
        Assert.Equal(0L, await duck.ScalarAsync<long>("select count(*) from staging.t where id = 3 and name is not null"));
        Assert.Equal(7, await duck.ScalarAsync<long>("select qty from staging.t where id = 3"));
    }

    /// <summary>Empirically proves (against the real bundled DuckDB, not just a fragment-string
    /// assertion) that a declared `columns:` contract prunes the staged result to EXACTLY its declared
    /// columns when it matches the file's real shape.</summary>
    [Fact]
    public async Task Native_scan_declared_contract_prunes_to_exactly_the_declared_columns()
    {
        var path = Path.Combine(_dir, "matching.csv");
        await File.WriteAllTextAsync(path, "id,qty\n1,5\n2,10\n3,15\n");

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "matching", new Dictionary<string, object?>
        {
            ["path"] = "matching.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["qty"] = "int" },
        });

        var ok = source.TryGetNativeScan(spec, out var scan);
        Assert.True(ok);
        Assert.Contains("auto_detect = false", scan!.SqlFragment);
        Assert.Contains("columns = {", scan.SqlFragment);

        await using var duck = DuckSession.Open(Path.Combine(_dir, "matching.duckdb"));
        await duck.ExecuteAsync("create schema if not exists staging");
        await duck.ExecuteAsync($"create table staging.t as select * from {scan!.SqlFragment}");

        var columnNames = await ListColumnNamesAsync(duck, "t");
        Assert.Equal(["id", "qty"], columnNames);
        Assert.Equal(3, await duck.ScalarAsync<long>("select count(*) from staging.t"));
        Assert.Equal(10, await duck.ScalarAsync<long>("select qty from staging.t where id = 2"));
    }

    /// <summary>A declared contract must confine the read: a file that grows an undeclared column must
    /// never silently ride along with an auto-detected type, widening the staged schema without
    /// warning. The `auto_detect = false, columns = {...}` fragment makes that impossible, verified
    /// here against a real DuckDB rather than by asserting on the fragment string: DuckDB's `columns=`
    /// map binds columns POSITIONALLY against each data row's actual width (not by matching declared
    /// names against the header), so a file with an undeclared extra column fails LOUDLY at read time
    /// with a row-width error. Either a loud failure or true name-based pruning would satisfy the
    /// invariant; DuckDB's mechanism here is the former. What matters is that nothing from an
    /// undeclared column ever reaches the staged table silently.</summary>
    [Fact]
    public async Task Native_scan_declared_contract_fails_loudly_instead_of_silently_widening_on_an_undeclared_extra_column()
    {
        var path = Path.Combine(_dir, "widened.csv");
        // Real file has THREE columns; the contract below only declares TWO -- the shape of an
        // existing project whose file grew a column the contract was never updated for.
        await File.WriteAllTextAsync(path, "id,qty,name\n1,5,Alice\n2,10,Bob\n");

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "widened", new Dictionary<string, object?>
        {
            ["path"] = "widened.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["qty"] = "int" },
        });

        var ok = source.TryGetNativeScan(spec, out var scan);
        Assert.True(ok);
        Assert.DoesNotContain("'name'", scan!.SqlFragment); // the undeclared column never enters the fragment at all

        await using var duck = DuckSession.Open(Path.Combine(_dir, "widened.duckdb"));
        await duck.ExecuteAsync("create schema if not exists staging");

        // The failure is what keeps a third, undeclared column ('name') out of the staged table.
        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => duck.ExecuteAsync($"create table staging.t as select * from {scan!.SqlFragment}"));
        Assert.Contains("Number of Columns", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static async Task<List<string>> ListColumnNamesAsync(DuckSession duck, string table)
    {
        var packed = await duck.ScalarAsync<string>(
            "select string_agg(column_name, ',' order by ordinal_position) from information_schema.columns " +
            $"where table_schema = 'staging' and table_name = '{table}'");
        return [.. (packed ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries)];
    }

    [Fact]
    public async Task Native_copy_parquet_writes_decimal128()
    {
        await using var duck = DuckSession.Open(Path.Combine(_dir, "copy.duckdb"));
        await duck.ExecuteAsync("create schema if not exists staging");

        var schema = new Schema([new Field("amount", new Decimal128Type(38, 9), nullable: true)], null);
        var builder = new ArrowBatchBuilder(schema, targetBatchBytes: int.MaxValue);
        builder.AppendRow([12345.123456789m]);
        var batch = builder.Flush()!;

        await duck.IngestArrowAsync("staging.src_files__amounts", schema, OneBatch(batch));

        var connector = new LocalFilesConnector();
        await using var sink = await ((ISinkConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new OutputSpec("lake", "out", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "out", ["format"] = "parquet" });

        var ok = sink.TryGetNativeCopy(spec, out var copy);
        Assert.True(ok);

        // TryGetNativeCopy does not create the output directory: planning must be side-effect-free.
        // The engine (SinkWriteExecutor) creates it at execution time, right before running the COPY,
        // so this test does the same.
        Directory.CreateDirectory(Path.GetDirectoryName(copy!.Finalizations[0].TempPath)!);

        await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", "staging.src_files__amounts"));
        foreach (var move in copy.Finalizations)
        {
            File.Move(move.TempPath, move.FinalPath, overwrite: true);
        }

        var finalPath = Path.Combine(_dir, "out", "out.parquet");
        Assert.True(File.Exists(finalPath));

        var quoted = finalPath.Replace("'", "''");
        var value = await duck.ScalarAsync<decimal>($"select amount from read_parquet('{quoted}')");
        Assert.Equal(12345.123456789m, value);
    }

    /// <summary>No `format:` at all takes the native tier's default (parquet) rather than declining
    /// native and falling back to the universal Parquet.Net session -- native is the preferred tier, and
    /// the sink's documented default format is parquet, so a format-less output should get the same
    /// native COPY a `format: parquet` output gets.</summary>
    [Fact]
    public async Task Sink_without_format_takes_the_native_parquet_copy()
    {
        var connector = new LocalFilesConnector();
        await using var sink = await ((ISinkConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new OutputSpec("lake", "out", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "out" });

        var ok = sink.TryGetNativeCopy(spec, out var copy);
        Assert.True(ok);
        Assert.Contains("(format parquet)", copy!.CopySql);
        Assert.Equal("COPY TO parquet", copy.Mechanism);
        Assert.EndsWith("out.parquet", copy.Finalizations[0].FinalPath);
    }

    [Fact]
    public async Task Sink_with_unknown_format_is_refused_at_plan_time_with_PZ0361()
    {
        var connector = new LocalFilesConnector();
        await using var sink = await ((ISinkConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new OutputSpec("lake", "out", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "out", ["format"] = "orc" });

        var ex = Assert.Throws<PzConnectorException>(() => sink.TryGetNativeCopy(spec, out _));
        Assert.StartsWith("PZ0361: output '", ex.Message, StringComparison.Ordinal);
        Assert.Contains("(supported: csv, json, parquet, tsv)", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Dataset_schema_embeds_the_catalog_format_properties()
    {
        var connector = new LocalFilesConnector();
        Assert.Contains(FileFormatCatalog.SchemaProperties, connector.DatasetConfigSchema, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Native_copy_replace_is_atomic()
    {
        await using var duck = DuckSession.Open(Path.Combine(_dir, "copy2.duckdb"));
        await duck.ExecuteAsync("create schema if not exists staging");
        await duck.ExecuteAsync("create table staging.t as select 1 as id");

        var connector = new LocalFilesConnector();
        var spec = new OutputSpec("lake", "out", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "out2", ["format"] = "parquet" });

        var outputDir = Path.Combine(_dir, "out2");
        var finalPath = Path.Combine(outputDir, "out.parquet");

        await RunNativeCopyOnceAsync(connector, spec, "staging.t", duck);
        Assert.True(File.Exists(finalPath));
        Assert.Empty(Directory.EnumerateFiles(outputDir, ".pz-native-*"));

        await RunNativeCopyOnceAsync(connector, spec, "staging.t", duck);
        Assert.True(File.Exists(finalPath));
        Assert.Single(Directory.EnumerateFiles(outputDir, "*.parquet"));
        Assert.Empty(Directory.EnumerateFiles(outputDir, ".pz-native-*"));
    }

    private async Task RunNativeCopyOnceAsync(LocalFilesConnector connector, OutputSpec spec, string sourceRelation, DuckSession duck)
    {
        await using var sink = await ((ISinkConnector)connector).OpenAsync(Config, CancellationToken.None);

        var ok = sink.TryGetNativeCopy(spec, out var copy);
        Assert.True(ok);

        // See the comment in Native_copy_parquet_writes_decimal128: the probe does not create the
        // output directory, so this helper (standing in for SinkWriteExecutor) does.
        Directory.CreateDirectory(Path.GetDirectoryName(copy!.Finalizations[0].TempPath)!);

        await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", sourceRelation));
        foreach (var move in copy.Finalizations)
        {
            File.Move(move.TempPath, move.FinalPath, overwrite: true);
        }
    }

    private static async IAsyncEnumerable<RecordBatch> OneBatch(RecordBatch batch)
    {
        yield return batch;
        await Task.CompletedTask;
    }

    /// <summary>DATE and TIMESTAMP(µs, UTC) logical types must round-trip through the REAL sink write
    /// session (universal path), verified behaviorally via a throwaway DuckDB `read_parquet` DESCRIBE.
    /// Writing both Date32 and Timestamp Arrow columns as a plain `DataField(typeof(DateTime))` would
    /// let Parquet.Net erase both to a TIMESTAMP-kind logical type, losing the DATE distinction.</summary>
    [Fact]
    public async Task Parquet_sink_writes_date_and_timestamp_logical_types()
    {
        var connector = new LocalFilesConnector();
        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, nullable: false),
            new Field("d", Date32Type.Default, nullable: true),
            new Field("ts", new TimestampType(TimeUnit.Microsecond, "+00:00"), nullable: true),
        ], null);

        var builder = new ArrowBatchBuilder(schema, targetBatchBytes: int.MaxValue);
        builder.AppendRow([1L, new DateOnly(2026, 1, 15), new DateTimeOffset(2026, 1, 15, 10, 30, 0, TimeSpan.Zero)]);
        builder.AppendRow([2L, new DateOnly(2026, 2, 20), new DateTimeOffset(2026, 2, 20, 11, 0, 0, TimeSpan.Zero)]);
        builder.AppendRow([3L, null, null]);
        // Row 4 uses a NON-zero offset (+05:00): if isAdjustedToUTC handling were a pass-through of the
        // wall-clock value instead of a real UTC conversion, this would read back as 15:00 instead of
        // the correct UTC instant 10:00 — proving normalization, not just UTC-input round-tripping.
        builder.AppendRow([4L, new DateOnly(2026, 3, 10), new DateTimeOffset(2026, 3, 10, 15, 0, 0, TimeSpan.FromHours(5))]);
        var batch = builder.Flush()!;

        var spec = new OutputSpec("lake", "out", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "out", ["format"] = "parquet" });

        await using (var sink = await ((ISinkConnector)connector).OpenAsync(Config, CancellationToken.None))
        await using (var writeSession = await sink.BeginWriteAsync(spec, schema, CancellationToken.None))
        {
            await writeSession.WriteBatchAsync(batch, CancellationToken.None);
            batch.Dispose();
            await writeSession.CommitAsync(CancellationToken.None);
        }

        var parquetPath = Path.Combine(_dir, "out", "out.parquet").Replace("'", "''");
        await using var duck = DuckSession.Open(Path.Combine(_dir, "probe.duckdb"));

        var dateType = await duck.ScalarAsync<string>(
            $"select column_type from (describe select * from read_parquet('{parquetPath}')) where column_name = 'd'");
        var tsType = await duck.ScalarAsync<string>(
            $"select column_type from (describe select * from read_parquet('{parquetPath}')) where column_name = 'ts'");
        Assert.Equal("DATE", dateType);
        Assert.StartsWith("TIMESTAMP", tsType); // TIMESTAMP (µs); must NOT be TIMESTAMP_NS

        var d0 = await duck.ScalarAsync<DateOnly>($"select d from read_parquet('{parquetPath}') where id = 1");
        Assert.Equal(new DateOnly(2026, 1, 15), d0);

        // ts's value round-trips too, but read as a string (TIMESTAMPTZ scalars come back as
        // DateTimeOffset, which Convert.ChangeType — DuckSession.ScalarAsync's conversion — cannot
        // coerce into a plain DateTime; the type-shape assertion above is the load-bearing one here).
        var tsText = await duck.ScalarAsync<string>(
            $"select ts::varchar from read_parquet('{parquetPath}') where id = 1");
        Assert.StartsWith("2026-01-15 10:30:00", tsText);

        // Row 4's source wall-clock time is 15:00+05:00; the correct UTC instant is 10:00. If the
        // sink's isAdjustedToUTC conversion degraded to a pass-through of the local components, this
        // would read back as 15:00 instead.
        var tsTextOffset = await duck.ScalarAsync<string>(
            $"select ts::varchar from read_parquet('{parquetPath}') where id = 4");
        Assert.StartsWith("2026-03-10 10:00:00", tsTextOffset);

        var nullCount = await duck.ScalarAsync<long>($"select count(*) from read_parquet('{parquetPath}') where d is null");
        Assert.Equal(1L, nullCount);
    }

    [Fact]
    public void Published_schemas_are_valid_json_schema()
    {
        var c = new LocalFilesConnector();
        foreach (var s in new[] { c.ConnectionConfigSchema, c.DatasetConfigSchema })
        {
            var schema = Json.Schema.JsonSchema.FromText(s); // throws on malformed
            Assert.NotNull(schema);
        }
    }

    private async Task WriteOneAsync(LocalFilesConnector connector, Schema schema, OutputSpec spec, long id, string name)
    {
        await using var sink = await ((ISinkConnector)connector).OpenAsync(Config, CancellationToken.None);

        var idBuilder = new Int64Array.Builder();
        idBuilder.Append(id);
        var nameBuilder = new StringArray.Builder();
        nameBuilder.Append(name);
        var batch = new RecordBatch(schema, [idBuilder.Build(), nameBuilder.Build()], 1);

        await using var writeSession = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
        await writeSession.WriteBatchAsync(batch, CancellationToken.None);
        batch.Dispose();
        await writeSession.CommitAsync(CancellationToken.None);
    }
    /// <summary>Only a contract-less csv scan lets DuckDB's auto_detect invent the schema, so only it
    /// declares <see cref="NativeScan.SchemaInferred"/>
    /// — the engine's integer-inference lint keys off this flag. A declared contract governs the read
    /// (nothing inferred), and parquet carries its own exact schema (nothing to mis-infer).</summary>
    [Fact]
    public async Task Csv_scan_declares_schema_inferred_only_when_contract_less()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var contractLess = new DatasetSpec("files", "t", new Dictionary<string, object?>
        {
            ["path"] = "t.csv",
            ["format"] = "csv",
        });
        Assert.True(source.TryGetNativeScan(contractLess, out var inferred));
        Assert.True(inferred!.SchemaInferred);
        Assert.NotNull(inferred.SniffFragment);
        Assert.StartsWith("sniff_csv(", inferred.SniffFragment, StringComparison.Ordinal);
        Assert.Contains("t.csv", inferred.SniffFragment, StringComparison.Ordinal);

        var declared = new DatasetSpec("files", "t", new Dictionary<string, object?>
        {
            ["path"] = "t.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        });
        Assert.True(source.TryGetNativeScan(declared, out var contracted));
        Assert.False(contracted!.SchemaInferred);
        Assert.Null(contracted.SniffFragment);

        var parquet = new DatasetSpec("files", "t", new Dictionary<string, object?>
        {
            ["path"] = "t.parquet",
            ["format"] = "parquet",
        });
        Assert.True(source.TryGetNativeScan(parquet, out var exact));
        Assert.False(exact!.SchemaInferred);
        Assert.Null(exact.SniffFragment);
    }
}
