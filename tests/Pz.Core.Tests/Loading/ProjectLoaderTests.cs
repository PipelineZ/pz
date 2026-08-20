using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Tests.Loading;

public class ProjectLoaderTests
{
    /// <summary>A project carries ONE connection list, so a fixture declaring both directions does not
    /// yield a single element. These pick the half a given assertion is about -- by NAME, since every
    /// connection shares the same file.</summary>
    private static ConnectionDef SourceOf(PzProject project) =>
        Assert.Single(project.Connections, c => c.Datasets.Count > 0);

    private static ConnectionDef SinkOf(PzProject project) =>
        Assert.Single(project.Connections, c => c.Datasets.Count == 0);

    private static readonly IReadOnlyDictionary<string, string> Env = new Dictionary<string, string>
    {
        ["DATA_DIR"] = "/tmp/pz-data",
        ["OUT_DIR"] = "/tmp/pz-out",
    };

    private static string FixturePath(string name) =>
        Path.Combine(AppContext.BaseDirectory, "Fixtures", name);

    [Fact]
    public void Load_discovers_pipelines_by_convention()
    {
        var project = ProjectLoader.Load(FixturePath("hello-pz"), Env);
        Assert.Equal(3, project.Pipelines.Count);
        Assert.Contains(project.Pipelines, p => p.Name == "stg_orders");
        Assert.Contains(project.Pipelines, p => p.Name == "orders_enriched");
        Assert.Contains(project.Pipelines, p => p.Name == "order_totals");
    }

    [Fact]
    public void Load_applies_sidecar_config()
    {
        var project = ProjectLoader.Load(FixturePath("hello-pz"), Env);
        var enriched = project.Pipelines.Single(p => p.Name == "orders_enriched");
        Assert.Equal("table", enriched.Materialization);
        Assert.Equal(new[] { "daily", "crm" }, enriched.Tags);
        Assert.Equal(2, enriched.Checks.Count);
        Assert.Equal("not_null", enriched.Checks[0].Type);
        Assert.Equal(new[] { "id", "email" }, enriched.Checks[0].Columns);
        var stg = project.Pipelines.Single(p => p.Name == "stg_orders");
        Assert.Equal("table", stg.Materialization);
        Assert.Empty(stg.Tags);
        Assert.Empty(stg.Checks);
    }

    [Fact]
    public void Env_interpolation_replaces_declared_vars()
    {
        var project = ProjectLoader.Load(FixturePath("hello-pz"), Env);
        Assert.Equal("/tmp/pz-data", SourceOf(project).Connection["root"]);
        Assert.Equal("/tmp/pz-out", SinkOf(project).Connection["root"]);
    }

    [Fact]
    public void Undeclared_env_var_is_error_PZ0103()
    {
        var envMissingDataDir = new Dictionary<string, string> { ["OUT_DIR"] = "/tmp/pz-out" };
        var ex = Assert.Throws<PzValidationException>(
            () => ProjectLoader.Load(FixturePath("hello-pz"), envMissingDataDir));
        var error = Assert.Single(ex.Errors, e => e.Code == "PZ0103");
        Assert.Contains("DATA_DIR", error.Message);
        Assert.NotNull(error.File);
        Assert.Contains("connections.yml", error.File);
        Assert.Contains("DATA_DIR", ex.Message);
    }

    [Fact]
    public void Var_overrides_win_over_project_vars()
    {
        var overrides = new Dictionary<string, object?> { ["min_amount"] = 25L };
        var project = ProjectLoader.Load(FixturePath("hello-pz"), Env, overrides);
        Assert.Equal(25L, project.Vars["min_amount"]);
        Assert.True(project.Vars.ContainsKey("statuses")); // non-overridden vars survive
    }

    [Fact]
    public void Duplicate_pipeline_name_is_error_PZ0110()
    {
        var ex = Assert.Throws<PzValidationException>(
            () => ProjectLoader.Load(FixturePath("broken-dup-pipeline"), Env));
        var error = Assert.Single(ex.Errors, e => e.Code == "PZ0110");
        Assert.Contains("stg_orders", error.Message);
    }

    [Fact]
    public void Loaded_model_carries_project_relative_file_paths()
    {
        var project = ProjectLoader.Load(FixturePath("hello-pz"), Env);
        Assert.Equal("connections.yml", project.Connections[0].FilePath);
        Assert.Equal("pipelines/stg_orders.sql",
            project.Pipelines.Single(p => p.Name == "stg_orders").FilePath);
    }

    [Fact]
    public void Engine_config_parses_threads_and_duckdb_options()
    {
        var project = ProjectLoader.Load(FixturePath("hello-pz"), Env);
        Assert.Equal(2, project.Engine.Threads);
        Assert.Equal("1GiB", project.Engine.DuckDb?.MemoryLimit);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Invalid_engine_threads_is_error_PZ0120(int threads)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            File.WriteAllText(Path.Combine(dir, "project.yml"),
                $"name: bad_engine\nversion: 0.1.0\nengine:\n  threads: {threads}\n");

            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            var error = Assert.Single(ex.Errors, e => e.Code == "PZ0120");
            Assert.Contains("project.yml", error.Message);
            Assert.Contains(threads.ToString(), error.Message);
            Assert.Equal("threads must be >= 1", error.Hint);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Theory]
    [InlineData(null, false)]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public void Force_universal_parses(string? value, bool expected)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var engineBlock = value is null ? string.Empty : $"engine:\n  force_universal: {value}\n";
            File.WriteAllText(Path.Combine(dir, "project.yml"),
                $"name: force_universal_test\nversion: 0.1.0\n{engineBlock}");

            var project = ProjectLoader.Load(dir, Env);
            Assert.Equal(expected, project.Engine.ForceUniversal);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>Project-wide `engine.check_samples` default; absent
    /// -> true, same convention as `force_universal` above.</summary>
    [Theory]
    [InlineData(null, true)]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public void Check_samples_parses(string? value, bool expected)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var engineBlock = value is null ? string.Empty : $"engine:\n  check_samples: {value}\n";
            File.WriteAllText(Path.Combine(dir, "project.yml"),
                $"name: check_samples_test\nversion: 0.1.0\n{engineBlock}");

            var project = ProjectLoader.Load(dir, Env);
            Assert.Equal(expected, project.Engine.CheckSamples);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>A check's dict form (`not_null: { columns: [...],
    /// sample_values: false }`) carries both `columns:` and the per-check opt-out override; the
    /// bare-list form (`unique: [id]`) still parses with SampleValues absent (null -> inherit the
    /// project default).</summary>
    [Fact]
    public void Sample_values_parses_on_check()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        Directory.CreateDirectory(Path.Combine(dir, "pipelines", "configs"));
        try
        {
            File.WriteAllText(Path.Combine(dir, "project.yml"), "name: sample_values_test\nversion: 0.1.0\n");
            File.WriteAllText(Path.Combine(dir, "pipelines", "p.sql"), "select 1 as id\n");
            File.WriteAllText(Path.Combine(dir, "pipelines", "configs", "p.yml"), """
                pipeline: p
                checks:
                  - not_null:
                      columns: [id]
                      sample_values: false
                  - unique: [id]
                """);

            var project = ProjectLoader.Load(dir, Env);
            var pipeline = Assert.Single(project.Pipelines);
            Assert.Equal(2, pipeline.Checks.Count);

            var notNull = pipeline.Checks[0];
            Assert.Equal("not_null", notNull.Type);
            Assert.Equal(new[] { "id" }, notNull.Columns);
            Assert.Equal(false, notNull.SampleValues);

            var unique = pipeline.Checks[1];
            Assert.Equal("unique", unique.Type);
            Assert.Equal(new[] { "id" }, unique.Columns);
            Assert.Null(unique.SampleValues);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData(33554432, 33554432)]
    public void Batch_bytes_parses_and_validates(int? batchBytes, int? expected)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var engineBlock = batchBytes is null ? string.Empty : $"engine:\n  batch_bytes: {batchBytes}\n";
            File.WriteAllText(Path.Combine(dir, "project.yml"),
                $"name: batch_bytes_test\nversion: 0.1.0\n{engineBlock}");

            var project = ProjectLoader.Load(dir, Env);
            Assert.Equal(expected, project.Engine.BatchBytes);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Theory]
    [InlineData(1023)]
    [InlineData(1_073_741_825)] // 1GiB + 1
    public void Invalid_batch_bytes_is_error_PZ0120(int batchBytes)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            File.WriteAllText(Path.Combine(dir, "project.yml"),
                $"name: bad_batch_bytes\nversion: 0.1.0\nengine:\n  batch_bytes: {batchBytes}\n");

            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            var error = Assert.Single(ex.Errors, e => e.Code == "PZ0120");
            Assert.Contains(batchBytes.ToString(), error.Message);
            Assert.Contains("1048576", error.Message);
            Assert.Contains("536870912", error.Message);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Breaker_config_parses()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            File.WriteAllText(Path.Combine(dir, "project.yml"),
                "name: breaker_test\nversion: 0.1.0\nengine:\n  breaker:\n    failure_threshold: 5\n    cool_down: 2m\n");

            var project = ProjectLoader.Load(dir, Env);

            Assert.NotNull(project.Engine.Breaker);
            Assert.Equal(5, project.Engine.Breaker!.FailureThreshold);
            Assert.Equal(TimeSpan.FromMinutes(2), project.Engine.Breaker.CoolDown);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Breaker_config_absent_is_null()
    {
        var project = ProjectLoader.Load(FixturePath("hello-pz"), Env);
        Assert.Null(project.Engine.Breaker);
    }

    [Fact]
    public void Breaker_failure_threshold_zero_is_error_PZ0120()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            File.WriteAllText(Path.Combine(dir, "project.yml"),
                "name: bad_breaker\nversion: 0.1.0\nengine:\n  breaker:\n    failure_threshold: 0\n    cool_down: 2m\n");

            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            var error = Assert.Single(ex.Errors, e => e.Code == "PZ0120");
            Assert.Contains("failure_threshold", error.Message);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Breaker_failure_threshold_overflow_is_error_PZ0120()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            File.WriteAllText(Path.Combine(dir, "project.yml"),
                "name: bad_breaker\nversion: 0.1.0\nengine:\n  breaker:\n    failure_threshold: 9999999999\n    cool_down: 2m\n");

            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            var error = Assert.Single(ex.Errors, e => e.Code == "PZ0120");
            Assert.Contains("failure_threshold", error.Message);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>The out-of-range threshold message must name the full legal
    /// range, mirroring <c>batch_bytes</c>' "must be between X and Y" wording (see
    /// <c>InvalidEngine_batch_bytes_out_of_range_names_the_bound</c>-style pinning elsewhere in this
    /// file) instead of the less precise "must be an integer &gt;= 1".</summary>
    [Fact]
    public void Breaker_failure_threshold_overflow_names_the_full_range()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            File.WriteAllText(Path.Combine(dir, "project.yml"),
                "name: bad_breaker\nversion: 0.1.0\nengine:\n  breaker:\n    failure_threshold: 9999999999\n    cool_down: 2m\n");

            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            var error = Assert.Single(ex.Errors, e => e.Code == "PZ0120");
            Assert.Contains("must be between 1 and 2147483647", error.Message);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Breaker_cool_down_invalid_is_error_PZ0120()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            File.WriteAllText(Path.Combine(dir, "project.yml"),
                "name: bad_breaker\nversion: 0.1.0\nengine:\n  breaker:\n    failure_threshold: 5\n    cool_down: fast\n");

            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            var error = Assert.Single(ex.Errors, e => e.Code == "PZ0120");
            Assert.Contains("cool_down", error.Message);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Breaker_missing_fields_say_missing_not_got_empty_string()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            File.WriteAllText(Path.Combine(dir, "project.yml"),
                "name: bad_breaker\nversion: 0.1.0\nengine:\n  breaker: {}\n");

            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            var errors = ex.Errors.Where(e => e.Code == "PZ0120").ToList();
            Assert.Equal(2, errors.Count);
            Assert.Contains(errors, e => e.Message.Contains("failure_threshold is missing"));
            Assert.Contains(errors, e => e.Message.Contains("cool_down is missing"));
            Assert.DoesNotContain(errors, e => e.Message.Contains("(got ''"));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Feeds_in_project_yml_is_refused_with_PZ0352()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            File.WriteAllText(Path.Combine(dir, "project.yml"), """
                name: feeds_test
                version: 0.1.0

                feeds:
                  - "https://api.nuget.org/v3/index.json"
                """);

            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            var error = Assert.Single(ex.Errors, e => e.Code == "PZ0352");
            Assert.Contains("removed", error.Message);
            Assert.Contains("PZ_FEEDS", error.Hint);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Malformed_yaml_is_aggregated_error_PZ0101()
    {
        var ex = Assert.Throws<PzValidationException>(
            () => ProjectLoader.Load(FixturePath("broken-yaml-syntax"), Env));
        var error = Assert.Single(ex.Errors, e => e.Code == "PZ0101");
        Assert.NotNull(error.File);
        Assert.Contains("connections.yml", error.File);
        Assert.Contains(error.File!, ex.Message);
    }

    // --- Incremental config surface + merge-keys validation ---

    private static string WriteProject(string name, string sourcesYaml, string sinksYaml)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "project.yml"), $"name: {name}\nversion: 0.1.0\n");
        File.WriteAllText(Path.Combine(dir, "connections.yml"), sourcesYaml + "\n" + sinksYaml);
        return dir;
    }

    [Fact]
    public void Sync_mode_incremental_parses_into_dataset_def()
    {
        var dir = WriteProject("incremental_test", """
            crm:
              connector: localfiles
              root: /data
              entities:
                orders:
                  read:
                    path: orders.csv
                    format: csv
                    sync:
                      mode: incremental
                      cursor: updated_at
            """, """
            lake:
              connector: localfiles
              root: /out
            """);
        try
        {
            var project = ProjectLoader.Load(dir, Env);
            var dataset = SourceOf(project).Datasets.Single();
            Assert.Equal(new SyncModeDef(SyncMode.Incremental, new IncrementalDef("updated_at")), dataset.SyncMode);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Sync_absent_parses_to_null_sync_mode()
    {
        var dir = WriteProject("no_incremental_test", """
            crm:
              connector: localfiles
              root: /data
              entities:
                orders:
                  read:
                    path: orders.csv
                    format: csv
            """, """
            lake:
              connector: localfiles
              root: /out
            """);
        try
        {
            var project = ProjectLoader.Load(dir, Env);
            var dataset = SourceOf(project).Datasets.Single();
            Assert.Null(dataset.SyncMode);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    // The old `incremental:` block is retired regardless of its shape -- RetiredReadSurface fires
    // before any mapping/cursor check would even run.
    [Fact]
    public void Old_incremental_key_is_error_PZ0332()
    {
        var dir = WriteProject("bad_incremental_test", """
            crm:
              connector: localfiles
              root: /data
              entities:
                orders:
                  read:
                    path: orders.csv
                    format: csv
                    incremental: updated_at
            """, """
            lake:
              connector: localfiles
              root: /out
            """);
        try
        {
            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.RetiredReadSurface);
            Assert.Contains("incremental", error.Message);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }




    // ---- per-instance retry config ----

    private static string TempProject(string sourceYaml, string? sinkYaml = null)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "project.yml"), "name: t\nversion: \"1\"\n");
        // Both directions land in one file, so the two blocks a caller passes are simply concatenated.
        File.WriteAllText(Path.Combine(dir, "connections.yml"),
            sinkYaml is null ? sourceYaml : sourceYaml + "\n" + sinkYaml);
        return dir;
    }

    private const string RetrySourceYaml = """
        pg:
          connector: postgres
          retry:
            max_attempts: 8
            base_delay: 2s
            max_delay: 5m
          entities:
            orders:
              read:
        """;

    [Fact]
    public void Source_retry_block_parses()
    {
        var project = ProjectLoader.Load(TempProject(RetrySourceYaml), Env);
        var retry = Assert.Single(project.Connections).Retry;
        Assert.NotNull(retry);
        Assert.Equal(8, retry.MaxAttempts);
        Assert.Equal(TimeSpan.FromSeconds(2), retry.BaseDelay);
        Assert.Equal(TimeSpan.FromMinutes(5), retry.MaxDelay);
    }

    [Fact]
    public void Sink_retry_block_parses()
    {
        var sinkYaml = """
            out:
              connector: postgres
              retry:
                max_attempts: 4
            """;
        var project = ProjectLoader.Load(TempProject(RetrySourceYaml, sinkYaml), Env);
        var retry = SinkOf(project).Retry;
        Assert.NotNull(retry);
        Assert.Equal(4, retry.MaxAttempts);
        Assert.Null(retry.BaseDelay); // absent keys stay null — engine overlays the default
        Assert.Null(retry.MaxDelay);
    }

    [Fact]
    public void Absent_retry_block_is_null()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
            """;
        var project = ProjectLoader.Load(TempProject(yaml), Env);
        Assert.Null(Assert.Single(project.Connections).Retry);
    }

    [Fact]
    public void Dataset_retry_override_parses_and_is_stripped_from_options()
    {
        var yaml = """
            pg:
              connector: postgres
              retry:
                max_attempts: 5
              entities:
                orders:
                  read:
                    retry:
                      max_attempts: 10
                      max_delay: 10m
            """;
        var project = ProjectLoader.Load(TempProject(yaml), Env);
        var source = Assert.Single(project.Connections);
        Assert.Equal(5, source.Retry!.MaxAttempts);
        var dataset = Assert.Single(source.Datasets);
        Assert.NotNull(dataset.Retry);
        Assert.Equal(10, dataset.Retry.MaxAttempts);
        Assert.Equal(TimeSpan.FromMinutes(10), dataset.Retry.MaxDelay);
        Assert.False(dataset.Options.ContainsKey("retry")); // reserved key — never a connector option
    }


    [Fact]
    public void Invalid_dataset_retry_is_error_PZ0121()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    retry:
                      max_attempts: 0
            """;
        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(TempProject(yaml), Env));
        var error = Assert.Single(ex.Errors, e => e.Code == "PZ0121");
        Assert.Contains("entity 'orders'", error.ToString());
    }


    [Theory]
    [InlineData("retry: nope")]                                  // not a mapping
    [InlineData("retry:\n  max_attempts: 0")]                    // < 1
    [InlineData("retry:\n  max_attempts: eight")]                // non-integer
    [InlineData("retry:\n  base_delay: fast")]                   // unparseable duration
    [InlineData("retry:\n  base_delay: 0s")]                     // non-positive
    [InlineData("retry:\n  max_delay: -3s")]                     // unparseable (sign)
    [InlineData("retry:\n  base_delay: 5m\n  max_delay: 2s")]    // max < base
    public void Invalid_retry_block_is_error_PZ0121(string retryYaml)
    {
        var yaml = $$"""
            pg:
              connector: postgres
              {{retryYaml}}
              entities:
                orders:
                  read:
            """;
        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(TempProject(yaml), Env));
        var error = Assert.Single(ex.Errors, e => e.Code == "PZ0121");
        Assert.Contains("connections.yml", error.ToString());
    }

    [Fact]
    public void Sync_mode_incremental_window_fields_parse()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    columns:
                      updated_at: timestamp
                    sync:
                      mode: incremental
                      cursor: updated_at
                      max_window: 1d
                      initial: "2020-01-01"
                      until: "2026-07-01"
            """;
        var project = ProjectLoader.Load(TempProject(yaml), Env);
        var incremental = Assert.Single(Assert.Single(project.Connections).Datasets).SyncMode!.Incremental;
        Assert.NotNull(incremental);
        Assert.Equal("updated_at", incremental.Cursor);
        Assert.Equal("1d", incremental.MaxWindow);
        Assert.Equal("2020-01-01", incremental.Initial);
        Assert.Equal("2026-07-01", incremental.Until);
    }

    [Fact]
    public void Sync_mode_incremental_window_fields_default_null()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    sync:
                      mode: incremental
                      cursor: updated_at
            """;
        var project = ProjectLoader.Load(TempProject(yaml), Env);
        var incremental = Assert.Single(Assert.Single(project.Connections).Datasets).SyncMode!.Incremental;
        Assert.NotNull(incremental);
        Assert.Null(incremental.MaxWindow);
        Assert.Null(incremental.Initial);
        Assert.Null(incremental.Until);
    }

    [Theory]
    [InlineData("max_window: [1d]")]   // non-scalar
    [InlineData("initial: {a: 1}")]    // non-scalar
    [InlineData("until: [x]")]         // non-scalar
    public void Sync_mode_incremental_window_fields_must_be_scalars(string badField)
    {
        var yaml = $$"""
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    sync:
                      mode: incremental
                      cursor: updated_at
                      {{badField}}
            """;
        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(TempProject(yaml), Env));
        Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SyncModeInvalid);
    }

    [Fact]
    public void Sync_mode_incremental_numeric_scalars_round_trip_as_invariant_strings()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    sync:
                      mode: incremental
                      cursor: id
                      max_window: 1000
                      initial: 100
                      until: 5000
            """;
        var project = ProjectLoader.Load(TempProject(yaml), Env);
        var incremental = Assert.Single(Assert.Single(project.Connections).Datasets).SyncMode!.Incremental;
        Assert.NotNull(incremental);
        Assert.Equal("1000", incremental.MaxWindow);
        Assert.Equal("100", incremental.Initial);
        Assert.Equal("5000", incremental.Until);
    }

    // ---- There is no opaque `sync: {}` marker -- an empty sync block parses as a MISSING mode
    // (SyncModeInvalid), whose hint tells the user to delete the block entirely (a connector-managed
    // feed needs no sync: block). ----

    [Fact]
    public void Empty_sync_block_is_missing_mode_error()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                changes:
                  read:
                    path: /x
                    sync: {}
            """;
        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(TempProject(yaml), Env));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SyncModeInvalid);
        Assert.Contains("missing required field 'mode'", error.Message);
        Assert.Contains("delete it", error.Message);
    }

    // ---- per-instance max_concurrency config ----

    private const string SourceYamlNoCap = """
        pg:
          connector: postgres
          entities:
            orders:
              read:
        """;

    [Fact]
    public void Source_max_concurrency_parses()
    {
        var yaml = """
            pg:
              connector: postgres
              max_concurrency: 2
              entities:
                orders:
                  read:
            """;
        var project = ProjectLoader.Load(TempProject(yaml), Env);
        Assert.Equal(2, Assert.Single(project.Connections).MaxConcurrency);
    }

    [Fact]
    public void Sink_max_concurrency_parses_and_absent_is_null()
    {
        var sinkYaml = """
            out:
              connector: postgres
              max_concurrency: 1
            """;
        var project = ProjectLoader.Load(TempProject(SourceYamlNoCap, sinkYaml), Env);
        Assert.Equal(1, SinkOf(project).MaxConcurrency);
        Assert.Null(SourceOf(project).MaxConcurrency);
    }

    [Theory]
    [InlineData("max_concurrency: 0")]
    [InlineData("max_concurrency: -3")]
    [InlineData("max_concurrency: two")]
    public void Invalid_max_concurrency_is_PZ0122(string badLine)
    {
        var yaml = $$"""
            pg:
              connector: postgres
              {{badLine}}
              entities:
                orders:
                  read:
            """;
        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(TempProject(yaml), Env));
        Assert.Single(ex.Errors, e => e.Code == "PZ0122");
    }

    // `max_rows_per_second` is not a reserved key — it flows to the connector options bag like any
    // unknown key, and strict-schema connectors reject it there.
    [Fact]
    public void Max_rows_per_second_is_not_reserved_and_lands_in_dataset_options()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    max_rows_per_second: 500
            """;
        var project = ProjectLoader.Load(TempProject(yaml), Env);
        var dataset = Assert.Single(Assert.Single(project.Connections).Datasets);
        Assert.True(dataset.Options.ContainsKey("max_rows_per_second"));
        Assert.Equal(500L, dataset.Options["max_rows_per_second"]);
    }

    // ---- accept_duplicates output key ----

    private static string LoadProjectWithSinkYaml(string sinkYaml)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "project.yml"), "name: t\nversion: \"1\"\n");
        File.WriteAllText(Path.Combine(dir, "connections.yml"), SourceYamlNoCap + "\n" + sinkYaml);
        return dir;
    }



    // The old `accept_duplicates:` key is retired regardless of its shape -- RetiredWriteSurface fires
    // before any bool check would even run (the write-side twin of Old_incremental_key_is_error_PZ0332).

    // ---- PZ0113 compile-time check validation + column: normalization ----

    /// <summary>Minimal project (one pipeline `p`) with the given `checks:` YAML list body, for
    /// PZ0113 check-validation tests. Caller must delete the returned directory.</summary>
    private static string WriteCheckProject(string checksYaml)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path.Combine(dir, "pipelines", "configs"));
        File.WriteAllText(Path.Combine(dir, "project.yml"), "name: check_validation_test\nversion: 0.1.0\n");
        File.WriteAllText(Path.Combine(dir, "pipelines", "p.sql"), "select 1 as id\n");
        File.WriteAllText(Path.Combine(dir, "pipelines", "configs", "p.yml"),
            "pipeline: p\nchecks:\n" + checksYaml);
        return dir;
    }

    /// <summary>`column:` (singular, dbt parity) normalizes into CheckDef.Columns and is
    /// stripped from Options, so node naming and canonical hashing work unchanged.</summary>
    [Fact]
    public void Column_singular_normalizes_into_columns()
    {
        var dir = WriteCheckProject("""
              - freshness: { column: updated_at, max_age: 24h }
              - accepted_values: { column: status, values: [a, b] }
            """);
        try
        {
            var project = ProjectLoader.Load(dir, Env);
            var pipeline = Assert.Single(project.Pipelines);

            Assert.Equal(new[] { "updated_at" }, pipeline.Checks[0].Columns);
            Assert.Equal(new[] { "max_age" }, pipeline.Checks[0].Options.Keys.Order().ToArray());
            Assert.Equal(new[] { "status" }, pipeline.Checks[1].Columns);
            Assert.Equal(new[] { "values" }, pipeline.Checks[1].Options.Keys.Order().ToArray());
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Valid_custom_sql_check_loads()
    {
        var dir = WriteCheckProject("""
              - custom_sql:
                  name: no_negatives
                  sql: select * from staging.p where id < 0
            """);
        try
        {
            var project = ProjectLoader.Load(dir, Env);
            var check = Assert.Single(Assert.Single(project.Pipelines).Checks);
            Assert.Equal("custom_sql", check.Type);
            Assert.Empty(check.Columns);
            Assert.Equal("no_negatives", check.Options["name"]);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Theory]
    [InlineData("  - bogus: [id]\n", "unknown check type 'bogus'")]
    [InlineData("  - not_null: []\n", "declares no columns")]
    [InlineData("  - unique: []\n", "declares no columns")]
    [InlineData("  - row_count: { }\n", "at least one of min/max")]
    [InlineData("  - row_count: { min: 10, max: 2 }\n", "exceeds max")]
    [InlineData("  - row_count: { min: abc }\n", "must be an integer")]
    [InlineData("  - freshness: { max_age: 24h }\n", "exactly one 'column'")]
    [InlineData("  - freshness: { column: u }\n", "must be a positive duration")]
    [InlineData("  - freshness: { column: u, max_age: nope }\n", "must be a positive duration")]
    [InlineData("  - freshness: { column: u, max_age: 0s }\n", "must be a positive duration")]
    [InlineData("  - accepted_values: { values: [a] }\n", "exactly one 'column'")]
    [InlineData("  - accepted_values: { column: s }\n", "must be a non-empty list")]
    [InlineData("  - accepted_values: { column: s, values: [] }\n", "must be a non-empty list")]
    [InlineData("  - custom_sql: { sql: select 1 }\n", "requires 'name'")]
    [InlineData("  - custom_sql: { name: Bad-Name, sql: select 1 }\n", "requires 'name'")]
    [InlineData("  - custom_sql: { name: ok }\n", "non-empty 'sql'")]
    [InlineData("  - not_null: { columns: [id], stray: 1 }\n", "unknown option 'stray'")]
    [InlineData("  - freshness: { column: u, max_age: 1h, stray: 1 }\n", "unknown option 'stray'")]
    [InlineData("  - freshness: { column: [a, b], max_age: 24h }\n", "must be a single column name")]
    [InlineData("  - freshness: { column: \"\", max_age: 24h }\n", "must be a single column name")]
    [InlineData("  - custom_sql: { name: ok, sql: select 1, columns: [id] }\n", "does not take columns")]
    public void Invalid_check_is_PZ0113(string checksYaml, string messageFragment)
    {
        var dir = WriteCheckProject(checksYaml);
        try
        {
            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            var error = Assert.Single(ex.Errors, e => e.Code == "PZ0113");
            Assert.Contains(messageFragment, error.Message);
            Assert.Contains("pipeline 'p'", error.Message);
            Assert.Equal("pipelines/configs/p.yml", error.File);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>A check entry that isn't a single-key mapping must fail
    /// loudly at compile time instead of silently vanishing from the checks list.</summary>
    [Theory]
    [InlineData("  - freshness\n", "must be a mapping")]
    [InlineData("  - {}\n", "must be a mapping")]
    public void Malformed_check_entry_shape_is_PZ0113(string checksYaml, string messageFragment)
    {
        var dir = WriteCheckProject(checksYaml);
        try
        {
            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            var error = Assert.Single(ex.Errors, e => e.Code == "PZ0113");
            Assert.Contains(messageFragment, error.Message);
            Assert.Contains("pipeline 'p'", error.Message);
            Assert.Equal("pipelines/configs/p.yml", error.File);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>A two-key mapping (typically an indentation mistake —
    /// two checks merged into one list item) must be refused rather than silently keeping only the
    /// first key via <c>checkDict.First()</c>.</summary>
    [Fact]
    public void Two_key_check_entry_is_PZ0113()
    {
        var dir = WriteCheckProject("  - not_null: [id]\n    unique: [id]\n");
        try
        {
            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            var error = Assert.Single(ex.Errors, e => e.Code == "PZ0113");
            Assert.Contains("one check per list item", error.Message);
            Assert.Contains("not_null", error.Message);
            Assert.Contains("unique", error.Message);
            Assert.Contains("pipeline 'p'", error.Message);
            Assert.Equal("pipelines/configs/p.yml", error.File);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Duplicate_custom_sql_name_is_PZ0113()
    {
        var dir = WriteCheckProject("""
              - custom_sql: { name: same, sql: select 1 }
              - custom_sql: { name: same, sql: select 2 }
            """);
        try
        {
            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            var error = Assert.Single(ex.Errors, e => e.Code == "PZ0113");
            Assert.Contains("duplicate custom_sql name 'same'", error.Message);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>All invalid checks in a sidecar are reported together — aggregate, never
    /// fail-one-at-a-time (the house validation rule).</summary>
    [Fact]
    public void Check_errors_aggregate()
    {
        var dir = WriteCheckProject("""
              - bogus: [id]
              - freshness: { column: u, max_age: nope }
              - custom_sql: { sql: select 1 }
            """);
        try
        {
            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            Assert.Equal(3, ex.Errors.Count(e => e.Code == "PZ0113"));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>`column` and `columns` on the same check would silently shadow each other — refuse.</summary>
    [Fact]
    public void Column_and_columns_together_is_PZ0113()
    {
        var dir = WriteCheckProject("  - not_null: { column: id, columns: [id] }\n");
        try
        {
            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            var error = Assert.Single(ex.Errors, e => e.Code == "PZ0113");
            Assert.Contains("both 'column' and 'columns'", error.Message);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    // --- project.yml `retention:` ---

    /// <summary>Sibling of <see cref="WriteProject"/> for the tests that need to vary project.yml itself
    /// rather than connections.yml. The sink-only connections.yml keeps ConnectionsLoader quiet so the
    /// only errors these tests can see are the ones they are about.</summary>
    private static string WriteRetentionProject(string retentionBlock)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "project.yml"),
            $"name: retention_test\nversion: 0.1.0\n{retentionBlock}");
        File.WriteAllText(Path.Combine(dir, "connections.yml"), "lake:\n  connector: localfiles\n  root: /out\n");
        return dir;
    }

    [Fact]
    public void Retention_absent_defaults_to_keep_last_10()
    {
        var dir = WriteRetentionProject("");
        try
        {
            var project = ProjectLoader.Load(dir, Env);
            Assert.NotNull(project.Retention);
            Assert.Equal(10, project.Retention!.KeepLast);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Retention_map_takes_its_keep_last()
    {
        var dir = WriteRetentionProject("retention:\n  keep_last: 25\n");
        try
        {
            Assert.Equal(25, ProjectLoader.Load(dir, Env).Retention!.KeepLast);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    // The off-set spans both YAML shapes: "false" reaches the loader as a real bool (YamlMapper.ConvertScalar
    // maps exactly "true"/"false"), while "off"/"no"/"Off"/"FALSE" reach it as strings. They must be
    // indistinguishable here -- this is the test that fails if someone reaches for TryGetBool.
    [Theory]
    [InlineData("off")]
    [InlineData("Off")]
    [InlineData("OFF")]
    [InlineData("no")]
    [InlineData("false")]
    [InlineData("FALSE")]
    public void Retention_off_disables_it(string spelling)
    {
        var dir = WriteRetentionProject($"retention: {spelling}\n");
        try
        {
            Assert.Null(ProjectLoader.Load(dir, Env).Retention);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Theory]
    [InlineData("retention:\n  keep_last: 0\n")]
    [InlineData("retention:\n  keep_last: -1\n")]
    [InlineData("retention:\n  keep_last: nope\n")]
    [InlineData("retention:\n  nothing: 3\n")]
    [InlineData("retention: on\n")]
    [InlineData("retention: maybe\n")]
    public void Retention_invalid_is_PZ0123(string retentionBlock)
    {
        var dir = WriteRetentionProject(retentionBlock);
        try
        {
            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            Assert.Single(ex.Errors, e => e.Code == PzErrorCode.RetentionConfigInvalid);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    // Validation aggregates: a retention error must not short-circuit an unrelated one in the same file.
    [Fact]
    public void Retention_error_aggregates_with_an_unrelated_project_error()
    {
        var dir = WriteRetentionProject("engine:\n  threads: 0\n\nretention:\n  keep_last: 0\n");
        try
        {
            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.RetentionConfigInvalid);
            Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.InvalidEngineConfig);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    // --- project.yml `on_source_drift:` ---

    /// <summary>Sibling of <see cref="WriteRetentionProject"/> for the drift-policy tests -- same
    /// sink-only connections.yml so the only errors these tests can see are the ones they are about.</summary>
    private static string WriteDriftPolicyProject(string driftBlock)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-loader-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "project.yml"),
            $"name: drift_policy_test\nversion: 0.1.0\n{driftBlock}");
        File.WriteAllText(Path.Combine(dir, "connections.yml"), "lake:\n  connector: localfiles\n  root: /out\n");
        return dir;
    }

    [Fact]
    public void OnSourceDrift_absent_defaults_to_ignore()
    {
        var dir = WriteDriftPolicyProject("");
        try
        {
            var project = ProjectLoader.Load(dir, Env);
            Assert.Equal(DriftPolicy.Ignore, project.OnSourceDrift);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Theory]
    [InlineData("warn", DriftPolicy.Warn)]
    [InlineData("fail", DriftPolicy.Fail)]
    [InlineData("ignore", DriftPolicy.Ignore)]
    public void OnSourceDrift_parses_each_known_value(string spelling, DriftPolicy expected)
    {
        var dir = WriteDriftPolicyProject($"on_source_drift: {spelling}\n");
        try
        {
            var project = ProjectLoader.Load(dir, Env);
            Assert.Equal(expected, project.OnSourceDrift);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void OnSourceDrift_invalid_is_PZ0126_and_defaults_to_ignore()
    {
        var dir = WriteDriftPolicyProject("on_source_drift: banana\n");
        try
        {
            var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env));
            var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.DriftPolicyInvalid);
            Assert.Equal("project.yml", error.File);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
