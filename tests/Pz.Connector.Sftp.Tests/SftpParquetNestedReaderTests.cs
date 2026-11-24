using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Formats;
using Pz.DuckDb;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

/// <summary>sftp reads a DuckDB-written parquet file's lists, structs, maps, fixed-size arrays and lists of lists
/// back with every null in place: a null list apart from an empty one, a null struct apart from a struct of nulls,
/// a null map value, and DuckDB's null fixed-size array (written as one entry per slot).</summary>
public sealed class SftpParquetNestedReaderTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-sftp-nested-parquet", Guid.NewGuid().ToString("N"));

    private string File => Path.Combine(_dir, "nested.parquet");

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        await using var duck = DuckSession.Open(Path.Combine(_dir, "probe.duckdb"));
        await duck.ExecuteAsync($"""
            copy (select * from (values
                (1, [1, null], {"{"}'name': 'a', 'tags': ['t']{"}"}, map {"{"}'k': 1, 'z': null{"}"}, [1, 2]::int[2], [[1], []]),
                (2, [], {"{"}'name': null, 'tags': []{"}"}, map {"{"}{"}"}, null, []),
                (3, null, null, null, [3, null]::int[2], null)
            ) t(id, xs, rec, m, arr, nn)) to '{File}' (format parquet)
            """);
    }

    public Task DisposeAsync()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
        return Task.CompletedTask;
    }

    [Fact]
    public async Task Nested_columns_read_back_with_every_null_in_place()
    {
        await using var stream = System.IO.File.OpenRead(File);
        var schema = await SftpParquetReader.ReadSchemaAsync(stream, "sftp:nested.parquet", CancellationToken.None);
        Assert.IsType<ListType>(schema.GetFieldByName("xs").DataType);
        Assert.IsType<StructType>(schema.GetFieldByName("rec").DataType);
        Assert.IsType<MapType>(schema.GetFieldByName("m").DataType);

        stream.Position = 0;
        var output = new MemoryStream();
        await foreach (var batch in SftpParquetReader.ReadAsync(stream, null, "sftp:nested.parquet", BatchOptions.Default, CancellationToken.None))
        {
            using (batch)
            {
                await NdjsonCodec.WriteAsync(batch, output, CancellationToken.None);
            }
        }

        Assert.Equal(
            "{\"id\":1,\"xs\":[1,null],\"rec\":{\"name\":\"a\",\"tags\":[\"t\"]},\"m\":{\"k\":1,\"z\":null},\"arr\":[1,2],\"nn\":[[1],[]]}\n" +
            "{\"id\":2,\"xs\":[],\"rec\":{\"name\":null,\"tags\":[]},\"m\":{},\"arr\":null,\"nn\":[]}\n" +
            "{\"id\":3,\"xs\":null,\"rec\":null,\"m\":null,\"arr\":[3,null],\"nn\":null}\n",
            Encoding.UTF8.GetString(output.ToArray()));
    }

    [Fact]
    public async Task A_projection_picks_nested_columns_by_name_in_the_requested_order()
    {
        await using var stream = System.IO.File.OpenRead(File);
        var batches = new List<RecordBatch>();
        await foreach (var batch in SftpParquetReader.ReadAsync(stream, ["nn", "id"], "sftp:nested.parquet", BatchOptions.Default, CancellationToken.None))
        {
            batches.Add(batch);
        }

        using var only = Assert.Single(batches);
        Assert.Equal(["nn", "id"], only.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(3, only.Length);
    }
}
