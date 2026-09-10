using System.Diagnostics;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

public sealed class DataPlaneSchemaTests
{
    private static readonly Schema TwoColumns = new Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("name", StringType.Default, true))
        .Build();

    /// <summary>Yields the given batches verbatim.</summary>
    private sealed class FixedPartition(IReadOnlyList<RecordBatch> batches) : IDatasetPartition
    {
        public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
        {
            foreach (var batch in batches)
            {
                yield return batch;
            }

            await Task.CompletedTask;
        }
    }

    private static RecordBatch Batch(Schema schema, params IArrowArray[] arrays) => new(schema, arrays, arrays[0].Length);

    private static ReadTicket Ticket(Schema schema, IDatasetPartition partition) =>
        new(schema, partition, BatchOptions.Default, CancellationToken.None, new SyncStateCapture(), default);

    [Fact]
    public async Task A_batch_with_fewer_columns_than_the_stream_schema_is_refused_before_it_is_written()
    {
        using var source = new ActivitySource("test");
        using var stream = new MemoryStream();
        var pruned = Batch(
            new Schema.Builder().Field(new Field("name", StringType.Default, true)).Build(),
            new StringArray.Builder().Append("a").Build());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            DataPlaneListener.ServeReadAsync(stream, Ticket(TwoColumns, new FixedPartition([pruned])), source, CancellationToken.None));

        Assert.Contains("id:int64, name:utf8", ex.Message);
        Assert.Contains("[name:utf8]", ex.Message);
        Assert.Contains("2 column(s)", ex.Message);
        Assert.Contains("1 column(s)", ex.Message);

        // Only the schema header reached the stream: no batch was written under the wrong header.
        stream.Position = 0;
        using var reader = new ArrowStreamReader(stream);
        Assert.Null(await reader.ReadNextRecordBatchAsync());
    }

    [Fact]
    public async Task A_batch_with_more_columns_than_the_stream_schema_is_refused_before_it_is_written()
    {
        using var source = new ActivitySource("test");
        using var stream = new MemoryStream();
        var extra = Batch(
            new Schema.Builder()
                .Field(new Field("id", Int64Type.Default, false))
                .Field(new Field("name", StringType.Default, true))
                .Field(new Field("extra", BooleanType.Default, true))
                .Build(),
            new Int64Array.Builder().Append(1).Build(),
            new StringArray.Builder().Append("a").Build(),
            new BooleanArray.Builder().Append(true).Build());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            DataPlaneListener.ServeReadAsync(stream, Ticket(TwoColumns, new FixedPartition([extra])), source, CancellationToken.None));

        Assert.Contains("2 column(s)", ex.Message);
        Assert.Contains("3 column(s)", ex.Message);

        // Only the schema header reached the stream: no batch was written under the wrong header.
        stream.Position = 0;
        using var reader = new ArrowStreamReader(stream);
        Assert.Null(await reader.ReadNextRecordBatchAsync());
    }

    [Fact]
    public async Task A_batch_whose_column_type_differs_is_refused()
    {
        using var source = new ActivitySource("test");
        using var stream = new MemoryStream();
        var wrongType = Batch(
            new Schema.Builder()
                .Field(new Field("id", StringType.Default, false))
                .Field(new Field("name", StringType.Default, true))
                .Build(),
            new StringArray.Builder().Append("1").Build(),
            new StringArray.Builder().Append("a").Build());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            DataPlaneListener.ServeReadAsync(stream, Ticket(TwoColumns, new FixedPartition([wrongType])), source, CancellationToken.None));

        Assert.Contains("id:utf8", ex.Message);
    }

    [Fact]
    public async Task Nullability_and_metadata_differences_are_not_a_mismatch()
    {
        using var source = new ActivitySource("test");
        using var stream = new MemoryStream();
        var relaxed = new Schema(
            [new Field("id", Int64Type.Default, true), new Field("name", StringType.Default, false)],
            new Dictionary<string, string> { ["origin"] = "test" });
        var batch = Batch(relaxed,
            new Int64Array.Builder().Append(7).Build(),
            new StringArray.Builder().Append("seven").Build());

        await DataPlaneListener.ServeReadAsync(stream, Ticket(TwoColumns, new FixedPartition([batch])), source, CancellationToken.None);

        stream.Position = 0;
        using var reader = new ArrowStreamReader(stream);
        var read = await reader.ReadNextRecordBatchAsync();
        Assert.NotNull(read);
        Assert.Equal(1, read.Length);
        Assert.Equal(7L, ((Int64Array)read.Column(0)).GetValue(0));
        Assert.Null(await reader.ReadNextRecordBatchAsync());
    }
}
