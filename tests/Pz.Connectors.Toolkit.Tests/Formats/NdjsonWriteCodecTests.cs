using Apache.Arrow;
using Pz.Connectors.Abstractions.Formats;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connectors.Toolkit.Tests.Formats;

public class NdjsonWriteCodecTests
{
    [Fact]
    public async Task Output_is_byte_identical_to_abstractions_codec()
    {
        var schema = new Schema([
            new Field("id", Apache.Arrow.Types.Int64Type.Default, nullable: true),
            new Field("name", Apache.Arrow.Types.StringType.Default, nullable: true)], null);
        var builder = new Pz.Connectors.Abstractions.Batches.ArrowBatchBuilder(schema);
        builder.AppendRow([1L, "a"]);
        builder.AppendRow([2L, null]);
        using var batch = builder.Flush()!;

        using var expected = new MemoryStream();
        await NdjsonCodec.WriteAsync(batch, expected, CancellationToken.None);
        using var batch2 = batch.Clone();
        using var actual = new MemoryStream();
        await NdjsonWriteCodec.WriteAsync(batch2, actual, CancellationToken.None);

        Assert.Equal(expected.ToArray(), actual.ToArray());
    }
}
