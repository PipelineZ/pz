using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connectors.Toolkit.Tests.Formats;

public sealed class CsvWriteCodecDelimiterTests
{
    private static RecordBatch Batch()
    {
        var schema = new Schema([
            new Field("id", Int64Type.Default, true),
            new Field("name", StringType.Default, true),
        ], null);
        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var names = new StringArray.Builder().Append("a\tb").Append("plain,comma").Build();
        return new RecordBatch(schema, [ids, names], 2);
    }

    [Fact]
    public async Task Tab_delimiter_separates_fields_and_quotes_only_tab_bearing_values()
    {
        using var batch = Batch();
        using var ms = new MemoryStream();
        await using (var codec = new CsvWriteCodec(ms, batch.Schema, "test", leaveOpen: true, delimiter: '\t'))
        {
            await codec.WriteBatchAsync(batch, CancellationToken.None);
            await codec.FlushAsync(CancellationToken.None);
        }

        Assert.Equal("id\tname\n1\t\"a\tb\"\n2\tplain,comma\n", Encoding.UTF8.GetString(ms.ToArray()));
    }

    [Fact]
    public async Task Default_delimiter_is_unchanged()
    {
        using var batch = Batch();
        using var ms = new MemoryStream();
        await using (var codec = new CsvWriteCodec(ms, batch.Schema, "test", leaveOpen: true))
        {
            await codec.WriteBatchAsync(batch, CancellationToken.None);
            await codec.FlushAsync(CancellationToken.None);
        }

        Assert.Equal("id,name\n1,a\tb\n2,\"plain,comma\"\n", Encoding.UTF8.GetString(ms.ToArray()));
    }

    [Theory]
    [InlineData('é')]
    [InlineData('"')]
    [InlineData('\n')]
    [InlineData('\r')]
    public void Non_ascii_delimiter_is_rejected(char delimiter)
    {
        using var batch = Batch();
        Assert.Throws<ArgumentOutOfRangeException>(() => new CsvWriteCodec(new MemoryStream(), batch.Schema, "test", delimiter: delimiter));
    }
}
