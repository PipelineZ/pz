using System.Data.SqlTypes;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connectors.Toolkit.Tests.Formats;

public sealed class CsvWriteCodecDecimalTests
{
    [Fact]
    public async Task Decimal128_beyond_system_decimal_is_written_exactly()
    {
        var schema = new Schema([new Field("d", new Decimal128Type(38, 9), true)], null);
        var values = new Decimal128Array.Builder(new Decimal128Type(38, 9))
            .Append(SqlDecimal.Parse("12345678901234567890123456789.123456789"))
            .Append(SqlDecimal.Parse("1.500000000"))
            .AppendNull()
            .Build();
        using var batch = new RecordBatch(schema, [values], 3);
        using var ms = new MemoryStream();
        await using (var codec = new CsvWriteCodec(ms, batch.Schema, "test", leaveOpen: true))
        {
            await codec.WriteBatchAsync(batch, CancellationToken.None);
            await codec.FlushAsync(CancellationToken.None);
        }

        Assert.Equal("d\n12345678901234567890123456789.123456789\n1.500000000\n\n", Encoding.UTF8.GetString(ms.ToArray()));
    }
}
