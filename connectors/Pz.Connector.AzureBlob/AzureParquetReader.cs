using Apache.Arrow;
using Parquet;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.AzureBlob;

/// <summary>Parquet footer → Arrow schema for <see cref="AzureSource.GetSchemaAsync"/>'s peek (the same
/// parquet-field→v0-typename→Arrow-field pipeline LocalFiles' <c>ParquetSource.GetSchemaAsync</c> uses).
/// Row reading has no .NET-side counterpart here: azure reads execute on the native tier only.</summary>
internal static class AzureParquetReader
{
    /// <summary>Reads the parquet footer and maps it to an Arrow schema via the same
    /// parquet-field→v0-typename→Arrow-field pipeline LocalFiles' <c>ParquetSource.GetSchemaAsync</c> uses.
    /// Leaves <paramref name="blob"/> open and does not consume its position beyond footer parsing.</summary>
    public static Schema ReadSchema(Stream blob)
    {
        var reader = ParquetReader.CreateAsync(blob, leaveStreamOpen: true).GetAwaiter().GetResult();
        try
        {
            var fields = reader.Schema.GetDataFields()
                .Select(f => AzureTypeNameMap.ToArrowField(f.Name, AzureParquetTypeMap.ToV0TypeName(f)))
                .ToArray();
            return new Schema(fields, null);
        }
        finally
        {
            reader.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }
}
