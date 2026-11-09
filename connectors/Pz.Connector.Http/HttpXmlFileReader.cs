using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json.Nodes;
using System.Xml;
using System.Xml.Linq;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connector.Http;

/// <summary>Reads an XML file by streaming: a record is an element whose chain of local names from the
/// root equals `records`, and only that branch is descended — every other subtree is skipped without
/// being loaded. A root element other than the first `records` segment is an error, not zero rows.
/// Each record is loaded alone as an <see cref="XElement"/>. A DTD is refused and nothing external is
/// ever resolved. The declared encoding is honoured unless `encoding` overrides it. Namespaces are
/// ignored: elements and attributes match by local name, and namespace declarations are never data.
///
/// Contract mode: a column is the direct child element of that name, else the attribute of that
/// name, parsed by <see cref="TextValueParser"/> (empty is NULL); absent is NULL; a child with child
/// elements of its own is an error. Raw mode: the record becomes a JSON object of `"@attr"` values,
/// children by local name (repeated names as an array; text-only children as strings, empty as "";
/// nested children as objects), and its own non-whitespace text as `"#text"`.</summary>
internal sealed class HttpXmlFileReader : IFileFormatReader
{
    /// <summary>The for_each item this file was downloaded for; its fields land on every row.</summary>
    public ItemValues? Item { get; init; }

    public IAsyncEnumerable<RecordBatch> ReadAsync(FileSource file, HttpDatasetConfig config,
        DateTimeOffset fetchedAt, BatchOptions options, string label, CancellationToken ct) =>
        FileReaderErrors.WrapAsync(ReadCoreAsync(file, config, Item, fetchedAt, options, label, ct), label, file.Name, ct);

    private static async IAsyncEnumerable<RecordBatch> ReadCoreAsync(FileSource file, HttpDatasetConfig config,
        ItemValues? itemValues, DateTimeOffset fetchedAt, BatchOptions options, string label,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var item = ItemCells.For(config, itemValues, label);
        var settings = new XmlReaderSettings
        {
            DtdProcessing = DtdProcessing.Prohibit,
            XmlResolver = null,
            IgnoreComments = true,
            Async = true,
        };
        using var text = config.File!.Encoding is { } encoding
            ? new StreamReader(file.Stream, encoding, detectEncodingFromByteOrderMarks: true, leaveOpen: true)
            : null;
        // Over a raw stream CloseInput defaults to false, so the caller-owned stream stays open.
        using var reader = text is null ? XmlReader.Create(file.Stream, settings) : XmlReader.Create(text, settings);

        var builder = new ArrowBatchBuilder(FileOutput.Schema(config), options.TargetBatchBytes,
            maxRowsPerBatch: options.MaxRowsPerBatch);
        long row = 0;
        await foreach (var record in RecordsAsync(reader, config.File.RecordPath!, $"{label}: file '{file.Name}'", ct).ConfigureAwait(false))
        {
            row++;
            var context = $"{label}: file '{file.Name}' record {row}";
            builder.AppendRow(config.IsContractMode
                ? ContractRow(record, config, item, file.Name, context)
                : FileOutput.RawRow(RecordObject(record), file.Name, row, fetchedAt, config, context, item));
            if (builder.TryTakeBatch(out var batch))
            {
                yield return batch!;
            }
        }

        if (builder.Flush() is { } tail)
        {
            yield return tail;
        }
    }

    /// <summary>Descends only into elements on <paramref name="path"/>, so every element seen at depth d
    /// already has the path's first d names as its ancestors.</summary>
    private static async IAsyncEnumerable<XElement> RecordsAsync(XmlReader reader, IReadOnlyList<string> path,
        string context, [EnumeratorCancellation] CancellationToken ct)
    {
        if (!await reader.ReadAsync().ConfigureAwait(false))
        {
            yield break;
        }

        while (reader.ReadState == ReadState.Interactive)
        {
            ct.ThrowIfCancellationRequested();
            if (reader.NodeType != XmlNodeType.Element)
            {
                await reader.ReadAsync().ConfigureAwait(false);
                continue;
            }

            var depth = reader.Depth;
            if (depth == 0 && reader.LocalName != path[0])
            {
                // A wrong `records` would otherwise land a green, empty read.
                throw new PzConnectorException(
                    $"{context}: root element is <{reader.LocalName}>, but records starts with /{path[0]}",
                    isTransient: false);
            }

            if (depth >= path.Count || reader.LocalName != path[depth])
            {
                await reader.SkipAsync().ConfigureAwait(false);
                continue;
            }

            if (depth == path.Count - 1)
            {
                // Leaves the reader on the node after the record's end tag.
                yield return (XElement)await XNode.ReadFromAsync(reader, ct).ConfigureAwait(false);
                continue;
            }

            await reader.ReadAsync().ConfigureAwait(false);
        }
    }

    private static object?[] ContractRow(XElement record, HttpDatasetConfig config, ItemCells? item,
        string fileName, string context)
    {
        var columns = item?.RecordColumns ?? config.Columns!;
        var extra = config.File!.FileColumn is null ? 0 : 1;
        var values = new object?[columns.Count + extra];
        var i = 0;
        foreach (var (name, typeName) in columns)
        {
            string? text;
            if (record.Elements().FirstOrDefault(e => e.Name.LocalName == name) is { } child)
            {
                if (child.HasElements)
                {
                    throw new PzConnectorException(
                        $"{context}: column '{name}' is an element with child elements; contract columns take " +
                        "text values only — use raw mode (no 'columns') to land nested XML", isTransient: false);
                }

                text = child.Value;
            }
            else
            {
                text = Attributes(record).FirstOrDefault(a => a.Name.LocalName == name)?.Value;
            }

            values[i++] = text is null ? null : TextValueParser.Parse(text, typeName, $"{context}: column '{name}'");
        }

        if (item is not null)
        {
            values = item.Place(values, extra);
        }

        if (extra > 0)
        {
            values[^1] = fileName;
        }

        return values;
    }

    /// <summary>The record itself is always an object, even when it carries only text.</summary>
    private static JsonObject RecordObject(XElement element)
    {
        var obj = new JsonObject();
        foreach (var attribute in Attributes(element))
        {
            obj["@" + attribute.Name.LocalName] = attribute.Value;
        }

        foreach (var group in element.Elements().GroupBy(e => e.Name.LocalName))
        {
            var items = group.ToList();
            obj[group.Key] = items.Count == 1
                ? ElementValue(items[0])
                : new JsonArray([.. items.Select(ElementValue)]);
        }

        var text = string.Concat(element.Nodes().OfType<XText>().Select(t => t.Value)).Trim();
        if (text.Length > 0)
        {
            obj["#text"] = text;
        }

        return obj;
    }

    /// <summary>A text-only element (no attributes, no children) is its text; anything else an object.</summary>
    private static JsonNode ElementValue(XElement element) =>
        element.HasElements || Attributes(element).Any()
            ? RecordObject(element)
            : JsonValue.Create(element.Value);

    private static IEnumerable<XAttribute> Attributes(XElement element) =>
        element.Attributes().Where(a => !a.IsNamespaceDeclaration);
}
