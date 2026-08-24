using Google.Protobuf.Reflection;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;

namespace Pz.Connectors.Protocol.Tests;

/// <summary>Adding a field to a boundary-crossing record without adding it to the PCP message
/// silently drops it for process-hosted connectors. This pin makes the omission a test failure,
/// in the spirit of EventsDocReflectionTests.</summary>
public class RecordMessageParityTests
{
    private static readonly (Type Record, MessageDescriptor Message, string[] ProtoOnly)[] Pairs =
    [
        (typeof(DatasetSpec), DatasetSpecMsg.Descriptor, []),
        (typeof(OutputSpec), OutputSpecMsg.Descriptor, ["max_text_lengths_set"]),
        (typeof(WriteAttempt), WriteAttemptMsg.Descriptor, []),
        (typeof(ReadHints), ReadHintsMsg.Descriptor, ["columns_set"]),
        (typeof(BatchOptions), BatchOptionsMsg.Descriptor, []),
        (typeof(ValidationResult), ValidationResultMsg.Descriptor, []),
        (typeof(ConnectionCheck), ConnectionCheckMsg.Descriptor, []),
        (typeof(ConnectorInfo), ConnectorInfoMsg.Descriptor, []),
        (typeof(NativeScan), NativeScanResponse.Descriptor, ["found"]),
        (typeof(NativeCopy), NativeCopyResponse.Descriptor, ["found"]),
        (typeof(FileMove), FileMoveMsg.Descriptor, []),
        (typeof(WriteResult), WriteResultMsg.Descriptor, []),
    ];

    public static IEnumerable<object[]> PairData() => Pairs.Select(p => new object[] { p.Record, p.Message, p.ProtoOnly });

    [Theory]
    [MemberData(nameof(PairData))]
    public void Every_record_property_has_a_message_field(Type record, MessageDescriptor message, string[] protoOnly)
    {
        var recordFields = record.GetProperties()
            // Constructor params + init-only props only (per this test's own contract): both compile
            // to a property with a set accessor. A get-only computed property — e.g.
            // ValidationResult.IsValid, derived from Errors and carrying no independent wire data —
            // has none, and EqualityContract (every record) has none either.
            .Where(p => p.Name != "EqualityContract" && p.SetMethod is not null)
            .Select(p => ToSnakeCase(p.Name))
            .ToHashSet();
        var messageFields = message.Fields.InDeclarationOrder().Select(f => f.Name).ToHashSet();

        var missingInProto = recordFields.Except(messageFields)
            // DatasetSchema wraps Apache.Arrow.Schema; schemas cross as IPC bytes by spec rule.
            .Where(f => f != "schema").ToArray();
        var unexplainedInProto = messageFields.Except(recordFields).Except(protoOnly)
            .Where(f => f != "arrow_schema_ipc").ToArray();

        Assert.True(missingInProto.Length == 0,
            $"{record.Name} properties missing from {message.Name}: {string.Join(", ", missingInProto)} — add them to pz_connector.proto in this PR");
        Assert.True(unexplainedInProto.Length == 0,
            $"{message.Name} fields with no {record.Name} peer and no allowlist entry: {string.Join(", ", unexplainedInProto)}");
    }

    private static string ToSnakeCase(string pascal) =>
        string.Concat(pascal.Select((c, i) => char.IsUpper(c) && i > 0 ? "_" + char.ToLowerInvariant(c) : char.ToLowerInvariant(c).ToString()));
}
