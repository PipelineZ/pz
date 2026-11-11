using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;
using Pz.Shared;

namespace Pz.PackageManagement.ProcessHosting;

/// <summary>Bidirectional converters between the ABI's boundary records and their PCP wire messages.
/// The host is always the RPC CLIENT here (it sends specs/options, receives results/errors back), so
/// only one direction of each pair is exercised on the production path -- both directions exist and are
/// implemented anyway so a round trip through the wire shape is provably lossless (see ShimTests), and
/// so this class is the single place that shape is defined rather than split across callers.
///
/// <para>Mirrors <c>tests/fixtures/PcpFakeConnector/PcpService.cs</c>'s <c>SpecMapping</c> exactly --
/// that fixture is the reference peer this shim talks to, and any drift between the two would only
/// ever show up as a wire-level test failure, never a compile error. The <c>Struct</c> decoding is
/// <c>Pz.Shared.StructMapping</c>, compiled into this assembly and the connector SDK alike.</para></summary>
public static class MessageMapping
{
    // ---- google.protobuf.Struct <-> IReadOnlyDictionary<string, object?> ---------------------

    public static Struct ToStruct(IReadOnlyDictionary<string, object?> values)
    {
        var result = new Struct();
        foreach (var (key, value) in values)
        {
            result.Fields[key] = ToValue(value);
        }

        return result;
    }

    public static IReadOnlyDictionary<string, object?> ToDictionary(Struct? value) => StructMapping.ToDictionary(value);

    private static Value ToValue(object? value) => value switch
    {
        null => Value.ForNull(),
        bool b => Value.ForBool(b),
        string s => Value.ForString(s),
        IReadOnlyDictionary<string, object?> map => Value.ForStruct(ToStruct(map)),
        IReadOnlyDictionary<string, string> strings => Value.ForStruct(
            ToStruct(strings.ToDictionary(pair => pair.Key, object? (pair) => pair.Value, StringComparer.Ordinal))),
        sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal =>
            Value.ForNumber(Convert.ToDouble(value, CultureInfo.InvariantCulture)),
        // Non-generic IEnumerable, not IEnumerable<object?>: generic variance covers reference types
        // only, so an int[]/List<int> option (an int is a value type) does not match
        // IEnumerable<object?> and fell through to the string catch-all below -- value.ToString() on a
        // List<int> is .NET's default collection rendering ("System.Collections.Generic.List`1[...]"),
        // never the list of numbers a connector reading it would expect. A dictionary is matched above
        // this arm, so nothing here double-handles the two map cases.
        System.Collections.IEnumerable list => Value.ForList([.. list.Cast<object?>().Select(ToValue)]),
        _ => Value.ForString(value.ToString() ?? string.Empty),
    };

    // ---- DatasetSpec ---------------------------------------------------------------------------

    public static DatasetSpecMsg ToDatasetSpecMsg(DatasetSpec spec)
    {
        var msg = new DatasetSpecMsg
        {
            Source = spec.Source,
            Dataset = spec.Dataset,
            Options = ToStruct(spec.Options),
            WatermarkLowerInclusive = spec.WatermarkLowerInclusive,
            ChangeCapture = spec.ChangeCapture,
        };
        if (spec.WatermarkCursor is not null)
        {
            msg.WatermarkCursor = spec.WatermarkCursor;
        }

        if (spec.WatermarkValue is not null)
        {
            msg.WatermarkValue = spec.WatermarkValue;
        }

        if (spec.WatermarkUpperBound is not null)
        {
            msg.WatermarkUpperBound = spec.WatermarkUpperBound;
        }

        if (spec.PriorSyncState is not null)
        {
            msg.PriorSyncState = spec.PriorSyncState;
        }

        if (spec.ChangeCaptureSlot is not null)
        {
            msg.ChangeCaptureSlot = spec.ChangeCaptureSlot;
        }

        return msg;
    }

    public static DatasetSpec ToDatasetSpec(DatasetSpecMsg msg) =>
        new(msg.Source, msg.Dataset, ToDictionary(msg.Options))
        {
            WatermarkCursor = msg.HasWatermarkCursor ? msg.WatermarkCursor : null,
            WatermarkValue = msg.HasWatermarkValue ? msg.WatermarkValue : null,
            WatermarkUpperBound = msg.HasWatermarkUpperBound ? msg.WatermarkUpperBound : null,
            WatermarkLowerInclusive = msg.WatermarkLowerInclusive,
            PriorSyncState = msg.HasPriorSyncState ? msg.PriorSyncState : null,
            ChangeCapture = msg.ChangeCapture,
            ChangeCaptureSlot = msg.HasChangeCaptureSlot ? msg.ChangeCaptureSlot : null,
        };

    // ---- ReadHints -------------------------------------------------------------------------------

    public static ReadHintsMsg ToReadHintsMsg(ReadHints hints)
    {
        var msg = new ReadHintsMsg { ColumnsSet = hints.Columns is not null };
        if (hints.Columns is not null)
        {
            msg.Columns.AddRange(hints.Columns);
        }

        if (hints.PredicateSql is not null)
        {
            msg.PredicateSql = hints.PredicateSql;
        }

        if (hints.Limit is not null)
        {
            msg.Limit = hints.Limit.Value;
        }

        return msg;
    }

    public static ReadHints ToReadHints(ReadHintsMsg? msg) => msg is null
        ? ReadHints.None
        : new ReadHints(
            msg.ColumnsSet ? msg.Columns.ToArray() : null,
            msg.HasPredicateSql ? msg.PredicateSql : null,
            msg.HasLimit ? msg.Limit : null);

    // ---- BatchOptions ----------------------------------------------------------------------------

    public static BatchOptionsMsg ToBatchOptionsMsg(BatchOptions options) => new()
    {
        TargetBatchBytes = options.TargetBatchBytes,
        MaxRowsPerBatch = options.MaxRowsPerBatch,
    };

    public static BatchOptions ToBatchOptions(BatchOptionsMsg? msg) => msg is null
        ? BatchOptions.Default
        : new BatchOptions(
            msg.TargetBatchBytes > 0 ? msg.TargetBatchBytes : BatchOptions.Default.TargetBatchBytes,
            msg.MaxRowsPerBatch > 0 ? msg.MaxRowsPerBatch : BatchOptions.Default.MaxRowsPerBatch);

    // ---- WriteAttempt ----------------------------------------------------------------------------

    public static WriteAttemptMsg ToWriteAttemptMsg(WriteAttempt attempt) =>
        new() { Node = attempt.Node, Run = attempt.Run, Ordinal = attempt.Ordinal };

    public static WriteAttempt ToWriteAttempt(WriteAttemptMsg msg) => new(msg.Node, msg.Run, msg.Ordinal);

    // ---- ConnectorRunContext ---------------------------------------------------------------------

    public static RunContextMsg ToRunContextMsg(ConnectorRunContext context)
    {
        var msg = new RunContextMsg { Run = context.Run };
        if (context.Node is not null)
        {
            msg.Node = context.Node;
        }

        if (context.StartedAt is { } startedAt)
        {
            msg.StartedAt = startedAt.ToString("O", System.Globalization.CultureInfo.InvariantCulture);
        }

        return msg;
    }

    // ---- OutputSpec ------------------------------------------------------------------------------

    public static OutputSpecMsg ToOutputSpecMsg(OutputSpec spec)
    {
        var msg = new OutputSpecMsg
        {
            Sink = spec.Sink,
            Output = spec.Output,
            Mode = spec.Mode,
            SchemaPolicy = spec.SchemaPolicy,
            Options = ToStruct(spec.Options),
            MaxTextLengthsSet = spec.MaxTextLengths is not null,
        };
        msg.Keys.AddRange(spec.Keys);
        if (spec.OnDelete is not null)
        {
            msg.OnDelete = spec.OnDelete;
        }

        if (spec.MaxTextLengths is not null)
        {
            foreach (var (key, value) in spec.MaxTextLengths)
            {
                msg.MaxTextLengths[key] = value;
            }
        }

        if (spec.Attempt is not null)
        {
            msg.Attempt = ToWriteAttemptMsg(spec.Attempt);
        }

        return msg;
    }

    public static OutputSpec ToOutputSpec(OutputSpecMsg msg) =>
        new(msg.Sink, msg.Output, msg.Mode, msg.SchemaPolicy, ToDictionary(msg.Options))
        {
            Keys = msg.Keys.ToArray(),
            OnDelete = msg.HasOnDelete ? msg.OnDelete : null,
            MaxTextLengths = msg.MaxTextLengthsSet
                ? msg.MaxTextLengths.ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal)
                : null,
            Attempt = msg.Attempt is { } attempt ? ToWriteAttempt(attempt) : null,
        };

    // ---- ConnectorInfo ---------------------------------------------------------------------------

    public static ConnectorInfoMsg ToConnectorInfoMsg(ConnectorInfo info) =>
        new() { Name = info.Name, Version = info.Version, ProtocolMajor = info.ProtocolMajor };

    public static ConnectorInfo ToConnectorInfo(ConnectorInfoMsg msg) => new(msg.Name, msg.Version, msg.ProtocolMajor);

    // ---- ValidationResult ------------------------------------------------------------------------

    public static ValidationResultMsg ToValidationResultMsg(ValidationResult result)
    {
        var msg = new ValidationResultMsg();
        msg.Errors.AddRange(result.Errors);
        msg.Warnings.AddRange(result.Warnings);
        return msg;
    }

    public static ValidationResult ToValidationResult(ValidationResultMsg msg) =>
        new(msg.Errors.ToArray()) { Warnings = msg.Warnings.ToArray() };

    // ---- ConnectionCheck -------------------------------------------------------------------------

    public static ConnectionCheckMsg ToConnectionCheckMsg(ConnectionCheck check)
    {
        var msg = new ConnectionCheckMsg { Ok = check.Ok };
        if (check.Message is not null)
        {
            msg.Message = check.Message;
        }

        return msg;
    }

    public static ConnectionCheck ToConnectionCheck(ConnectionCheckMsg msg) =>
        new(msg.Ok, msg.HasMessage ? msg.Message : null);

    // ---- NativeScan ------------------------------------------------------------------------------

    public static NativeScanResponse ToNativeScanResponse(NativeScan scan)
    {
        var msg = new NativeScanResponse
        {
            Found = true,
            SqlFragment = scan.SqlFragment,
            SchemaInferred = scan.SchemaInferred,
        };
        msg.SetupStatements.AddRange(scan.SetupStatements);
        if (scan.Mechanism is not null)
        {
            msg.Mechanism = scan.Mechanism;
        }

        if (scan.SniffFragment is not null)
        {
            msg.SniffFragment = scan.SniffFragment;
        }

        return msg;
    }

    public static NativeScan ToNativeScan(NativeScanResponse msg) =>
        new(msg.SqlFragment, msg.SetupStatements.ToArray())
        {
            Mechanism = msg.HasMechanism ? msg.Mechanism : null,
            SchemaInferred = msg.SchemaInferred,
            SniffFragment = msg.HasSniffFragment ? msg.SniffFragment : null,
        };

    // ---- NativeCopy ------------------------------------------------------------------------------

    public static NativeCopyResponse ToNativeCopyResponse(NativeCopy copy)
    {
        var msg = new NativeCopyResponse { Found = true, CopySql = copy.CopySql };
        msg.SetupStatements.AddRange(copy.SetupStatements);
        if (copy.Mechanism is not null)
        {
            msg.Mechanism = copy.Mechanism;
        }

        msg.Finalizations.AddRange(copy.Finalizations.Select(ToFileMoveMsg));
        return msg;
    }

    public static NativeCopy ToNativeCopy(NativeCopyResponse msg) =>
        new(msg.CopySql, msg.SetupStatements.ToArray())
        {
            Mechanism = msg.HasMechanism ? msg.Mechanism : null,
            Finalizations = msg.Finalizations.Select(ToFileMove).ToArray(),
        };

    public static FileMoveMsg ToFileMoveMsg(FileMove move) => new() { TempPath = move.TempPath, FinalPath = move.FinalPath };

    public static FileMove ToFileMove(FileMoveMsg msg) => new(msg.TempPath, msg.FinalPath);

    // ---- WriteResult -----------------------------------------------------------------------------

    public static WriteResultMsg ToWriteResultMsg(WriteResult result) =>
        new() { RowsWritten = result.RowsWritten, BatchesWritten = result.BatchesWritten };

    public static WriteResult ToWriteResult(WriteResultMsg msg) => new(msg.RowsWritten, msg.BatchesWritten);

    // ---- AbortSemantics --------------------------------------------------------------------------

    public static AbortSemanticsMsg ToAbortSemanticsMsg(AbortSemantics semantics) => semantics switch
    {
        AbortSemantics.DiscardsAll => AbortSemanticsMsg.AbortSemanticsDiscardsAll,
        AbortSemantics.BestEffort => AbortSemanticsMsg.AbortSemanticsBestEffort,
        AbortSemantics.None => AbortSemanticsMsg.AbortSemanticsNone,
        _ => throw new ArgumentOutOfRangeException(nameof(semantics), semantics, "unrecognized AbortSemantics"),
    };

    public static AbortSemantics ToAbortSemantics(AbortSemanticsMsg msg) => msg switch
    {
        AbortSemanticsMsg.AbortSemanticsDiscardsAll => AbortSemantics.DiscardsAll,
        AbortSemanticsMsg.AbortSemanticsBestEffort => AbortSemantics.BestEffort,
        AbortSemanticsMsg.AbortSemanticsNone => AbortSemantics.None,
        _ => throw new ArgumentOutOfRangeException(nameof(msg), msg, "unrecognized AbortSemanticsMsg"),
    };

    // ---- Arrow schema, IPC-stream-bytes only (DatasetSchemaMsg.arrow_schema_ipc / BeginWriteRequest.arrow_schema_ipc) --

    /// <summary>Same shape as <c>PcpService.SerializeSchemaAsync</c>: a schema-only Arrow IPC stream
    /// (start message + end-of-stream marker, no record batches) -- the spec rule for crossing a
    /// <see cref="Schema"/> is IPC bytes, never a hand-rolled field-list encoding.</summary>
    public static async Task<ByteString> SerializeSchemaAsync(Schema schema, CancellationToken ct)
    {
        using var buffer = new MemoryStream();
        using (var writer = new ArrowStreamWriter(buffer, schema, leaveOpen: true))
        {
            await writer.WriteStartAsync(ct).ConfigureAwait(false);
            await writer.WriteEndAsync(ct).ConfigureAwait(false);
        }

        return ByteString.CopyFrom(buffer.ToArray());
    }

    public static async ValueTask<Schema> DeserializeSchemaAsync(ByteString bytes, CancellationToken ct)
    {
        using var buffer = new MemoryStream(bytes.ToByteArray(), writable: false);
        using var reader = new ArrowStreamReader(buffer, leaveOpen: true);
        return await reader.GetSchema(ct).ConfigureAwait(false);
    }
}
