# Pz.Connectors.Sdk

Write a [PipelineZ](https://pipelinez.dev) connector in C# the way a builtin is written, and serve it
out of process (PCP) from one line.

```xml
<!-- MyConnector.csproj -->
<PropertyGroup>
  <OutputType>Exe</OutputType>
</PropertyGroup>
```

```csharp
// Program.cs
using Pz.Connectors.Sdk;

return await PzConnectorHost.RunAsync(args, new MyConnector());
// or, to log to the pz host:
// return await PzConnectorHost.RunAsync(args, ctx => new MyConnector(ctx.LoggerFactory));
```

`MyConnector` implements `ISourceConnector` and/or `ISinkConnector` from `Pz.Connectors.Abstractions`.
Declare only the capabilities you implement: the SDK answers every optional RPC from the interfaces
your objects actually implement, and `pz connector test` fails a declaration nothing backs.

## Two argv modes

- `--pz-socket <path>` — serve over PCP (what `pz` passes when it spawns you).
- `--pz-manifest --out <file> [--entrypoint <rid>=<path>]... [--project-directory-anchor]` — write
  `pz.connector.json` from the connector object itself (the packaging targets run this for you).

Anything else is refused. Connector configuration only ever arrives through the `Configure` RPC.

## Packaging

```
dotnet publish -c Release -r linux-x64      # one per platform; Native AOT by default
dotnet pack    -c Release -p:PzNativeStaging=<dir holding every RID's publish>
```

| Property | Default | Meaning |
|---|---|---|
| `PzPackaging` | `aot` | `self-contained` opts a connector out of Native AOT (single-file CoreCLR) |
| `PzRuntimeIdentifiers` | `linux-x64;linux-arm64;osx-arm64;win-x64` | RIDs a package is expected to ship; a missing one warns (`PZSDK002`) |
| `PzNativeStaging` | `bin/pz-native/` | where `publish -r` stages each RID and where `pack` collects from |
| `PzProjectDirectoryAnchor` | `false` | set to `true` when the connector resolves relative paths in its own config; `pz` then passes the project directory as the `base_dir` connection option |

Publish for the packing machine's own RID too, whichever RIDs you ship: the manifest is written by
running the binary, so a machine that packs without having published its own RID fails with
`PZSDK003`.

The nupkg carries `runtimes/<rid>/native/<binary>` per RID and the generated manifest at its root;
`pz restore` installs the host's RID and `pz run` spawns it. See
https://pipelinez.dev/how-to/author-a-connector/ for the full guide and a release workflow.

## Telemetry

When `pz run` is given `--otel-endpoint` (or `PZ_OTEL_ENDPOINT`), the host passes that endpoint and
the run id to your process in the handshake, and puts a W3C `traceparent` on every RPC it issues while
one of its own spans is current. The SDK then:

- builds an OpenTelemetry tracer and meter provider exporting OTLP/grpc to that endpoint, with
  resource `service.name=pz-connector`, `service.version=<your ConnectorInfo.Version>`,
  `pz.connector.name`, `pz.run.id`;
- opens a `pcp.<Rpc>` server span per RPC — every RPC but `HostChannel`, which lives as long as the
  process — under the engine's node span, tagged `pz.instance` (the host's id for this connector
  instance: the connection name for an open the engine drives, or `<connector name>#<n>` for one it
  cannot name), plus a `pcp.read_stream`/`pcp.write_stream` span around each data-plane transfer. An
  RPC that arrives with no `traceparent` starts a new trace rather than attaching to anything;
- flushes on shutdown, bounded to three seconds.

Anything you start from `ctx.ActivitySource` or record on `ctx.Meter` lands there too:

```csharp
return await PzConnectorHost.RunAsync(args, ctx => new MyConnector(ctx.LoggerFactory, ctx.ActivitySource, ctx.Meter));

// inside the connector:
using var span = _source.StartActivity("list objects");
_pagesFetched.Add(1);
```

To also export a client library's own `ActivitySource`/`Meter` (Npgsql, the AWS SDK, ...), name them:

```csharp
return await PzConnectorHost.RunAsync(args, ctx => new MyConnector(ctx),
    new PzConnectorHostOptions { ActivitySources = ["Npgsql"], Meters = ["Npgsql"] });
```

With no endpoint nothing is built and every span or meter call is a no-op. Never put a configuration
value in a span name, tag, or metric label: what you emit is what the operator sees.
