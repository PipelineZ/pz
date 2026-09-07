# Pz.Connectors.Sdk

Write a [PipelineZ](https://pipelinez.dev) connector in C# the way a builtin is written, and serve it
out of process (PCP) from one line.

```xml
<!-- MyConnector.csproj -->
<PropertyGroup>
  <OutputType>Exe</OutputType>
  <!-- Restore reads this line, not the SDK's packaging files: it is what pulls the Native AOT
       compiler pack. PzPackaging=self-contained turns it back off. -->
  <PublishAot>true</PublishAot>
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

`<PublishAot>true</PublishAot>` belongs in the project file itself (or on the command line as
`-p:PublishAot=true`): NuGet restore evaluates a project with every package's build files excluded,
so the compiler pack Native AOT needs is only restored when the project asks for it, never because
the SDK's targets derived it from `PzPackaging`. A publish that would silently fall back to a CoreCLR
layout fails with `PZSDK005` instead. `self-contained` turns `PublishAot` off again, so the line
costs a connector that opts out nothing but a download; switching such a connector to Native AOT
from the command line takes both flags (`-p:PzPackaging=aot -p:PublishAot=true`).

Publish for the packing machine's own RID too, whichever RIDs you ship: the manifest is written by
running the binary, so a machine that packs without having published its own RID fails with
`PZSDK003`.

The nupkg carries `runtimes/<rid>/native/<binary>` per RID and the generated manifest at its root;
`pz restore` installs the host's RID and `pz run` spawns it. See
https://pipelinez.dev/how-to/author-a-connector/ for the full guide and a release workflow.
