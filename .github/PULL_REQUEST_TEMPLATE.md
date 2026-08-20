## What this changes

<!-- One or two sentences: the behavior before, the behavior after, and why. -->

## Checklist

- [ ] `dotnet build Pz.slnx -c Release` — zero warnings (TreatWarningsAsErrors)
- [ ] `dotnet test Pz.slnx -c Release --no-build` — zero failures (docker-gated suites may SKIP)
- [ ] Behavior changes landed test-first (see CONTRIBUTING.md)
- [ ] Touched `src/Pz.Cli`, the init template, or a packable `.csproj`? → ran `scripts/verify-tool-install.sh`
- [ ] Changed a stability contract (NDJSON events, MCP envelope, error codes, connector ABI)? → updated the matching doc; the reflection tests will hold you to it
