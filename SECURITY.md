# Security policy

## Supported versions

Pre-1.0, only the latest v0.x minor receives fixes. Once v1.0.0 ships, the latest
minor of the current major is supported.

## Reporting a vulnerability

Please **do not open a public issue** for anything security-sensitive.

Report privately via GitHub's security advisories:
[github.com/pipelinez/pz/security/advisories/new](https://github.com/pipelinez/pz/security/advisories/new)
(the "Report a vulnerability" button on the repository's Security tab). If that is
unavailable to you, email contact@pipelinez.dev with `[pz security]` in the
subject.

You can expect an acknowledgement within a week. Please include a reproduction if
you can; a project directory + the `pz` command that triggers it is ideal.

## Scope notes

- `pz` is a **local-first tool that executes your own configuration**: SQL in your
  pipelines runs in your DuckDB session, and connector credentials come from your
  own environment. A project author being able to affect their own machine through
  their own config is not, by itself, a vulnerability.
- The **agent surface is stricter**: under `pz mcp`, secrets never transit tool
  results, literal credentials are refused (PZ0601), and paths are confined to the
  project directory (PZ0606). Bypasses of those guards ARE in scope.
- Credential leakage into logs, run artifacts, error messages, or the NDJSON event
  stream is in scope everywhere — the codebase's standing rule is that config
  values never appear in any output channel.
