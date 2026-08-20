# Documentation

The PipelineZ documentation lives at **[pipelinez.dev](https://pipelinez.dev)** — concepts,
how-to guides, the CLI and `project.yml` reference, the event-stream contract, and the
performance numbers.

`pz` itself can read it: `pz mcp` exposes `pz_docs_list`, `pz_docs_search` and `pz_docs_get`,
so an AI agent connected to the tool can search the documentation directly. Those tools read
the site over the network — set `PZ_DOCS_URL` to point them at a mirror.

## Why two files stayed behind

Both are here because something in the build or the test suite reads them off disk. Editing them
here is correct — these are the source of truth, and the site's copies are generated from them.

[`events.md`](events.md) is the NDJSON event-stream contract, and `EventsDocReflectionTests`
reflects over every `RunEvent` record to assert that each property is documented here and that
every documented field still exists. That test is what keeps the contract honest, and it cannot
run against a page on a website.

[`reference/authoring-for-agents.md`](reference/authoring-for-agents.md) is compiled into the `pz`
binary as an embedded resource. `pz mcp init` copies it onto disk as part of the `pz-pipelines`
skill, so it must be present with no network and no source tree.

Everything else is published only, and `pz mcp` fetches it.
