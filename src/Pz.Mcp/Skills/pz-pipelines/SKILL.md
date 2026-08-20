---
name: pz-pipelines
description: >-
  USE FOR authoring, validating, and inspecting pz batch-ETL pipelines (connections.yml
  + pipelines/*.sql, compiled into a dependency-ordered DAG that runs over DuckDB)
  through the pz MCP tools -- adding or editing connections/entities/pipelines, fixing
  PZ#### validation errors, inspecting a project's compiled DAG or execution plan, and
  (only when the connected `pz mcp` server was started with --allow-run) running or
  retrying a flow.
  DO NOT USE FOR general SQL/data-engineering work unrelated to a pz project, hand-editing
  files under .pz/ (generated run/state artifacts, never authored by hand), or
  running/retrying a flow when the server was NOT started with --allow-run -- in that
  case pz_run/pz_retry/pz_run_results are simply absent from the tool list; do not try
  to call them.
---

# pz pipelines

pz (`pipelinez`) is a dbt-inspired batch ETL CLI: one `connections.yml` (places with
credentials, and the entities read from / written to them) plus SQL files under
`pipelines/` compile into a dependency-ordered DAG that runs in-process over DuckDB.

## The tool loop

1. **Orient.** `pz_connector_reference` (every connector this project uses, its
   capability flags, and the JSON Schemas for its connection/entity options) and
   `pz_project_overview` (flows, connections and their entities, pipelines and their
   refs/sources/sinks, the compiled DAG) -- read these before writing anything.
2. **Author.** `pz_add_connection` / `pz_update_connection` / `pz_remove_connection`,
   `pz_add_entity` / `pz_set_entity_options` / `pz_remove_entity`,
   `pz_write_pipeline` / `pz_remove_pipeline`, and `pz_init_project` to scaffold a
   brand-new project. Every mutating tool self-verifies after applying and reports the
   resulting errors, if any -- read its response, don't assume success.
3. **Verify.** `pz_validate` (config, connector option schemas, and SQL; pass
   `connect: true` to also probe live connections and fetch schemas), `pz_compile`
   (render pipelines and build the DAG), `pz_entity_schema` (a live schema fetch for
   one connection+entity), and `pz_state` (stored watermarks, sync-state, schema
   baselines, plus the latest run's summary).
4. **Plan.** `pz_plan` -- the per-node execution strategy (native scan/copy vs. the
   universal batch path) and why. Read-only; never writes `plan.json`.
5. **Run -- only under `--allow-run`.** `pz_run` (a named flow, or `all: true` for the
   whole project), `pz_retry` (re-run the last failed run, reusing staged data where
   safe), and `pz_run_results` (full structured results for a run id). These three
   tools move real data and are absent from the tool list entirely when the server was
   started without `--allow-run` -- that is the operator's capability boundary, never
   a per-call refusal.

## The PZ-code fix-loop

Every tool's error response carries a `code` (`PZ####`), a `message`, and a
`next_step`. Trust the `next_step` over guessing -- it names the exact fix pz expects.
Validation reports every error at once, never fails on the first one, so fix everything
it reports before re-validating rather than iterating one error at a time.

## Full reference

`references/authoring-for-agents.md` (installed alongside this file) is the complete
authoring guide: every tool's parameters, the connections.yml/pipeline-SQL authoring
surface, and worked examples.
