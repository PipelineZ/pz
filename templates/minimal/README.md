# pz_new_project

A new PipelineZ project. Two files, both commented, nothing to delete.

## What goes where

- `project.yml` — name, version, declared connectors, vars, engine settings, retention
- `connections.yml` — every place this project talks to, and optionally the entities in each
- `pipelines/*.sql` — one file per transformation (create this directory when you add your first)
- `pipelines/configs/*.yml` — optional per-pipeline data-quality check sidecars

## Your first pipeline

1. Declare a connection in `connections.yml` — a connection is a place with credentials.
2. Add `pipelines/<name>.sql` that reads with `source()` and writes with `sink()`:

       INSERT INTO {{ sink('lake', 'my_table', strategy: 'replace') }}
       select * from {{ source('warehouse', 'dbo.orders') }}

3. `pz validate` checks config shape and SQL. `pz plan` shows the compiled DAG. `pz run` executes it.

## Verbs worth knowing

| Verb | Does |
|---|---|
| `pz validate` | Check config, SQL, and (with `--connect`) live connectivity |
| `pz plan` | Show the DAG and per-node plan without running anything |
| `pz run [name]` | Run a flow, or `--all` for everything |
| `pz test` | Run only the data-quality checks |
| `pz retry` | Resume a failed run from where it stopped |

Want a worked example instead? `pz init <name> --template sample` scaffolds a runnable
four-pipeline project. `pz init --list-templates` shows every starting point.

Full documentation: https://pipelinez.dev
