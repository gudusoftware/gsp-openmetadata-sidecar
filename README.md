# gsp-openmetadata-sidecar

Recover SQL lineage that OpenMetadata's parser misses — across MSSQL stored procedures, BigQuery procedural SQL, Snowflake, Oracle, and [20+ dialects](https://www.gudusoft.com/sql-dialects/) — using [Gudu SQLFlow](https://sqlflow.gudusoft.com).

## The problem

OpenMetadata uses a three-parser chain (sqlglot → sqlfluff → sqlparse via `collate-sqllineage`) for SQL lineage extraction. This chain silently loses lineage on:

- **MSSQL stored procedures** — `BEGIN...END`, `DECLARE`, `CREATE PROCEDURE` ([#16737](https://github.com/open-metadata/OpenMetadata/issues/16737), [#25299](https://github.com/open-metadata/OpenMetadata/issues/25299))
- **MSSQL identifier casing** — square bracket notation `[dbo].[table]` ([#16710](https://github.com/open-metadata/OpenMetadata/issues/16710))
- **Temp table lineage** — `#tempTable` as intermediate steps (3-hop lineage fails) ([#25299](https://github.com/open-metadata/OpenMetadata/issues/25299))
- **Multi-statement SQL / SQL scripting** — `DECLARE`, `IF/THEN`, `CREATE TEMP TABLE`
- **MERGE column-level lineage** — `MERGE INTO ... WHEN MATCHED ... WHEN NOT MATCHED`
- **Cross-database MSSQL lineage** — queries spanning multiple databases ([Discussion #23717](https://github.com/open-metadata/OpenMetadata/discussions/23717))

## The solution

This sidecar parses SQL that OpenMetadata's parser failed on using Gudu SQLFlow (which handles procedural SQL and dialect-specific syntax natively), then pushes the recovered lineage to OpenMetadata via its REST API (`PUT /api/v1/lineage`).

```
OpenMetadata ingestion (unchanged)       gsp-openmetadata-sidecar (this tool)
  query logs -> collate-sqllineage         |
                    |                      |  parse SQL with Gudu SQLFlow
                    v                      |  resolve tables via OM API
              silently skipped             |  push lineage via PUT /api/v1/lineage
              (lineage lost)               v
                                     OpenMetadata (lineage restored)
```

No changes to OpenMetadata code required. This tool uses the public REST API only.

## Install

```bash
pip install gsp-openmetadata-sidecar
```

## Quick start

Example SQL files are included in the `examples/` directory.

```bash
# MSSQL stored procedure with BEGIN/END, temp tables, MERGE (dry run):
gsp-openmetadata-sidecar --sql-file examples/mssql_stored_procedure.sql --dry-run

# MSSQL view with square-bracket identifiers:
gsp-openmetadata-sidecar --sql-file examples/mssql_case_sensitivity.sql --dry-run

# BigQuery procedural SQL with DECLARE, CREATE TEMP TABLE:
gsp-openmetadata-sidecar --sql-file examples/bigquery_procedural.sql --db-vendor dbvbigquery --dry-run

# Inline SQL:
gsp-openmetadata-sidecar --sql "CREATE PROC p AS BEGIN INSERT INTO t2 SELECT a, b FROM t1 END" --dry-run
```

## Push lineage to OpenMetadata

To actually push lineage (not just dry-run), you need:

1. An OpenMetadata instance with the target tables already ingested
2. A JWT token (get from Settings > Bots in the OpenMetadata UI)
3. The database service name as registered in OpenMetadata

```bash
gsp-openmetadata-sidecar \
  --sql-file stored_proc.sql \
  --om-server http://localhost:8585/api \
  --om-token "eyJ..." \
  --service-name mssql_prod \
  --database-name SalesDB \
  --schema-name dbo
```

Or use a config file (see `examples/sidecar.yaml.example`):

```bash
cp examples/sidecar.yaml.example sidecar.yaml
# Edit sidecar.yaml with your settings
gsp-openmetadata-sidecar --config sidecar.yaml --sql-file stored_proc.sql
```

## How it works

1. **Parse SQL** — sends your SQL to [Gudu SQLFlow](https://sqlflow.gudusoft.com) (cloud API, self-hosted Docker, or local JAR — your choice)
2. **Extract lineage** — maps SQLFlow's response to table-level and column-level lineage relationships
3. **Resolve entities** — looks up table FQNs in OpenMetadata via `GET /api/v1/tables/name/{fqn}` to get entity UUIDs
4. **Push lineage** — sends lineage edges to OpenMetadata via `PUT /api/v1/lineage` with column-level detail

## Backend modes

| Mode | Auth | Rate limit | Data stays... | Best for |
|---|---|---|---|---|
| `anonymous` (default) | None | 50/day per IP | Gudu cloud | Quick evaluation |
| `authenticated` | API key | 10k/month | Gudu cloud | Regular use |
| `self_hosted` | Token exchange | Unlimited | Your network | Production / air-gapped |
| `local_jar` | None (local) | Unlimited | Your machine | Offline / no Docker |

```bash
# Anonymous (default — no signup needed):
gsp-openmetadata-sidecar --sql-file proc.sql --dry-run

# Authenticated (personal API key):
GSP_BACKEND_MODE=authenticated GSP_SQLFLOW_SECRET_KEY=sk-xxx \
  gsp-openmetadata-sidecar --sql-file proc.sql --dry-run

# Self-hosted Docker:
gsp-openmetadata-sidecar --mode self_hosted \
  --sqlflow-url http://localhost:8165/api/gspLive_backend/sqlflow/generation/sqlflow/exportFullLineageAsJson \
  --sql-file proc.sql --dry-run

# Local JAR (no network):
gsp-openmetadata-sidecar --mode local_jar --jar-path /path/to/gsqlparser-shaded.jar \
  --sql-file proc.sql --dry-run
```

## FQN resolution

OpenMetadata identifies tables by fully-qualified names (FQNs) in the format `service.database.schema.table`. The sidecar builds FQNs from SQLFlow's output using the defaults you provide:

| SQL reference | Config needed | Resolved FQN |
|---|---|---|
| `customers` | `--service-name mssql --database-name SalesDB --schema-name dbo` | `mssql.salesdb.dbo.customers` |
| `dbo.customers` | `--service-name mssql --database-name SalesDB` | `mssql.salesdb.dbo.customers` |
| `SalesDB.dbo.customers` | `--service-name mssql` | `mssql.salesdb.dbo.customers` |
| `[Sales].[dbo].[Invoices]` | `--service-name mssql` | `mssql.sales.dbo.invoices` |

Square brackets, backticks, and quotes are automatically stripped.

## Configuration

All settings can be provided via CLI flags, environment variables, or a YAML config file. Priority: CLI > env vars > YAML > defaults.

| Setting | CLI flag | Env var | YAML key |
|---|---|---|---|
| Backend mode | `--mode` | `GSP_BACKEND_MODE` | `sqlflow.mode` |
| SQLFlow URL | `--sqlflow-url` | `GSP_SQLFLOW_URL` | `sqlflow.url` |
| SQL dialect | `--db-vendor` | `GSP_DB_VENDOR` | `sqlflow.db_vendor` |
| OM server | `--om-server` | `GSP_OM_SERVER` | `openmetadata.server` |
| OM token | `--om-token` | `GSP_OM_TOKEN` | `openmetadata.token` |
| Service name | `--service-name` | `GSP_OM_SERVICE_NAME` | `openmetadata.service_name` |
| Database name | `--database-name` | `GSP_OM_DATABASE_NAME` | `openmetadata.database_name` |
| Schema name | `--schema-name` | `GSP_OM_SCHEMA_NAME` | `openmetadata.schema_name` |

## Issues this tool addresses

| OpenMetadata issue | Problem | Status |
|---|---|---|
| [#16737](https://github.com/open-metadata/OpenMetadata/issues/16737) | MSSQL stored procedure lineage not reflected | Open since June 2024 |
| [#25299](https://github.com/open-metadata/OpenMetadata/issues/25299) | CREATE PROCEDURE / BEGIN-END / temp tables fail | Open — release backlog |
| [#16710](https://github.com/open-metadata/OpenMetadata/issues/16710) | SQL Server ingestion fails on stored procs with `%` | Open |
| [#17586](https://github.com/open-metadata/OpenMetadata/issues/17586) | MS SQL procedures lineage not picked up | Partially fixed |
| [Discussion #23717](https://github.com/open-metadata/OpenMetadata/discussions/23717) | Cross-database MSSQL lineage | Unanswered |

## License

Apache-2.0. See [LICENSE](LICENSE).

This tool calls the [Gudu SQLFlow](https://sqlflow.gudusoft.com) service for SQL parsing. The SQLFlow service is proprietary software by [Gudu Software](https://www.gudusoft.com). See the [SQLFlow documentation](https://docs.gudusoft.com) for service terms.

## Related

- [gsp-datahub-sidecar](https://github.com/gudusoftware/gsp-datahub-sidecar) — same concept for DataHub
- [Gudu SQLFlow](https://sqlflow.gudusoft.com) — the SQL lineage engine powering this tool
- [General SQL Parser](https://sqlparser.com) — the SQL parser library by Gudu Software
