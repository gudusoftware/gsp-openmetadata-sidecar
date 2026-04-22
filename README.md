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

The sidecar is a bridge between two systems: **SQLFlow** (which understands SQL) and **OpenMetadata** (which stores metadata). It does not parse SQL itself.

### Pipeline

```
SQL file / inline SQL
        |
        v
  [1] SQLFlow API ───── parses SQL, returns table & column relationships
        |                (the sidecar sends raw SQL + dialect, nothing else)
        v
  [2] Lineage mapper ── extracts upstream/downstream table pairs + column mappings
        |                from SQLFlow's JSON response
        v
  [3] FQN builder ───── converts SQLFlow table names (e.g. SALESDB.DBO.CUSTOMERS)
        |                to OpenMetadata FQNs (e.g. mssql.salesdb.dbo.customers)
        |                using --service-name, --database-name, --schema-name
        v
  [4] Entity lookup ─── looks up each FQN in OpenMetadata to get entity UUIDs
        |                (exact match first, then case-insensitive search fallback)
        v
  [5] Lineage push ──── sends edges to OpenMetadata via PUT /api/v1/lineage
                         with column-level detail
```

### Where do table/column names come from?

**All table and column names come from SQLFlow, not from the sidecar.** The sidecar sends your raw SQL text to the SQLFlow API, and SQLFlow's parser identifies every table reference, column reference, and data-flow relationship in the SQL. The sidecar then maps those names to OpenMetadata entities.

For example, given this SQL:

```sql
CREATE VIEW [ReportDB].[dbo].[vw_CustomerOrders] AS
SELECT [SalesDB].[dbo].[Customers].[CustomerID],
       [SalesDB].[dbo].[Orders].[OrderDate]
FROM [SalesDB].[dbo].[Customers]
JOIN [SalesDB].[dbo].[Orders] ON ...
```

SQLFlow returns relationships like:
- `SALESDB.DBO.CUSTOMERS.CUSTOMERID` → `REPORTDB.DBO.VW_CUSTOMERORDERS.CUSTOMERID`
- `SALESDB.DBO.ORDERS.ORDERDATE` → `REPORTDB.DBO.VW_CUSTOMERORDERS.ORDERDATE`

The sidecar reads these and builds the lineage edges.

### What happens when tables don't exist in OpenMetadata?

**Tables must already exist in OpenMetadata before you run the sidecar.** The sidecar only creates lineage edges between existing entities — it does not create tables, databases, or schemas.

If a table referenced in the SQL is not found in OpenMetadata:

- The sidecar logs a warning: `Skipping lineage: upstream table not found: mssql.salesdb.dbo.customers`
- That specific lineage edge is skipped
- Other edges (where both sides exist) are still emitted

This means you need to either:
1. Run OpenMetadata's metadata ingestion first (so tables are registered), or
2. Create the table entities manually via the OpenMetadata API or UI

### Case-insensitive entity matching

SQLFlow uppercases identifiers for case-insensitive databases (MSSQL, etc.), but OpenMetadata may store them in mixed case (e.g. `Customers` not `CUSTOMERS`). The sidecar handles this automatically:

1. First tries an exact FQN lookup: `GET /api/v1/tables/name/mssql.salesdb.dbo.customers`
2. If that returns 404, falls back to OpenMetadata's search API with case-insensitive matching
3. Picks the best match (exact case-insensitive FQN match wins)

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

## FQN resolution and default database/schema

OpenMetadata identifies tables by fully-qualified names (FQNs) in the format `service.database.schema.table`. The sidecar builds FQNs from SQLFlow's output using the defaults you provide.

### How defaults work

When SQL references a table without a full `database.schema.table` path, SQLFlow returns only the parts present in the SQL. The sidecar fills in the missing parts from your `--database-name` and `--schema-name` settings:

| Parts in SQL | SQLFlow returns | Sidecar fills in from defaults |
|---|---|---|
| `customers` (1-part) | `CUSTOMERS` | database from `--database-name`, schema from `--schema-name` |
| `dbo.customers` (2-part) | `DBO.CUSTOMERS` | database from `--database-name` |
| `SalesDB.dbo.customers` (3-part) | `SALESDB.DBO.CUSTOMERS` | nothing — all parts present |

**Important: `--database-name` and `--schema-name` are NOT sent to SQLFlow.** They are used only on the sidecar side for FQN assembly. SQLFlow receives only the raw SQL text and the `--db-vendor` dialect flag. SQLFlow's parser extracts whatever table names appear in the SQL as-is.

### Defaults

| Setting | Default | When used |
|---|---|---|
| `--service-name` | `mssql` | Always prepended as the first FQN segment |
| `--database-name` | *(none)* | Used when SQL has 1-part or 2-part table names |
| `--schema-name` | `dbo` | Used when SQL has 1-part table names |

### Examples

| SQL reference | Config | Resolved FQN |
|---|---|---|
| `customers` | `--service-name mssql --database-name SalesDB --schema-name dbo` | `mssql.salesdb.dbo.customers` |
| `dbo.customers` | `--service-name mssql --database-name SalesDB` | `mssql.salesdb.dbo.customers` |
| `SalesDB.dbo.customers` | `--service-name mssql` | `mssql.salesdb.dbo.customers` |
| `[Sales].[dbo].[Invoices]` | `--service-name mssql` | `mssql.sales.dbo.invoices` |

Square brackets, backticks, and quotes are automatically stripped. All table/database/schema names from SQL are lowercased during FQN construction (with case-insensitive search fallback for OpenMetadata lookup).

### When to set `--database-name`

If your SQL uses fully-qualified names like `[SalesDB].[dbo].[Customers]`, you don't need `--database-name` — the database is already in the SQL. But if your SQL uses short names like `SELECT * FROM Customers`, you must provide `--database-name` so the sidecar knows which database to look up in OpenMetadata.

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
