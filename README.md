# gsp-openmetadata-sidecar

Recover SQL lineage that OpenMetadata's parser silently drops — MSSQL stored procedures, BigQuery procedural SQL, MERGE column lineage, temp-table hops, and [20+ dialects](https://www.gudusoft.com/sql-dialects/) — using [Gudu SQLFlow](https://sqlflow.gudusoft.com), then push the recovered lineage back to OpenMetadata via `PUT /api/v1/lineage`.

No changes to OpenMetadata code. Public REST API only.

## When to use this

OpenMetadata uses a three-parser chain (sqlglot → sqlfluff → sqlparse via `collate-sqllineage`) that silently loses lineage on procedural and dialect-specific SQL. Use this sidecar when you see any of:

- MSSQL stored procedures (`BEGIN…END`, `DECLARE`, `CREATE PROCEDURE`)
- MSSQL square-bracket identifiers (`[dbo].[table]`)
- Temp-table intermediates (`#tempTable`), breaking multi-hop lineage
- Multi-statement scripts (`DECLARE`, `IF/THEN`, `CREATE TEMP TABLE`)
- `MERGE INTO … WHEN MATCHED … WHEN NOT MATCHED` column lineage
- Cross-database MSSQL queries

See [Related issues](#related-issues) for upstream tickets.

## OpenMetadata primer (for first-time users)

If you've never used OpenMetadata before, this section gives you the vocabulary the rest of this README assumes. Skim it before you start passing flags like `--service-name` or `--om-token` — most first-time friction with lineage comes from misunderstanding how OpenMetadata identifies tables, not from bugs in this sidecar.

### The entity hierarchy

OpenMetadata stores metadata as **entities**. Every table, column, dashboard, pipeline, and user is an entity. For SQL lineage you care about three entity types, arranged as a strict 4-level hierarchy:

```
Database Service   (e.g. "mssql_prod")      — a connection to a source system
    └── Database   (e.g. "SalesDB")         — a logical database within that service
        └── Schema (e.g. "dbo")             — a schema/namespace within the database
            └── Table (e.g. "Customers")    — the actual table (with columns inside)
```

Key points for newcomers:

- **Service is not the server.** A service is an OpenMetadata-registered connection. You choose the name when you set it up in the OM UI (Settings → Services → Databases → Add). It is *usually* named after the source system (e.g. `mssql_prod`, `snowflake_analytics`), but it is an opaque identifier — not the SQL Server hostname or DSN.
- **Database ≠ schema.** For MSSQL, `SalesDB.dbo.Customers` has database `SalesDB` and schema `dbo`. For PostgreSQL or Snowflake, same structure. For MySQL there's effectively no separate schema level, so OM stores a synthetic schema called `default`.
- **Columns live inside the table entity**, not as standalone entities with their own URLs. Column lineage is expressed *within* a table-to-table lineage edge.

### Fully-Qualified Name (FQN)

An FQN is how OpenMetadata uniquely identifies an entity across the whole catalog. For tables the shape is:

```
service.database.schema.table
```

Concrete example:

```
mssql_prod.SalesDB.dbo.Customers
```

Things worth knowing up front:

- **FQNs are case-preserving but the API lookup is case-sensitive by default.** `mssql_prod.SalesDB.dbo.Customers` and `mssql_prod.salesdb.dbo.customers` are treated as different FQNs by the direct lookup endpoint `GET /api/v1/tables/name/{fqn}`. This sidecar handles that by falling back to a case-insensitive search — but it's still the single biggest source of "why can't it find my table?" confusion.
- **Segments containing dots or special characters are quoted.** `mssql_prod."My.Database".dbo."Order Details"` is a valid FQN. The sidecar does not currently emit quoted segments, so avoid dots in names if you can.
- **The service segment is chosen by whoever registered the service**, not derived from the SQL. This is why the sidecar needs `--service-name` — SQLFlow can tell you the SQL said `SalesDB.dbo.Customers`, but only you know that `SalesDB` sits under the OM service named `mssql_prod`.

### Ingestion vs lineage

OpenMetadata distinguishes two kinds of metadata:

1. **Structural ingestion** — creates the table entities themselves by inspecting the source system's information_schema (or equivalent). Usually runs on a schedule as an Airflow DAG or as the OM Ingestion container. Produces rows in `tables`, `databases`, `schemas`, etc.
2. **Lineage ingestion** — creates directed edges *between* existing table entities. Produces rows in `entity_relationship` with relation type `upstream`.

**Lineage ingestion does NOT create tables.** If `SalesDB.dbo.Customers` doesn't already exist as a table entity, you cannot attach lineage to it. This sidecar does lineage ingestion only — you must run structural ingestion first (or create tables via the API / UI).

When the sidecar logs `Skipping lineage: upstream table not found: mssql.salesdb.dbo.customers`, it means: "I asked OM for that FQN and got a 404 (even after case-insensitive fallback). The SQL references a table that OM doesn't know about yet."

### Table-level vs column-level lineage

- **Table-level lineage** — a directed edge between two table entities. Answers *"does data flow from A to B?"*. Persisted as a single row per edge.
- **Column-level lineage** — on top of the table edge, a list of column mappings: *"column `A.orderId` feeds column `B.orderId`, and columns `A.firstName + A.lastName` feed column `B.fullName`"*. Persisted inside the edge's `lineageDetails.columnsLineage` JSON field.

OpenMetadata's UI renders column lineage as thin lines between column pills inside each table card. You only get those lines if the sidecar populated `columnsLineage`. Turn it off with `--no-column-lineage` if you only want table-level arrows.

### Authentication: Bots and JWT tokens

OpenMetadata's REST API uses bearer-token auth. For automated tools (like this sidecar), the recommended pattern is:

1. In the OM UI: **Settings → Bots → Add new bot** (e.g. `lineage-sidecar-bot`).
2. Generate a JWT token for that bot. Copy it immediately — OM only shows it once.
3. Pass it to the sidecar via `--om-token` or the `GSP_OM_TOKEN` env var.

Bot tokens are long-lived by default (OM rotates them only when you regenerate). Treat them like passwords. The token carries the bot's role — make sure the bot has the `DataConsumer` role plus `EditLineage` permission, otherwise the `PUT /api/v1/lineage` call will return 403.

### The lineage API this sidecar uses

The sidecar pushes edges via:

```
PUT /api/v1/lineage
{
  "edge": {
    "fromEntity": { "id": "<upstream-uuid>",   "type": "table" },
    "toEntity":   { "id": "<downstream-uuid>", "type": "table" },
    "lineageDetails": {
      "sqlQuery": "CREATE PROC ...",
      "source":   "QueryLineage",
      "columnsLineage": [ { "fromColumns": [...], "toColumn": "..." } ]
    }
  }
}
```

Note that both sides are identified by **entity UUID**, not FQN. The sidecar resolves FQN → UUID itself (step [4] in the pipeline diagram below). That lookup is where FQN correctness pays off: a wrong FQN means a 404 means a skipped edge.

### How these concepts map to sidecar flags

| OM concept | Sidecar flag | Notes |
|---|---|---|
| Database service name | `--service-name` | First FQN segment. Default: `mssql`. |
| Database | `--database-name` | Fills FQN when SQL has 1- or 2-part names. No default. |
| Schema | `--schema-name` | Fills FQN when SQL has 1-part names. Default: `dbo`. |
| OM server URL | `--om-server` | E.g. `http://localhost:8585/api` (note the `/api` suffix). |
| Bot JWT token | `--om-token` | From Settings → Bots. |
| Column lineage toggle | `--column-lineage` / `--no-column-lineage` | Default: on. |

With that vocabulary in hand, the rest of this README should read without surprises.

## Requirements

- Python 3.9+
- Network access to the SQLFlow API (for `anonymous`, `authenticated`, `self_hosted` modes)
- Java 8+ and a licensed `gsqlparser-*-shaded.jar` (for `local_jar` mode only)
- OpenMetadata tables must already exist if you plan to push lineage — the sidecar creates lineage edges, not tables

## Install

From PyPI:

```bash
pip install gsp-openmetadata-sidecar
```

From source (recommended for contributors and environments with an externally managed Python):

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
gsp-openmetadata-sidecar --help
```

## Quick start

Example SQL files are in `examples/`. The fastest verified command (anonymous mode, no signup):

```bash
gsp-openmetadata-sidecar --sql-file examples/mssql_case_sensitivity.sql --dry-run
```

This parses the file with SQLFlow, prints the extracted lineage edges, and does not call OpenMetadata (because of `--dry-run`).

More examples:

```bash
# MSSQL stored procedure with BEGIN/END, temp tables, MERGE:
gsp-openmetadata-sidecar --sql-file examples/mssql_stored_procedure.sql --dry-run

# BigQuery procedural SQL with DECLARE, CREATE TEMP TABLE:
gsp-openmetadata-sidecar --sql-file examples/bigquery_procedural.sql --db-vendor dbvbigquery --dry-run

# Inline SQL:
gsp-openmetadata-sidecar --sql "CREATE PROC p AS BEGIN INSERT INTO t2 SELECT a, b FROM t1 END" --dry-run
```

## Common commands

```bash
# Table-level lineage only (skip column mappings):
gsp-openmetadata-sidecar --sql-file proc.sql --no-column-lineage --dry-run

# Print raw SQLFlow JSON response to stdout:
gsp-openmetadata-sidecar --sql-file proc.sql --json --dry-run

# Verbose/debug logging:
gsp-openmetadata-sidecar --sql-file proc.sql --dry-run -v

# Show version:
gsp-openmetadata-sidecar --version
```

## Push lineage to OpenMetadata

To actually push lineage (not dry-run), you need:

1. An OpenMetadata instance with the target tables already ingested
2. A JWT token (Settings → Bots in the OpenMetadata UI)
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

Or via a config file (see `examples/sidecar.yaml.example`):

```bash
cp examples/sidecar.yaml.example sidecar.yaml
# Edit sidecar.yaml with your settings
gsp-openmetadata-sidecar --config sidecar.yaml --sql-file stored_proc.sql
```

## Backend modes

| Mode | Auth | Rate limit | Data stays… | Best for |
|---|---|---|---|---|
| `anonymous` (default) | None | 50/day per IP | Gudu cloud | Quick evaluation |
| `authenticated` | `user_id` + `secret_key` | 10k/month | Gudu cloud | Regular use |
| `self_hosted` | `user_id` + `secret_key` (token exchange) | Unlimited | Your network | Production / air-gapped |
| `local_jar` | None (local process) | Unlimited | Your machine | Offline / no Docker |

```bash
# Anonymous (default — no signup needed):
gsp-openmetadata-sidecar --sql-file proc.sql --dry-run

# Authenticated — BOTH user_id and secret_key are required:
GSP_BACKEND_MODE=authenticated \
GSP_SQLFLOW_USER_ID=your-user-id \
GSP_SQLFLOW_SECRET_KEY=your-secret-key \
  gsp-openmetadata-sidecar --sql-file proc.sql --dry-run

# Equivalent with CLI flags:
gsp-openmetadata-sidecar --mode authenticated \
  --user-id your-user-id --secret-key your-secret-key \
  --sql-file proc.sql --dry-run

# Self-hosted SQLFlow Docker (token-exchange protocol, same creds pattern):
gsp-openmetadata-sidecar --mode self_hosted \
  --sqlflow-url http://localhost:8165/api/gspLive_backend/sqlflow/generation/sqlflow/exportFullLineageAsJson \
  --user-id gudu --secret-key 0123456789 \
  --sql-file proc.sql --dry-run

# Local JAR (no network; requires Java and a licensed JAR):
gsp-openmetadata-sidecar --mode local_jar \
  --jar-path /path/to/gsqlparser-shaded.jar \
  --sql-file proc.sql --dry-run
```

## How it works

The sidecar is a bridge between **SQLFlow** (which understands SQL) and **OpenMetadata** (which stores metadata). SQL syntax parsing is delegated to SQLFlow — the sidecar decides how the file is chunked, maps the result to OpenMetadata entities, and pushes lineage edges.

### Pipeline

```
SQL file / inline SQL
        |
        v
  [1] SQLFlow backend ── parses SQL, returns table & column relationships
        |                 (sidecar sends raw SQL + dialect, nothing else)
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

**All table and column names come from SQLFlow, not from the sidecar.** The sidecar sends your raw SQL text to SQLFlow, and SQLFlow's parser identifies every table reference, column reference, and data-flow relationship. The sidecar then maps those names to OpenMetadata entities.

Example — given this SQL:

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

The sidecar reads these and builds lineage edges.

### What happens when tables don't exist in OpenMetadata?

The sidecar only creates lineage edges between existing entities — it does not create tables, databases, or schemas.

If a table referenced in the SQL is not found in OpenMetadata:

- The sidecar logs `Skipping lineage: upstream table not found: mssql.salesdb.dbo.customers`
- That specific edge is skipped
- Other edges (where both sides exist) are still emitted

So either run OpenMetadata's metadata ingestion first, or create the table entities via the OpenMetadata API / UI.

### Case-insensitive entity matching

SQLFlow uppercases identifiers for case-insensitive databases (MSSQL, etc.), but OpenMetadata may store them in mixed case (e.g. `Customers` not `CUSTOMERS`). The sidecar:

1. First tries an exact FQN lookup: `GET /api/v1/tables/name/mssql.salesdb.dbo.customers`
2. On 404, falls back to the search API (case-insensitive)
3. Picks the best match (exact case-insensitive FQN match wins)

## FQN resolution and default database/schema

OpenMetadata identifies tables by FQN: `service.database.schema.table`. The sidecar builds FQNs from SQLFlow's output using the defaults you provide.

### How defaults work

When SQL references a table without a full `database.schema.table` path, SQLFlow returns only the parts present in the SQL. The sidecar fills in missing parts from your `--database-name` and `--schema-name` settings:

| Parts in SQL | SQLFlow returns | Sidecar fills in from defaults |
|---|---|---|
| `customers` (1-part) | `CUSTOMERS` | database from `--database-name`, schema from `--schema-name` |
| `dbo.customers` (2-part) | `DBO.CUSTOMERS` | database from `--database-name` |
| `SalesDB.dbo.customers` (3-part) | `SALESDB.DBO.CUSTOMERS` | nothing — all parts present |

**Important: `--database-name` and `--schema-name` are NOT sent to SQLFlow.** They are used only on the sidecar side for FQN assembly. SQLFlow receives only the raw SQL text and the `--db-vendor` dialect flag.

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

Square brackets, backticks, and quotes are automatically stripped. All table/database/schema names from SQL are lowercased during FQN construction (with case-insensitive search fallback).

### When to set `--database-name`

If your SQL uses fully-qualified names like `[SalesDB].[dbo].[Customers]`, you don't need `--database-name`. If your SQL uses short names like `SELECT * FROM Customers`, you must provide `--database-name` so the sidecar knows which database to look up in OpenMetadata.

## Configuration reference

All settings can be provided via CLI flags, environment variables, or a YAML config file. Priority: **CLI > env vars > YAML > defaults**.

### Required

| Setting | CLI flag | Env var | YAML key | Notes |
|---|---|---|---|---|
| Backend mode | `--mode` | `GSP_BACKEND_MODE` | `sqlflow.mode` | `anonymous` \| `authenticated` \| `self_hosted` \| `local_jar` |
| User ID | `--user-id` | `GSP_SQLFLOW_USER_ID` | `sqlflow.user_id` | Required for `authenticated` and (usually) `self_hosted` |
| Secret key | `--secret-key` | `GSP_SQLFLOW_SECRET_KEY` | `sqlflow.secret_key` | Required for `authenticated` and (usually) `self_hosted` |
| JAR path | `--jar-path` | `GSP_JAR_PATH` | `sqlflow.jar_path` | Required for `local_jar` |

### Common

| Setting | CLI flag | Env var | YAML key |
|---|---|---|---|
| SQLFlow URL | `--sqlflow-url` | `GSP_SQLFLOW_URL` | `sqlflow.url` |
| SQL dialect | `--db-vendor` | `GSP_DB_VENDOR` | `sqlflow.db_vendor` |
| SQL file | `--sql-file` | `GSP_SQL_FILE` | `input.sql_file` |
| Inline SQL | `--sql` | `GSP_SQL_TEXT` | `input.sql_text` |
| OM server | `--om-server` | `GSP_OM_SERVER` | `openmetadata.server` |
| OM token | `--om-token` | `GSP_OM_TOKEN` | `openmetadata.token` |
| Service name | `--service-name` | `GSP_OM_SERVICE_NAME` | `openmetadata.service_name` |
| Database name | `--database-name` | `GSP_OM_DATABASE_NAME` | `openmetadata.database_name` |
| Schema name | `--schema-name` | `GSP_OM_SCHEMA_NAME` | `openmetadata.schema_name` |
| Column lineage | `--column-lineage` / `--no-column-lineage` | `GSP_COLUMN_LINEAGE` | `openmetadata.column_lineage` |

### Advanced

| Setting | CLI flag | Env var | YAML key | Notes |
|---|---|---|---|---|
| Java executable | `--java-bin` | `GSP_JAVA_BIN` | `sqlflow.java_bin` | `local_jar` only; defaults to `java` on PATH |
| Dry run | `--dry-run` | — | — | Skip the write to OpenMetadata |
| JSON output | `--json` | — | — | Print raw SQLFlow response to stdout |
| Verbose logging | `-v` / `--verbose` | — | — | Enable DEBUG-level logs |

## Input handling

The sidecar does not do SQL lineage parsing itself, but it does decide how input files are chunked before calling SQLFlow:

- **Procedural files** — if the file contains `DECLARE`, `BEGIN`, `IF … THEN`, `CALL`, `LOOP`, `EXCEPTION WHEN`, `END LOOP`, `END IF`, or `WHILE`, the entire file is sent as a single statement. Splitting on semicolons would break the procedural block.
- **Non-procedural files** — the file is split on `;` and each statement is sent to SQLFlow independently.
- **Inline SQL** (`--sql`) — always sent as a single statement.

Statement-boundary detection is heuristic: it keys off keyword presence, not full SQL parsing. If a non-procedural file has lineage issues from the split, try running it through the `--sql` path as one block.

## Troubleshooting and limitations

**`ValueError: sqlflow.user_id and sqlflow.secret_key are both required when mode is 'authenticated'`**
`authenticated` mode needs *both* credentials. Set `GSP_SQLFLOW_USER_ID` + `GSP_SQLFLOW_SECRET_KEY`, or pass `--user-id` + `--secret-key`.

**`Anonymous API rate limit exceeded`**
The anonymous tier is 50 calls/day per IP. The tool exits with code `2` on this error. Switch to `authenticated` (10k/month) or `self_hosted` (unlimited).

**`Skipping lineage: upstream table not found: …`**
That FQN does not exist in OpenMetadata. Run OpenMetadata ingestion first, or create the table entity. The sidecar keeps going and emits the other edges it *can* resolve — a single file can partially succeed.

**Multi-statement files can partially succeed.** Per-statement errors are logged and counted, but processing continues through the rest of the file. The process still exits with code `1` if any statement failed.

**Large SQL is truncated in the stored `sqlQuery`.** OpenMetadata's `lineageDetails.sqlQuery` field is capped at 10,000 characters when the sidecar builds the payload. The lineage edges themselves are unaffected.

**`local_jar: jar not found at …`**
The sidecar does not bundle the SQLFlow JAR. Point `--jar-path` (or `GSP_JAR_PATH`) at a licensed `gsqlparser-*-shaded.jar`.

**JVM cold-start with `local_jar`.** Each call spawns a fresh `java` process (~0.5–1 s overhead). Fine for ad-hoc files; not ideal for log ingestion with hundreds of statements.

## Related issues

This tool is motivated by (and has been tested against) the following upstream OpenMetadata threads:

- [#16737](https://github.com/open-metadata/OpenMetadata/issues/16737) — MSSQL stored procedure lineage not reflected
- [#25299](https://github.com/open-metadata/OpenMetadata/issues/25299) — `CREATE PROCEDURE` / `BEGIN-END` / temp tables fail
- [#16710](https://github.com/open-metadata/OpenMetadata/issues/16710) — SQL Server ingestion fails on stored procs with `%`
- [#17586](https://github.com/open-metadata/OpenMetadata/issues/17586) — MSSQL procedure lineage not picked up
- [Discussion #23717](https://github.com/open-metadata/OpenMetadata/discussions/23717) — Cross-database MSSQL lineage

## License

Apache-2.0. See [LICENSE](LICENSE).

This tool calls the [Gudu SQLFlow](https://sqlflow.gudusoft.com) service for SQL parsing. The SQLFlow service is proprietary software by [Gudu Software](https://www.gudusoft.com). See the [SQLFlow documentation](https://docs.gudusoft.com) for service terms.

## Related

- [gsp-datahub-sidecar](https://github.com/gudusoftware/gsp-datahub-sidecar) — same concept for DataHub
- [Gudu SQLFlow](https://sqlflow.gudusoft.com) — the SQL lineage engine powering this tool
- [General SQL Parser](https://sqlparser.com) — the SQL parser library by Gudu Software
