# gsp-openmetadata-sidecar

Recover SQL lineage that OpenMetadata's parser silently drops — MSSQL stored procedures, BigQuery procedural SQL, MERGE column lineage, temp-table hops, and [20+ dialects](https://www.gudusoft.com/sql-dialects/) — using [Gudu SQLFlow](https://sqlflow.gudusoft.com), then push the recovered lineage back to OpenMetadata via `PUT /api/v1/lineage`.

Optionally, [auto-create](#auto-create-missing-entities-opt-in) any missing `Database` / `DatabaseSchema` / `Table` entities before emitting lineage, so SQL referencing not-yet-ingested tables still produces complete graphs.

No changes to OpenMetadata code. Public REST API only.

## Quick start

```bash
pip install gsp-openmetadata-sidecar
gsp-openmetadata-sidecar --dry-run --sql "CREATE PROC p AS BEGIN INSERT INTO t2 SELECT a, b FROM t1 END"
```

This command:

- parses the SQL with SQLFlow's free anonymous tier
- prints the extracted lineage edges (`t1 → t2`) to stdout
- does not contact OpenMetadata
- does not require `sidecar.yaml` — if the file is absent, built-in defaults are used

If you see upstream → downstream edges, the tool works in your environment. For more realistic fixtures — MSSQL stored procedures, BigQuery procedural SQL, MERGE column lineage — see [`examples/`](examples/) in the repo:

```bash
gsp-openmetadata-sidecar --sql-file examples/mssql_stored_procedure.sql --dry-run
```

Everything below is about graduating from that first `--dry-run` to a useful live deployment.

## Choose your path

The sidecar has three onboarding paths with different prerequisites. Pick the row that matches your goal:

| Goal | What you need | Shortest command |
|---|---|---|
| Evaluate lineage extraction | A SQL file or inline SQL | `gsp-openmetadata-sidecar --sql-file proc.sql --dry-run` |
| Push lineage to an existing OM catalog | Above + `--om-server`, `--om-token`, `--service-name` (+ `--database-name` if SQL uses short names) | See [Push lineage to OpenMetadata](#push-lineage-to-openmetadata) |
| Push lineage and auto-create missing tables | Above + `--default-database`/`--default-schema` + bot with `Create` RBAC | See [Auto-create missing entities](#auto-create-missing-entities-opt-in) |

If you're new to OpenMetadata, you will also want to skim the [OpenMetadata primer](#openmetadata-primer-for-first-time-users) once — most first-time friction comes from misunderstanding how OpenMetadata identifies tables, not from bugs in this sidecar.

## When to use this

OpenMetadata uses a three-parser chain (sqlglot → sqlfluff → sqlparse via `collate-sqllineage`) that silently loses lineage on procedural and dialect-specific SQL. Use this sidecar when you see any of:

- MSSQL stored procedures (`BEGIN…END`, `DECLARE`, `CREATE PROCEDURE`)
- MSSQL square-bracket identifiers (`[dbo].[table]`)
- Temp-table intermediates (`#tempTable`), breaking multi-hop lineage
- Multi-statement scripts (`DECLARE`, `IF/THEN`, `CREATE TEMP TABLE`)
- `MERGE INTO … WHEN MATCHED … WHEN NOT MATCHED` column lineage
- Cross-database MSSQL queries
- Lineage against tables that don't exist in OpenMetadata yet (staging, ephemeral, not-yet-ingested sources)

See [Related issues](#related-issues) for upstream tickets.

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

Requirements:

- Python 3.9+
- Network access to the SQLFlow API (for `anonymous`, `authenticated`, `self_hosted` modes)
- Java 8+ and a licensed `gsqlparser-*-shaded.jar` (for `local_jar` mode only)

## Push lineage to OpenMetadata

To push lineage (instead of dry-running), you need:

1. An OpenMetadata instance with the target tables already ingested — or [`--auto-create-entities`](#auto-create-missing-entities-opt-in) enabled
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

Or via a config file. A minimal `sidecar.yaml` for a live run:

```yaml
sqlflow:
  mode: anonymous
  db_vendor: dbvmssql

openmetadata:
  server: http://localhost:8585/api
  token: "eyJ..."
  service_name: mssql_prod
  database_name: SalesDB
  schema_name: dbo
```

```bash
gsp-openmetadata-sidecar --config sidecar.yaml --sql-file stored_proc.sql
```

See [`examples/sidecar.yaml.example`](examples/sidecar.yaml.example) for the full annotated version, including `authenticated` / `self_hosted` / `local_jar` and auto-create knobs.

### The flags you'll actually type

| Flag | What it does |
|---|---|
| `--sql-file` / `--sql` | Input SQL. One file path, or an inline string. |
| `--dry-run` | Skip the write to OpenMetadata; print extracted lineage instead. |
| `--om-server` | OpenMetadata API base URL. **Must include `/api`.** |
| `--om-token` | Bot JWT token. From Settings → Bots in the OM UI. |
| `--service-name` | Database service name as registered in OM. First FQN segment. |
| `--database-name` / `--schema-name` | Fallbacks for FQN assembly when SQL uses short names. |
| `--auto-create-entities` | Opt in to creating missing `Database` / `DatabaseSchema` / `Table` before emitting. |

All other flags are listed in the [Configuration reference](#configuration-reference).

## Which defaults do I need?

The sidecar has two separate layers of defaults with similar-looking names. Confusing them is the most common first-run mistake:

| Layer | Flags | Purpose |
|---|---|---|
| **SQLFlow (parse-time)** | `--default-server` / `--default-database` / `--default-schema` | Help **SQLFlow understand the SQL**. Fed as request fields so `SELECT * FROM Customers` is parsed as `SalesDB.dbo.Customers` instead of a placeholder. |
| **Sidecar (FQN-assembly)** | `--service-name` / `--database-name` / `--schema-name` | Help the sidecar **build OpenMetadata FQNs**. Fill in missing parts when SQLFlow returns fewer than 3 segments. |

Rules of thumb:

- If your SQL uses fully-qualified names (`[SalesDB].[dbo].[Customers]`), you need only `--service-name`.
- If your SQL uses bare table names, set **both** pairs to the same values so SQLFlow parses correctly *and* the sidecar can build a valid 4-part FQN.
- `--default-*` flags are ignored in `local_jar` mode (with a warning) — the JAR-based `DataFlowAnalyzer` CLI does not accept them.

For the full mechanics, see [FQN resolution and default database/schema](#fqn-resolution-and-default-databaseschema) and [SQLFlow default qualifiers (parse-time)](#sqlflow-default-qualifiers-parse-time) below.

## Common first-run mistakes

A short list of the failure modes most new users hit:

- **`--om-server` missing the `/api` suffix.** It must be `http://host:port/api`, not just `http://host:port`.
- **Confusing `--default-database` with `--database-name`.** See the cheat-sheet above.
- **Expecting `--dry-run` to contact OpenMetadata.** It does not. Drop `--dry-run` for a live run.
- **Expecting `local_jar` mode to honor `--default-*`.** It doesn't — SQLFlow's JAR CLI doesn't accept those.
- **Expecting lineage push to create tables by itself.** It doesn't. Either ingest structure first, or pass [`--auto-create-entities`](#auto-create-missing-entities-opt-in).
- **Bot token missing `EditLineage`.** `PUT /api/v1/lineage` returns 403. Add the permission or grant the bot the `DataConsumer` role.
- **Expecting column-level lineage after an auto-create run.** Auto-created tables are intentionally skeletal (no columns), so column lineage is suppressed on edges touching them. See [Why isn't there column lineage after auto-create?](#why-isnt-there-column-lineage-after-auto-create) below.

## Common commands

```bash
# Inline SQL (no file required):
gsp-openmetadata-sidecar --sql "CREATE PROC p AS BEGIN INSERT INTO t2 SELECT a, b FROM t1 END" --dry-run

# BigQuery procedural SQL with DECLARE, CREATE TEMP TABLE:
gsp-openmetadata-sidecar --sql-file examples/bigquery_procedural.sql --db-vendor dbvbigquery --dry-run

# Table-level lineage only (skip column mappings):
gsp-openmetadata-sidecar --sql-file proc.sql --no-column-lineage --dry-run

# Print raw SQLFlow JSON response to stdout:
gsp-openmetadata-sidecar --sql-file proc.sql --json --dry-run

# Verbose/debug logging:
gsp-openmetadata-sidecar --sql-file proc.sql --dry-run -v

# Preview what entities would be auto-created (zero writes):
gsp-openmetadata-sidecar --sql-file proc.sql --auto-create-entities --dry-run

# Live run with auto-create, conservative cap, fail on any create failure:
gsp-openmetadata-sidecar --sql-file proc.sql --auto-create-entities \
  --max-entities-to-create 10 --on-create-failure abort

# Show version:
gsp-openmetadata-sidecar --version
```

## Auto-create missing entities (opt-in)

By default, the sidecar only writes lineage *edges*. If a referenced table isn't found in OpenMetadata, it logs `Skipping lineage: upstream table not found: …`, skips that edge, and continues. This is the safe baseline.

With `--auto-create-entities` (or `openmetadata.auto_create_entities: true` in YAML, or `GSP_OM_AUTO_CREATE_ENTITIES=true`), the sidecar runs a pre-pass planner, creates any missing `Database` / `DatabaseSchema` / `Table` via `POST` in strict order, and then emits the lineage. The `DatabaseService` is never auto-created.

When to reach for it:

- SQL references staging / ephemeral / not-yet-onboarded tables
- You can't wait for a metadata ingestion run to populate the catalog
- You want complete lineage graphs *now*, and are willing to accept skeletal (column-less) table entities that a connector can enrich later

### Two hard prerequisites

1. **Set parse-time defaults.** Auto-create refuses to run unless `--default-database` + `--default-schema` are set (or the `openmetadata.*` equivalents). Without them, SQLFlow can return partial identifiers that would otherwise materialize as ghost entities at non-4-part FQNs.
2. **The bot needs `Create` RBAC** on `Database`, `DatabaseSchema`, and `Table`, scoped to the configured service. See the [operator guide](docs/auto-create-operator-guide.md#required-rbac) for a recommended role shape.

### The minimum safe command

```bash
# 1. Dry-run to preview what would be created.
gsp-openmetadata-sidecar \
  --config sidecar.yaml \
  --sql-file my_etl.sql \
  --auto-create-entities \
  --dry-run

# 2. Live run with a conservative cap.
gsp-openmetadata-sidecar \
  --config sidecar.yaml \
  --sql-file my_etl.sql \
  --auto-create-entities \
  --on-create-failure abort \
  --max-entities-to-create 10
```

For safety invariants, rollout recipe, and stop-ship criteria before rolling this out on a real OpenMetadata instance, see [**`docs/auto-create-operator-guide.md`**](docs/auto-create-operator-guide.md).

### Why isn't there column lineage after auto-create?

This is the single biggest surprise on a first auto-create run, so call it out up front:

**Auto-created tables have no columns.** The sidecar POSTs them with `columns: []` because SQLFlow never connected to the source database and therefore doesn't know the column types, lengths, nullability, or ordering. Inventing column metadata on admin-curated entities would be worse than leaving the tables skeletal — an ingestion connector that later scans the source cannot reliably diff-and-merge against placeholder columns.

**Column lineage is then suppressed on every edge touching a skeletal endpoint.** You'll see this in the run summary:

```
Lineage emission complete: 2 emitted, 0 skipped (column lineage suppressed on 2 edges, 0 column-pair(s) filtered)
```

Table-level arrows appear in the OM UI as usual; the thin column-to-column lines inside them do not.

**To get column lineage back**, populate the columns on the tables and re-run the sidecar. Normal path:

1. Run OM's native structural ingestion (Airflow DAG or the Ingestion container) against the source. It fills `columns[]` on the existing skeletal entities.
2. Re-run the sidecar on the same SQL. It finds the tables already exist (via exact FQN lookup), sees they now have columns, and emits full column-level `columnsLineage`.

For a smoke test without a real source database, you can PATCH columns onto the skeletal tables directly:

```bash
curl -X PATCH "$GSP_OM_SERVER/v1/tables/name/mssql_prod.sales.dbo.invoices" \
  -H "Authorization: Bearer $GSP_OM_TOKEN" \
  -H "Content-Type: application/json-patch+json" \
  -d '[{"op":"replace","path":"/columns","value":[
        {"name":"InvoiceDate","dataType":"DATETIME"},
        {"name":"CustomerId","dataType":"BIGINT"},
        {"name":"Amount","dataType":"NUMERIC"}]}]'
```

Then re-run the sidecar (no `--auto-create-entities` needed; the tables already exist). Column casing doesn't have to match SQLFlow's output — the sidecar does case-insensitive matching via a lowercase→canonical map.

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

## OpenMetadata primer (for first-time users)

If you've never used OpenMetadata before, this section gives you the vocabulary the rest of this README assumes. Skim it before you start passing flags like `--service-name` or `--om-token`.

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

By default, lineage ingestion does not create tables. This sidecar can optionally auto-create missing `Database` / `DatabaseSchema` / `Table` entities before emission (see [Auto-create missing entities](#auto-create-missing-entities-opt-in)) — but in the default mode, if `SalesDB.dbo.Customers` doesn't already exist as a table entity, you cannot attach lineage to it.

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
  [4b] (optional) Auto-create planner — only when --auto-create-entities is set.
        |                Groups missing endpoints into a Database → Schema → Table
        |                plan, enforces a safety cap, runs a preflight probe, then
        |                POSTs each tier in strict order. Never touches existing
        |                entities; never creates a DatabaseService.
        v
  [5] Lineage push ──── sends edges to OpenMetadata via PUT /api/v1/lineage
                         with column-level detail (suppressed on edges whose
                         endpoints are skeletal / column-less; unknown-column
                         pairs are filtered pre-emit).
```

### Where do table/column names come from?

**All table and column names come from SQLFlow, not from the sidecar — and not from an LLM.** SQLFlow is a deterministic SQL parser (Gudu Soft's General SQL Parser). The sidecar's Python is a mapper/translator, not a parser.

SQLFlow's JSON response has two branches the sidecar reads:

- **`dbobjs`** — a tree of `servers → databases → schemas → tables[]/views[]`. This is SQLFlow's inferred catalog from the SQL (with `--default-server/database/schema` applied at parse time). `src/gsp_openmetadata_sidecar/lineage_mapper.py::_build_id_to_fqn` walks it to produce `{sqlflow_entity_id: "server.db.schema.table"}`.
- **`relationships[]`** — column-level flow edges, each with a `target` and `sources[]`. `lineage_mapper.py::extract_lineage` filters to persistent effects (`create_view`, `insert`, `merge`, …), resolves intermediate result-sets (`RS-*`, `MERGE-INSERT-*`) transitively back to real tables, and emits `TableLineage(upstream_table, downstream_table, column_mappings[])` dataclasses.

The `upstream_table` / `downstream_table` strings are the tables that need to exist in OpenMetadata. `emitter.py` then completes them to 4-part FQNs via `_build_fqn` (using `--service-name`, `--database-name`, `--schema-name` as fallbacks) and does `GET /api/v1/tables/name/{fqn}` to resolve them to UUIDs.

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

> **Implication for auto-creation of missing entities:** SQLFlow's JSON is authoritative for the *identity* and *hierarchy* (service/db/schema/table) of every referenced table, because those are what it parsed from the SQL. It is **not** authoritative for source-table columns, types, descriptions, or owners — SQLFlow never connected to the source database. The auto-create path therefore creates tables with an empty columns array and leaves column-level enrichment to a real ingestion connector.

### What happens when tables don't exist in OpenMetadata?

There are two modes, chosen per-run:

**Default (feature off):** the sidecar only creates lineage *edges*. If a referenced table isn't found, it logs `Skipping lineage: upstream table not found: mssql.salesdb.dbo.customers`, skips that edge, and keeps going for the rest. This is the safe baseline — nothing is ever written to the catalog beyond lineage.

**Opt-in (`--auto-create-entities`):** the sidecar runs a pre-pass planner, creates any missing `Database` / `DatabaseSchema` / `Table` via `POST` (never `PUT`, never `DatabaseService`), enforces a hard safety cap, and then emits the lineage. Column lineage is suppressed on edges touching freshly-created or column-less endpoints — the sidecar never invents columns. See [`docs/auto-create-operator-guide.md`](docs/auto-create-operator-guide.md) for safety invariants, RBAC requirements, and the rollout recipe.

In either mode, running OpenMetadata's native metadata ingestion first remains the recommended way to populate the catalog — auto-create is a fallback for tables ingestion can't see (ephemeral staging, ad-hoc sources, pre-onboarding pilots).

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

## SQLFlow default qualifiers (parse-time)

`--default-server`, `--default-database`, and `--default-schema` are sent to SQLFlow as the `defaultServer` / `defaultDatabase` / `defaultSchema` request fields. They influence how SQLFlow *parses* unqualified references — before lineage ever reaches the sidecar. This is a different layer from `--database-name` / `--schema-name`, which only fill in the OM FQN on the sidecar side. Set both pairs (or neither) depending on which layer needs the default.

| Layer | Flags | What they do |
|---|---|---|
| SQLFlow (parse-time) | `--default-server` / `--default-database` / `--default-schema` | SQLFlow populates the server/database/schema of its output tree so unqualified SQL (e.g. `SELECT * FROM Customers`) resolves to a real qualified table instead of the placeholder `DEFAULT_SERVER.DEFAULT.DEFAULT.Customers`. |
| Sidecar (FQN-assembly) | `--service-name` / `--database-name` / `--schema-name` | Sidecar builds OpenMetadata FQNs from SQLFlow's output. Fills in missing parts when SQLFlow returned fewer than 3 segments. |

Typical pairing:

```bash
gsp-openmetadata-sidecar \
  --sql "SELECT * FROM Customers" \
  --default-database SalesDB --default-schema dbo \
  --service-name mssql_prod --database-name SalesDB --schema-name dbo \
  --dry-run
```

Without the `--default-*` flags, SQLFlow returns `CUSTOMERS` as a one-part name and the sidecar fills in from `--database-name` / `--schema-name`. With them, SQLFlow's tree itself carries `SalesDB.dbo.Customers`, the mapper prefers that qualified form, and the sidecar-side defaults become load-bearing only for references SQLFlow couldn't qualify at all.

The three flags are only applied by the HTTP backends. In `local_jar` mode they are ignored with a warning, because the `DataFlowAnalyzer` CLI does not expose them.

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
| SQLFlow default server | `--default-server` | `GSP_DEFAULT_SERVER` | `sqlflow.default_server` |
| SQLFlow default database | `--default-database` | `GSP_DEFAULT_DATABASE` | `sqlflow.default_database` |
| SQLFlow default schema | `--default-schema` | `GSP_DEFAULT_SCHEMA` | `sqlflow.default_schema` |
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
| Auto-create entities | `--auto-create-entities` / `--no-auto-create-entities` | `GSP_OM_AUTO_CREATE_ENTITIES` | `openmetadata.auto_create_entities` | Opt-in; see [Auto-create missing entities](#auto-create-missing-entities-opt-in) |
| Create-failure policy | `--on-create-failure {abort,skip-edge}` | `GSP_OM_ON_CREATE_FAILURE` | `openmetadata.on_create_failure` | Default `abort`. 401/403 always fatal regardless. |
| Safety cap | `--max-entities-to-create N` | `GSP_OM_MAX_ENTITIES_TO_CREATE` | `openmetadata.max_entities_to_create` | Default 100. Plan aborts before any write if exceeded. |
| Audit tag | — | `GSP_OM_AUTO_CREATED_TAG_FQN` | `openmetadata.auto_created_tag_fqn` | Best-effort PATCH each auto-created entity with this tag. |

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
That FQN does not exist in OpenMetadata. Run OpenMetadata ingestion first, create the table entity, or re-run with `--auto-create-entities` (see [Auto-create missing entities](#auto-create-missing-entities-opt-in)). The sidecar keeps going and emits the other edges it *can* resolve — a single file can partially succeed.

**`auto_create_entities=true requires sqlflow.default_database … AND sqlflow.default_schema …`**
Auto-create refuses to run without defaults because partial SQL references (e.g. bare `customers`) would otherwise synthesize ghost entities at non-4-part FQNs. Set `--default-database` + `--default-schema` (or the `openmetadata.database_name` / `schema_name` equivalents).

**`Plan would create N entities; max_entities_to_create=M`**
The pre-pass planner counted more missing entities than `--max-entities-to-create` (default 100) allows. Re-run with `--dry-run` to review the full tree, confirm the count matches your expectation, then raise `--max-entities-to-create` explicitly.

**`OpenMetadata rejected the minimal create payload … at preflight`**
Auto-create's first write returned HTTP 400. Your OpenMetadata version may have tightened payload validation beyond what the sidecar expects. See [`docs/entity-emission-api-evidence.md`](docs/entity-emission-api-evidence.md) for the payload shapes the sidecar sends. File an issue with the OM version + 400 body.

**`Auto-create refuses foreign service 'X' (configured: 'Y')`**
A lineage FQN named an OpenMetadata service other than the one you configured. Either the SQL or an upstream emitter labeled the lineage incorrectly. The sidecar is single-service by design; run once per service.

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
