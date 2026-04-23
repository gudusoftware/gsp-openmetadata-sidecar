# Plan: Support `defaultServer` / `defaultDatabase` / `defaultSchema` for the SQLFlow API

Status: **REVISION 2** — refined after expert review + live endpoint verification.
Author: Claude (drafted 2026-04-23, revised same day)
Scope: `gsp-openmetadata-sidecar` (this repo only)

---

## 0. Revision history

**R2 changes vs. R1 (after review in `plan-default-server-db-schema-review.md`):**

- **Dropped the OM → SQLFlow automatic fallback.** New flags are fully independent. Addresses review findings 1, 2, and 3.
- **Dropped the `""`-escape-hatch design.** No longer needed without fallback. Existing `is truthy` CLI checks remain consistent across the codebase.
- **Added `lineage_mapper.py` changes to v1 scope.** Live verification against both HTTP endpoints showed the three fields only populate `dbobjs.*.name` — `relationships[].parentName` stays bare. Forwarding the fields without updating the mapper would be a no-op for unqualified SQL (the main use case). See §3.
- **Confirmed `local_jar` CLI cannot pass the defaults.** Verified directly against `DataFlowAnalyzer.main()` in the upstream source. The underlying `Option.setDefaultServer/Database/Schema()` API exists but is programmatic-only — the `main()` method parses only `/f /d /t /o /s /i /j /text /json /traceView /log /version`. See §4.
- **Added a Pre-implementation Verification section** (§2) that pastes the exact commands used to probe the endpoints, and what was observed. This is the evidence the reviewer asked for in "Open Question 1".
- **Promoted tests from "out of scope" to part of v1.** Addresses review finding 5.

---

## 1. Background

### 1.1 What SQLFlow exposes

Gudu SQLFlow accepts three optional request parameters that influence **parse-time name resolution**:

| Parameter | Meaning |
|---|---|
| `defaultServer` | Server name SQLFlow uses to qualify references that lack a server segment (e.g. 4-part MSSQL `[server].[db].[schema].[table]`). Conceptually the SQL Server hostname / instance. |
| `defaultDatabase` | Database name used when SQL references tables with only 1 or 2 parts. |
| `defaultSchema` | Schema name used when SQL references tables with only 1 part. |

Consumed by two SQLFlow HTTP endpoints the sidecar already talks to:

- `POST /gspLive_backend/api/anonymous/lineage` — anonymous tier (JSON body)
- `POST /gspLive_backend/sqlflow/generation/sqlflow/exportFullLineageAsJson` — authenticated and self-hosted tiers (form-encoded body)

### 1.2 What the sidecar already has

Three OM-side flags that control the FQN built on the **sidecar side** for lookup in OpenMetadata:

| Flag | Env var | YAML key | Purpose today |
|---|---|---|---|
| `--service-name` | `GSP_OM_SERVICE_NAME` | `openmetadata.service_name` | First segment of OM FQN — opaque OM identifier. Default `mssql`. |
| `--database-name` | `GSP_OM_DATABASE_NAME` | `openmetadata.database_name` | Fills missing database part when `parentName` is 1- or 2-part. No default. |
| `--schema-name` | `GSP_OM_SCHEMA_NAME` | `openmetadata.schema_name` | Fills missing schema part when `parentName` is 1-part. Default `dbo`. |

Consumed only by `emitter.OpenMetadataClient._build_fqn()`. Not sent to SQLFlow.

### 1.3 Why the reviewer pushed back on reuse

The original plan proposed an automatic OM → SQLFlow fallback (`--database-name` → `defaultDatabase`, `--schema-name` → `defaultSchema`). The reviewer correctly pointed out:

- The "escape hatch" of setting separate SQLFlow defaults doesn't actually work, because `_build_fqn()` already trusts the db/schema coming from SQLFlow for 3-part names (`emitter.py:48-56`). Once SQLFlow returns qualified names, there is no sidecar-side place for `--database-name` to override them.
- Existing users with `--database-name` set would silently change SQLFlow behavior on upgrade — a semantic change to an existing flag.

**Conclusion: independent knobs only. No automatic fallback in v1.** Users who want the same value on both sides set both (one extra flag; common-case overhead is tolerable).

---

## 2. Pre-implementation verification (evidence)

Run against live endpoints using the credentials in `.env` (present locally, not checked in). Captured 2026-04-23.

### 2.1 Anonymous endpoint — unqualified SQL, defaults set

```bash
curl -sS -X POST https://api.gudusoft.com/gspLive_backend/api/anonymous/lineage \
  -H "Content-Type: application/json" \
  -d '{"sqltext":"CREATE VIEW v AS SELECT id FROM Customers",
       "dbvendor":"dbvmssql","showRelationType":"fdd",
       "defaultServer":"srv01","defaultDatabase":"SalesDB","defaultSchema":"dbo"}'
```

Observed response (trimmed):

```
dbobjs tree: srv01.SalesDB.dbo.Customers (id=4), srv01.SalesDB.dbo.v (id=10)
relationships:
  effectType=select      target.parentName=RS-1 (pid=7)  <- [parentName=CUSTOMERS   pid=4]
  effectType=create_view target.parentName=V    (pid=10) <- [parentName=RS-1        pid=7]
```

**Key observation**: `dbobjs` gets qualified (`srv01.SalesDB.dbo.Customers`), but `relationships[].parentName` stays BARE (`CUSTOMERS`, `V`). The only way to recover the qualified name at the relationship level is to join `parentId` against the `dbobjs` tree.

### 2.2 Anonymous endpoint — no defaults, qualified SQL

```bash
curl ... -d '{"sqltext":"CREATE VIEW ReportDB.dbo.v AS SELECT id FROM SalesDB.dbo.Customers", ...}'
```

```
dbobjs tree: DEFAULT_SERVER.ReportDB.dbo.v, DEFAULT_SERVER.SalesDB.dbo.Customers
relationships:
  effectType=select      target.parentName=RS-1             <- [SALESDB.DBO.CUSTOMERS]
  effectType=create_view target.parentName=REPORTDB.DBO.V   <- [RS-1]
```

**Key observation**: when SQL itself is qualified, `parentName` carries the qualified form (uppercased per SQLFlow's case-insensitive-dialect handling). This is the current code path that already works.

### 2.3 Anonymous endpoint — no defaults, unqualified SQL

Same as 2.1 but without defaults in the payload:

```
dbobjs tree: DEFAULT_SERVER.DEFAULT.DEFAULT.Customers
relationships: target.parentName=CUSTOMERS, V (bare)
```

**Key observation**: without defaults, the `dbobjs` tree contains the placeholder strings `DEFAULT_SERVER` / `DEFAULT` / `DEFAULT`. Any mapper logic that resolves `parentId` through the tree must detect and reject these placeholders (otherwise it would build garbage OM FQNs like `mssql.default.default.customers`).

### 2.4 Authenticated endpoint — same behavior

Using the token-exchange flow with `userId` + `secretKey` from `.env`:

```
code: 200
dbobjs tree: srv01.SalesDB.dbo.Customers (id=4), srv01.SalesDB.dbo.v (id=10)
relationships: same bare parentName structure as anonymous
```

**Both HTTP endpoints accept and honor the three fields identically.** Confirmed.

### 2.5 `local_jar` CLI — does not expose the three fields

Source: `/home/ubuntu/github/gsp_java/gsp_java_core/src/main/java/gudusoft/gsqlparser/dlineage/DataFlowAnalyzer.java`.

- `main()` (line 24593) parses only: `/f`, `/d`, `/t`, `/o`, `/s`, `/i`, `/j`, `/text`, `/json`, `/traceView`, `/log`, `/version`.
- The three-default constructors (lines 129 and 168) accept `defaultServer / defaultDatabase / defaultSchema` programmatically and call `option.setDefaultServer/Database/Schema()` — but `main()` uses the simpler constructor (line 24681) that does not pass them.
- No alternative `main()` in the `dlineage` package exposes the flags either (grepped `ParallelDataFlowAnalyzer`, `MetadataUtil`, `ProcessUtility`, `TableColumnUtility`, `SqlflowMetadataAnalyzer`, `SQLDepMetadataAnalyzer` — none parse default server/db/schema from argv).

**Conclusion:** `local_jar` mode cannot apply these defaults via the current CLI. Three options, in order of ambition:

1. **Accept and warn** (v1 choice). Log a warning when user sets the defaults in `local_jar` mode; run unchanged.
2. **Ship a tiny wrapper class** (future). A 50-line Java source that reads extra flags and calls the 6-arg constructor. Requires compiling a class alongside the sidecar. Out of v1 scope.
3. **Upstream patch to `DataFlowAnalyzer.main()`** (future). Out of this repo's scope.

---

## 3. Design decisions

### 3.1 New settings

All three live under `sqlflow.*` in YAML (they control the SQLFlow request), alongside existing `sqlflow.url`, `sqlflow.user_id`, etc.

| Setting | CLI flag | Env var | YAML key | Default |
|---|---|---|---|---|
| Default server | `--default-server` | `GSP_DEFAULT_SERVER` | `sqlflow.default_server` | None (omit from payload) |
| Default database | `--default-database` | `GSP_DEFAULT_DATABASE` | `sqlflow.default_database` | None (omit from payload) |
| Default schema | `--default-schema` | `GSP_DEFAULT_SCHEMA` | `sqlflow.default_schema` | None (omit from payload) |

**Independence rule:** no automatic fallback from `openmetadata.database_name` / `schema_name` / `service_name`. The existing `--database-name` / `--schema-name` flags continue to behave exactly as today — used only by `emitter._build_fqn()`, never sent to SQLFlow.

Users who want identical values on both sides set both. The common two-pair pattern:

```yaml
sqlflow:
  default_database: SalesDB
  default_schema: dbo
openmetadata:
  database_name: SalesDB
  schema_name: dbo
  service_name: mssql_prod
```

### 3.2 Why `--service-name` is not considered for `defaultServer`

Different concepts:

- OM `service_name` is an **OpenMetadata service identifier**: opaque, chosen at OM service registration time. Typical value: `mssql_prod`, `warehouse-01`.
- SQLFlow `defaultServer` is a **SQL Server hostname / instance name** used to fill 4-part `[server].[db].[schema].[table]` references. Typical value: `sqlserver01.corp`, `PROD-SQL-A`.

Reusing one for the other would silently produce wrong lineage. Keep orthogonal.

### 3.3 Mapper change: prefer `dbobjs`-resolved FQN over bare `parentName`

This is the most consequential R2 addition. Without it, forwarding the three fields is essentially a no-op for the main use case (unqualified SQL).

Current mapper (`lineage_mapper.py`):

```python
tgt_key = (tgt["parentName"], tgt["column"])
```

Proposed mapper (schematic):

```python
# Build id -> qualified name map from dbobjs. Skip placeholder segments.
id_to_fqn = _build_id_to_fqn(sqlflow_response)

def qualified_name(node):
    resolved = id_to_fqn.get(node.get("parentId"))
    return resolved or node["parentName"]

tgt_key = (qualified_name(tgt), tgt["column"])
```

Placeholder-detection rules (based on §2.3):

- Skip server segment if `name == "DEFAULT_SERVER"` (the explicit placeholder SQLFlow emits when `defaultServer` is unset).
- Skip database segment if `name == "DEFAULT"`.
- Skip schema segment if `name == "DEFAULT"`.
- A name containing only those placeholders produces no qualified form — fall back to `parentName` (status quo).

Interaction with existing `_is_intermediate()` logic: `parentId` is absent on synthetic `RS-*` / `MERGE-INSERT-*` nodes (SQLFlow doesn't register them in `dbobjs`), so `id_to_fqn.get(...)` returns None and the fallback uses `parentName`. The existing intermediate-detection then catches them by prefix. No interference expected — confirm with test 6.

### 3.4 Scope summary

| Backend | Forward to payload? | Mapper resolves via tree? |
|---|---|---|
| `anonymous` | **Yes** — JSON body key | **Yes** |
| `authenticated` | **Yes** — form field | **Yes** |
| `self_hosted` | **Yes** — form field (same endpoint as authenticated) | **Yes** |
| `local_jar` | **No** — CLI does not accept these flags; warn and skip | Still uses mapper; tree will contain `DEFAULT_*` placeholders, so mapper falls back to `parentName`. Status quo behavior. |

### 3.5 Backward compatibility

- No CLI flag renamed or removed.
- No env var or YAML key renamed or removed.
- `_build_fqn()` behavior unchanged.
- `lineage_mapper` output **may change** for users with unqualified SQL who did not set the new defaults — but only because the mapper now consults `dbobjs`, and for `DEFAULT_*` placeholders the mapper falls back to `parentName` (the existing path). Net effect on status-quo users: **identical output.** I will call this out as a tested invariant (see test 6 below).
- `local_jar` users: identical output (mapper always falls back — tree is all placeholders).

### 3.6 Rejected alternatives

- **Automatic fallback from OM flags** — rejected per §1.3 and review findings 1-3.
- **Empty-string escape hatch** — no longer needed without fallback. Rejected for simplicity.
- **Ship a Java wrapper for `local_jar`** — out of v1 scope. Revisit if user demand exists.
- **Only forward the fields, don't update the mapper** — rejected because it's a no-op for unqualified SQL (verified in §2.1).

---

## 4. Implementation plan

### 4.1 `src/gsp_openmetadata_sidecar/config.py`

Add three fields to `SQLFlowConfig`:

```python
@dataclass
class SQLFlowConfig:
    mode: str = "anonymous"
    url: Optional[str] = None
    user_id: Optional[str] = None
    secret_key: Optional[str] = None
    db_vendor: str = "dbvmssql"
    show_relation_type: str = "fdd"
    # Parse-time default qualifiers sent to SQLFlow. Independent from the
    # openmetadata.* defaults — see docs/plan-default-server-db-schema.md.
    default_server: Optional[str] = None
    default_database: Optional[str] = None
    default_schema: Optional[str] = None
    # local_jar mode only:
    jar_path: Optional[str] = None
    java_bin: str = "java"
```

Wire YAML loading (`load_config`):

```python
cfg.sqlflow.default_server   = sf.get("default_server",   cfg.sqlflow.default_server)
cfg.sqlflow.default_database = sf.get("default_database", cfg.sqlflow.default_database)
cfg.sqlflow.default_schema   = sf.get("default_schema",   cfg.sqlflow.default_schema)
```

Add env-var rows:

```python
"GSP_DEFAULT_SERVER":   ("sqlflow", "default_server"),
"GSP_DEFAULT_DATABASE": ("sqlflow", "default_database"),
"GSP_DEFAULT_SCHEMA":   ("sqlflow", "default_schema"),
```

No fallback logic. Validation block unchanged.

### 4.2 `src/gsp_openmetadata_sidecar/backend.py`

Update `SQLFlowBackend._build_payload` to forward non-empty defaults:

```python
def _build_payload(self, sql: str, db_vendor: str, **kwargs) -> dict:
    payload: dict[str, Any] = {
        "sqltext": sql,
        "dbvendor": db_vendor,
        "showRelationType": kwargs.get("show_relation_type", "fdd"),
    }
    for api_key, kw_key in (
        ("defaultServer",   "default_server"),
        ("defaultDatabase", "default_database"),
        ("defaultSchema",   "default_schema"),
    ):
        val = kwargs.get(kw_key)
        if val:  # skip None and empty string
            payload[api_key] = val
    return payload
```

Works unchanged for both JSON (anonymous) and form-encoded (authenticated / self-hosted) requests — `requests` serialises the dict correctly for both.

Update `create_backend` to warn in `local_jar` mode when the defaults are set:

```python
if config.mode == "local_jar":
    if any((config.default_server, config.default_database, config.default_schema)):
        logger.warning(
            "default_server/default_database/default_schema are not applied in "
            "local_jar mode — the DataFlowAnalyzer CLI does not accept these flags. "
            "They will be ignored. Use an HTTP backend to benefit from them."
        )
    return LocalJarBackend(...)
```

### 4.3 `src/gsp_openmetadata_sidecar/cli.py`

Add three args:

```python
parser.add_argument("--default-server",
    help="Default SQL server name SQLFlow uses to qualify 4-part MSSQL references. "
         "Independent from --service-name (which is an OM identifier).")
parser.add_argument("--default-database",
    help="Default database name SQLFlow uses to qualify 1- and 2-part references. "
         "Independent from --database-name (which fills OM FQN on the sidecar side).")
parser.add_argument("--default-schema",
    help="Default schema name SQLFlow uses to qualify 1-part references. "
         "Independent from --schema-name (which fills OM FQN on the sidecar side).")
```

Apply overrides with the existing truthy-check pattern (consistent with surrounding code):

```python
if args.default_server:
    config.sqlflow.default_server = args.default_server
if args.default_database:
    config.sqlflow.default_database = args.default_database
if args.default_schema:
    config.sqlflow.default_schema = args.default_schema
```

Pass through to backend:

```python
response = backend.get_lineage(
    sql=stmt.sql,
    db_vendor=config.sqlflow.db_vendor,
    show_relation_type=config.sqlflow.show_relation_type,
    default_server=config.sqlflow.default_server,
    default_database=config.sqlflow.default_database,
    default_schema=config.sqlflow.default_schema,
)
```

### 4.4 `src/gsp_openmetadata_sidecar/lineage_mapper.py`

Add placeholder-aware `dbobjs` → id→FQN map, wire into relationship processing.

```python
_PLACEHOLDER_SEGMENTS = {"DEFAULT_SERVER", "DEFAULT"}

def _build_id_to_fqn(sqlflow_response: dict) -> dict[str, str]:
    """Walk dbobjs.servers[].databases[].schemas[].{tables,views}[] and build
    a map from entity id to a qualified name. Placeholder segments (the ones
    SQLFlow emits when defaultServer/Database/Schema are NOT set) are dropped,
    yielding an entry identical to what parentName would produce."""
    result: dict[str, str] = {}
    dbobjs = _find_key(sqlflow_response, "dbobjs")
    if not dbobjs:
        return result
    for server in dbobjs.get("servers", []) or []:
        s_name = server.get("name") or ""
        s_keep = s_name and s_name not in _PLACEHOLDER_SEGMENTS
        for db in server.get("databases", []) or []:
            d_name = db.get("name") or ""
            d_keep = d_name and d_name not in _PLACEHOLDER_SEGMENTS
            for schema in db.get("schemas", []) or []:
                sc_name = schema.get("name") or ""
                sc_keep = sc_name and sc_name not in _PLACEHOLDER_SEGMENTS
                for ent in (schema.get("tables") or []) + (schema.get("views") or []):
                    ent_id = ent.get("id")
                    ent_name = ent.get("name") or ""
                    if not ent_id or not ent_name:
                        continue
                    parts = []
                    if s_keep: parts.append(s_name)
                    if d_keep: parts.append(d_name)
                    if sc_keep: parts.append(sc_name)
                    parts.append(ent_name)
                    result[str(ent_id)] = ".".join(parts)
    return result


def _qualified_parent_name(node: dict, id_to_fqn: dict[str, str]) -> str:
    """Prefer the id-resolved qualified name when the dbobjs tree has one;
    otherwise fall back to parentName (status quo behavior)."""
    pid = node.get("parentId")
    if pid is not None:
        resolved = id_to_fqn.get(str(pid))
        if resolved:
            return resolved
    return node["parentName"]
```

Then substitute every `["parentName"]` access in `extract_lineage` with `_qualified_parent_name(node, id_to_fqn)`. Concrete call sites in the current `extract_lineage`:

- Phase 2 reverse-map construction (both target and source loops).
- Phase 3 target/source processing.
- `resolve_sources` reads parent_name by parameter — pass qualified values in at the call sites.

Keep `_is_intermediate` unchanged: the RS-*/MERGE-*/# prefixes still need to match, and intermediates won't have a tree entry so the fallback to bare `parentName` preserves prefix detection.

### 4.5 `examples/sidecar.yaml.example`

Add to the `sqlflow:` block (independent, not commented as a fallback):

```yaml
sqlflow:
  # ... existing keys ...

  # Parse-time default qualifiers sent to SQLFlow. These populate the server
  # / database / schema in SQLFlow's output tree when the SQL itself does not
  # qualify a reference. INDEPENDENT from openmetadata.database_name /
  # schema_name / service_name below, which fill in the OM FQN on the
  # sidecar side (see docs/plan-default-server-db-schema.md).
  #
  # Note: only applied by HTTP backends (anonymous, authenticated,
  # self_hosted). In local_jar mode these are ignored with a warning because
  # DataFlowAnalyzer's CLI does not expose them.
  # default_server:   "sqlserver01"
  # default_database: "SalesDB"
  # default_schema:   "dbo"
```

### 4.6 `README.md`

- Add a new subsection *"SQLFlow default qualifiers (parse-time)"* under a new top-level heading sibling to "FQN resolution and default database/schema". Draw the distinction: `openmetadata.*` defaults are OM-side; `sqlflow.default_*` are SQLFlow-side.
- Extend the *Configuration reference / Common* table with three new rows.
- **Do NOT** change the existing *"Important: --database-name and --schema-name are NOT sent to SQLFlow"* paragraph. It is still true — the new defaults are separate.
- Document the `local_jar` limitation in the new subsection.

### 4.7 `tests/` (new)

Bootstrap pytest. Add `pyproject.toml` dev-dependency `pytest` if not already present.

Minimum test set (all files under `tests/`):

| # | Test | File | Assertion |
|---|---|---|---|
| 1 | Config YAML loads the three new keys | `test_config.py` | values appear on `SQLFlowConfig` |
| 2 | Env vars override YAML for the three new keys | `test_config.py` | precedence honored |
| 3 | No implicit fallback from `openmetadata.*` | `test_config.py` | `sqlflow.default_*` is `None` even when `openmetadata.database_name` is set |
| 4 | `_build_payload` includes the three keys when set | `test_backend.py` | payload has `defaultServer/Database/Schema` |
| 5 | `_build_payload` omits the three keys when unset | `test_backend.py` | payload lacks the keys |
| 6 | Mapper: identical output when defaults are unset (placeholder tree) | `test_lineage_mapper.py` | output matches R1 baseline recorded from `examples/mssql_stored_procedure.sql` |
| 7 | Mapper: id-resolved FQN used when tree is populated | `test_lineage_mapper.py` | synthetic SQLFlow response with populated tree yields `SalesDB.dbo.Customers` not `CUSTOMERS` |
| 8 | Mapper: placeholder segments stripped | `test_lineage_mapper.py` | tree with `DEFAULT.DEFAULT.X.T` yields `T` (not `.. .T`) |
| 9 | `local_jar` warning fires when defaults set | `test_backend.py` | `caplog` captures the WARNING from `create_backend` |
| 10 | `local_jar` warning silent when no defaults set | `test_backend.py` | no WARNING emitted |

Fixtures: canned SQLFlow JSON responses (captured from §2.1 / §2.2 probes) under `tests/fixtures/`.

---

## 5. Risks

### 5.1 Mapper behavior change is subtle

The mapper rewrite is the biggest semantic change. Test 6 is the backstop: for all existing SQL files in `examples/`, the mapper output must be byte-identical to R1. Any drift is a regression and must block merge.

Concrete risk: SQLFlow's placeholder names might be different in some edge case (different casing, null, missing field). The placeholder set `{"DEFAULT_SERVER", "DEFAULT"}` is derived from one observed response; add a test that uses an explicit empty string in the tree and falls back to `parentName` too.

### 5.2 Casing mismatch between SQLFlow output and OM storage

Once the mapper starts emitting qualified FQNs derived from `dbobjs` (which preserves the case of the input defaults — `SalesDB.dbo.Customers` — rather than uppercasing like `parentName` does), the downstream `_build_fqn()` lowercases everything anyway, so OM lookup still funnels through the case-insensitive search fallback. No functional change. Worth calling out in the README's "Case-insensitive entity matching" subsection.

### 5.3 `local_jar` parity gap

Users on `local_jar` get no benefit from these defaults. The warning is explicit, but someone reading only the new CLI flags might be surprised. Mitigation: the warning text names the specific reason (`DataFlowAnalyzer's CLI does not expose them`), and the README's `local_jar` row in the scope table makes it clear.

### 5.4 Future mapper / upstream changes

If SQLFlow changes the `dbobjs` placeholder text (e.g. `"UNKNOWN"` instead of `"DEFAULT"`), the mapper falls back to `parentName` harmlessly. If SQLFlow starts qualifying `parentName` directly when defaults are set, the id-resolved value and `parentName` would agree, so preferring the id-resolved form is still correct. Design is robust to either evolution.

---

## 6. Summary for reviewer (R2)

**What's changing in v1:**
- Three new **independent** SQLFlow-only settings: `default_server`, `default_database`, `default_schema`. Full CLI / env / YAML wiring. No automatic fallback from `openmetadata.*`.
- `_build_payload` forwards them as `defaultServer / defaultDatabase / defaultSchema` when non-empty.
- `lineage_mapper` gains a `dbobjs` → id→FQN resolver that supersedes bare `parentName` when the tree is populated (non-placeholder). Falls back to `parentName` otherwise — guaranteeing byte-identical output for users who don't set the new flags.
- `local_jar` mode: warn and no-op (CLI does not accept these flags — verified).
- Tests: 10-test minimum set seeded with canned responses captured from live endpoints.

**What's NOT changing:**
- `emitter._build_fqn()` logic.
- `--service-name` / `--database-name` / `--schema-name` semantics. They still fill the OM FQN on the sidecar side; they are NOT sent to SQLFlow.
- Any existing CLI flag, env var, or YAML key name.

**What v1 explicitly defers to v2:**
- `local_jar` parity (requires either a wrapper class or an upstream patch to `DataFlowAnalyzer.main()`).
- Independent OM-vs-SQLFlow qualification for 3-part names (requires an emitter rewrite rule; see review finding 1 — it's a real feature gap, but a separate one).

**Points I decided without needing further review:**
1. Automatic fallback — **removed**, per review findings 1-3.
2. `""`-escape hatch — **removed**, per review finding 3.
3. `local_jar` warning scope — **only explicit user settings trigger it**, automatic once fallback is gone. Per review finding 4.
4. Tests — **in scope** for v1, per review finding 5.
5. Mapper change — **added** to v1 scope based on live-endpoint evidence; without it forwarding is a no-op for the main use case.
6. Upstream evidence — **captured inline in §2** with reproducible curl commands.

If the expert agrees on this shape I'll start with `tests/` fixtures + config tests, then the mapper change (the highest-risk part), then wire config and backend, then the CLI and docs. Expected delta: ~200 lines of code across 4 files + ~300 lines of tests + fixtures.
