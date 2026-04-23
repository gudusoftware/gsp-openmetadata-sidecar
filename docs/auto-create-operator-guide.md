# Auto-create operator guide

Operator-focused notes for `--auto-create-entities`. The
[README](../README.md#auto-create-missing-entities-opt-in) covers what the
feature does and the minimum safe command; this doc is the rollout policy,
RBAC detail, and stop-ship criteria you want before enabling it on a real
OpenMetadata instance.

## Safety invariants

- **Default is off.** When `auto_create_entities=false`, behavior is
  byte-for-byte identical to the pre-feature release.
- **POST-only creation.** The sidecar never issues `PUT /v1/tables` (which
  is upsert — would overwrite admin-curated descriptions, tags, owners).
  409 responses are resolved via re-`GET` and treated as existing entities.
- **Existing entities are never mutated.** Including `columns[]` — column
  lineage against skeletal or column-less endpoints is emitted at table
  level only; unknown-column pairs are filtered pre-emit.
- **Partial FQNs are refused.** Anything less than 4 dotted parts lands in
  the dry-run `Unresolvable:` section; the affected edges are skipped.
- **Multi-service is refused (defense-in-depth).** Every FQN built from
  SQL is force-prefixed with the configured service, so ordinary input
  never cross-services. The planner still validates this invariant as a
  belt-and-suspenders guard for future direct-FQN emission paths.
- **Safety cap.** `--max-entities-to-create` (default 100) is enforced
  before the first write.

## Required RBAC

The bot user referenced by `--om-token` must carry a role with at least
the following operations on `Database`, `DatabaseSchema`, and `Table`,
scoped to the configured `DatabaseService`:

- `Create`
- `EditAll` (needed for the optional tag PATCH and — if you later enrich
  schemas via a connector — for ingestion to update the entities it owns)

The recommended shape is a dedicated classification + role named e.g.
`gsp-sidecar-auto-create` so you can audit exactly what the sidecar has
touched. When `auto_created_tag_fqn` is set the sidecar best-effort tags
each created entity with that tag FQN — see
[`examples/sidecar.yaml.example`](../examples/sidecar.yaml.example).

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `401` / `403` at preflight | Bot lacks `Create` on the target entity type | Attach a role with `Create` + `EditAll` on `database`, `databaseSchema`, `table` scoped to the configured service. |
| `OpenMetadata rejected the minimal create payload` at preflight | Payload-shape drift (new OM version tightened validation) | Check [`docs/entity-emission-api-evidence.md`](entity-emission-api-evidence.md) and file an issue. No placeholder columns are invented — the feature fails fast instead. |
| `Cannot auto-create from non-4-part FQN` warnings | `sqlflow.default_database` / `sqlflow.default_schema` unset, SQLFlow returned a partial identifier | Set both defaults (CLI: `--default-database` / `--default-schema`) or set `openmetadata.database_name` / `openmetadata.schema_name`. |
| `Plan would create N entities; max_entities_to_create=M` | Real SQL surface is larger than expected | Re-run dry-run, review the plan, raise `--max-entities-to-create` explicitly. |
| `Auto-create refuses foreign service 'X'` | A lineage FQN names a service the sidecar isn't configured for | The sidecar targets one service per run; either fix upstream lineage or run once per service. |

## Rollout recipe

1. Dry-run on a representative SQL file. Confirm plan size matches your
   mental model.
2. Live run at `--max-entities-to-create 10` on a pilot service. Inspect
   the `--- Entity materialization ---` summary and the OM UI.
3. After a full business week with no anomalies, raise the cap.
4. Broaden to more services. Keep `auto_created_tag_fqn` set so you can
   always answer "what did the sidecar create?".

## Stop-ship triggers

Any of the following indicate a plan invariant breach — disable the
feature immediately and file an issue:

- Mixed-case duplicate entities (e.g. `customers` and `Customers` in the
  same schema)
- A ghost entity at a partial FQN (fewer than 4 dotted parts)
- Any entity in a foreign service
- A pre-existing entity's admin-curated metadata (description, tags,
  owners, columns) changed by a sidecar run

Auto-create is designed so none of these can happen. If one does, the
cause is a regression in the planner or emitter, not configuration — do
not paper over it with policy changes.
