# Review: `plan-default-server-db-schema.md`

## Findings

### 1. High: the proposed `default_database` / `database_name` split does not actually work with the current emitter

The plan says users can keep SQLFlow defaults and OpenMetadata FQN defaults independent via new `sqlflow.default_database` / `sqlflow.default_schema` settings, with `openmetadata.database_name` / `openmetadata.schema_name` as separate OM-side concepts. That is not true with the current `_build_fqn()` behavior once SQLFlow starts returning 3-part names.

Relevant plan sections:

- `docs/plan-default-server-db-schema.md:72-80`
- `docs/plan-default-server-db-schema.md:88-97`
- `docs/plan-default-server-db-schema.md:121-130`

Current code:

- `src/gsp_openmetadata_sidecar/emitter.py:48-56`

Why this breaks:

- `_build_fqn()` uses `openmetadata.database_name` / `schema_name` only for 1-part and 2-part table names.
- For 3-part names, it always trusts the database/schema coming from SQLFlow.
- So if SQLFlow is given `defaultDatabase=SalesDB`, it will start returning `SALESDB.DBO.CUSTOMERS`, and `_build_fqn()` will produce `service.salesdb.dbo.customers` regardless of `openmetadata.database_name`.

Concrete example from the current code:

```text
CUSTOMERS               => svc.SalesDB_Staging.dbo.customers
DBO.CUSTOMERS           => svc.SalesDB_Staging.dbo.customers
SALESDB.DBO.CUSTOMERS   => svc.salesdb.dbo.customers
```

That means the plan's advertised escape hatch for cases like "SQL executes against `SalesDB`, but OM lookup should target `SalesDB_Staging`" is not implemented by this design.

Recommended fix:

- Either make SQLFlow defaults fully opt-in and accept that they override OM-side db/schema for qualified names, or
- add an explicit emitter rewrite rule if you truly want independent SQLFlow-side and OM-side db/schema settings.

As written, the plan promises more separation than the code path can deliver.

### 2. High: automatic fallback is a real breaking behavior change, not just an ergonomic enhancement

The plan proposes:

- `default_database <- database_name`
- `default_schema <- schema_name`

Relevant plan sections:

- `docs/plan-default-server-db-schema.md:88-104`
- `docs/plan-default-server-db-schema.md:150-155`
- `docs/plan-default-server-db-schema.md:357-364`

Current public contract:

- `README.md:218-226` explicitly says `--database-name` and `--schema-name` are not sent to SQLFlow today.

Why this matters:

- Existing users who already set `--database-name` / `--schema-name` for OM lookup will silently change SQLFlow parse-time behavior after upgrade.
- That is a semantic change to existing flags, not just support for new flags.
- The plan acknowledges the risk, but still frames the change as basically acceptable by default.

Recommended fix:

- Treat this as opt-in for v1, not implicit fallback.
- If you want fallback later, gate it behind a dedicated switch and ship that only after proving it does not regress common cases.

If backwards compatibility matters, the safer design is:

- new SQLFlow defaults are independent
- no implicit reuse of OM db/schema on first release

### 3. Medium: the empty-string escape hatch is brittle and incomplete under the current CLI override model

The plan relies on `None` vs `""` to distinguish "unset" from "explicitly suppress fallback".

Relevant plan sections:

- `docs/plan-default-server-db-schema.md:101-103`
- `docs/plan-default-server-db-schema.md:213-215`
- `docs/plan-default-server-db-schema.md:279-299`
- `docs/plan-default-server-db-schema.md:373-376`

Current CLI code:

- `src/gsp_openmetadata_sidecar/cli.py:180-185`

Problem:

- The existing OM-side CLI overrides still use truthy checks:
  - `if args.database_name:`
  - `if args.schema_name:`
- That means a CLI call like `--database-name ""` parses successfully, but the override is ignored.
- If the value came from env/YAML, the user cannot reliably clear it from the CLI.
- Once fallback is re-applied after CLI overrides, that stale OM value can still flow into `default_database` / `default_schema`.

Recommended fix:

- If you keep the empty-string design, change the existing OM-side CLI overrides to `is not None` as well, not just the new SQLFlow-side flags.
- Better still, avoid `""` as a control mechanism and use an explicit boolean or mode flag.

As written, the suppression story is too subtle for a feature that already changes existing behavior.

### 4. Medium: the proposed `local_jar` warning will likely fire for users who never asked for SQLFlow defaults

The plan says `local_jar` is out of scope, and proposes warning if any default is set.

Relevant plan sections:

- `docs/plan-default-server-db-schema.md:108-118`
- `docs/plan-default-server-db-schema.md:243-252`

Combined with the proposed fallback:

- `docs/plan-default-server-db-schema.md:202-210`
- `docs/plan-default-server-db-schema.md:292-296`

Problem:

- Today, many `local_jar` users may already set `openmetadata.database_name` / `schema_name`.
- Under the proposed fallback, those values populate `sqlflow.default_database` / `default_schema` automatically.
- The new warning would then fire on every `local_jar` run even though the user did not explicitly configure SQLFlow defaults and cannot use them in that mode anyway.

Recommended fix:

- Only warn when the new SQLFlow default settings were explicitly set by the user, or
- disable OM -> SQLFlow fallback entirely in `local_jar` mode.

Without that distinction, the warning becomes noisy and misleading.

### 5. Medium: tests should be part of this change, not deferred

The plan says tests are out of scope unless requested.

Relevant plan section:

- `docs/plan-default-server-db-schema.md:345-352`

I would push back on that. This change touches:

- config loading
- env/YAML precedence
- CLI override ordering
- payload generation for multiple backend types
- backwards-compatibility-sensitive semantics

And the repo currently has no test coverage in `tests/`.

Recommended minimum test set:

- config fallback behavior
- explicit empty-string suppression if that design stays
- no fallback for `default_server`
- payload includes/omits `defaultServer` / `defaultDatabase` / `defaultSchema` correctly
- `local_jar` warning behavior
- CLI override ordering when both OM-side and SQLFlow-side values are present

Shipping this without tests would make later regressions hard to detect.

## Open Questions / Assumptions

### 1. The plan should show proof that the two HTTP endpoints actually accept these keys

This is probably true, but the plan currently states it as fact without attaching evidence from this repo.

Recommended improvement:

- add one captured request/response example, or
- link a verified upstream example for each endpoint shape

That matters because the whole plan depends on the remote API honoring:

- `defaultServer`
- `defaultDatabase`
- `defaultSchema`

### 2. Decide explicitly whether the goal is "better defaults" or "independent SQLFlow and OM qualification"

Right now the plan tries to do both:

- automatic fallback for ergonomics
- separate knobs for rare mismatches

Given the current emitter behavior, those two goals are in tension. I would choose one explicitly:

- either optimize for ergonomics with shared semantics, or
- optimize for separation and make the SQLFlow defaults independent from day one

## Suggested Direction

If I were implementing this, I would simplify v1:

1. Add `default_server`, `default_database`, `default_schema` as explicit SQLFlow-only settings.
2. Do not auto-fallback from OM db/schema in the first release.
3. Add tests for config and payload behavior.
4. Revisit fallback only after deciding whether the emitter should preserve OM-side db/schema overrides for SQLFlow-qualified names.

That gives you the new capability without silently changing the meaning of existing flags.
