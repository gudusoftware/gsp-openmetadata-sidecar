# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Opt-in auto-creation of missing OpenMetadata entities.** Setting
  `openmetadata.auto_create_entities: true` (or passing
  `--auto-create-entities`) makes the sidecar look up each lineage endpoint
  before emitting the edge and create any missing `Database` /
  `DatabaseSchema` / `Table` via `POST` (never `PUT`; never
  `DatabaseService`). A pre-pass planner, runtime preflight probe, and hard
  `--max-entities-to-create` cap (default 100) protect against catalog
  pollution. Column lineage is suppressed (key omitted) on edges touching
  a skeletal/column-less endpoint; column-pair references to columns
  absent from the endpoint table are filtered pre-emit rather than
  materialized. Default off preserves byte-for-byte legacy behavior. See
  README §"Auto-create missing entities".
- `emit_lineage` now returns a structured `EmissionSummary` with per-tier
  created/existing counters, column-suppression counts, and unresolvable FQNs.
- New entities: `entity_planner.py` (`CreatePlan`, `EntityCache`,
  `build_plan`, error classes) and emitter methods `lookup_service`,
  `lookup_database`, `lookup_schema`, `create_database`, `create_schema`,
  `create_table`, `preflight`, `materialize_plan`, `apply_tag`.
- New config keys: `openmetadata.auto_create_entities`,
  `openmetadata.on_create_failure`, `openmetadata.max_entities_to_create`,
  `openmetadata.auto_created_tag_fqn` (+ `GSP_OM_*` env vars and matching
  CLI flags).

### Changed

- `emit_lineage` return type changed from `int` to `EmissionSummary`.
  Callers in this repo (CLI) have been updated; external callers must
  adapt.
