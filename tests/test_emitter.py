"""Unit tests for the OpenMetadata emitter (auto-create + lineage emission).

HTTP traffic is mocked with ``responses``. Planner-only behaviors live in
``test_entity_planner.py``.
"""

from __future__ import annotations

import json
import logging

import pytest
import responses

from gsp_openmetadata_sidecar.config import OpenMetadataConfig
from gsp_openmetadata_sidecar.emitter import (
    FatalRunError,
    OpenMetadataClient,
    _build_filtered_column_lineage,
    build_lineage_payload,
    emit_lineage,
)
from gsp_openmetadata_sidecar.entity_planner import (
    CapExceededError,
    CreatePlan,
    DatabasePlan,
    EmissionSummary,
    EntityCache,
    SchemaPlan,
    TablePlan,
)
from gsp_openmetadata_sidecar.lineage_mapper import TableLineage


API = "http://om.test/api"


def _cfg(**kw) -> OpenMetadataConfig:
    base = dict(
        server=API,
        token="t0",
        service_name="mssql_prod",
        database_name="SalesDB",
        schema_name="dbo",
        auto_create_entities=True,
        on_create_failure="abort",
        max_entities_to_create=100,
    )
    base.update(kw)
    return OpenMetadataConfig(**base)


def _table_entity(
    fqn: str = "mssql_prod.salesdb.dbo.customers",
    tid: str = "t1",
    columns: list[dict] | None = None,
) -> dict:
    return {
        "id": tid,
        "fullyQualifiedName": fqn,
        "name": fqn.split(".")[-1],
        "columns": columns if columns is not None else [{"name": "id"}],
    }


# ---------------------------------------------------------------------------
# U2, U3, U4, U5, U6, U7, U8 — create_table HTTP matrix
# ---------------------------------------------------------------------------

@responses.activate
def test_create_table_posts_expected_body_and_returns_201():
    """U2 + U3: minimal body matches §9.6; 201 → entity returned."""
    client = OpenMetadataClient(_cfg())
    created = _table_entity()
    responses.add(
        method=responses.POST,
        url=f"{API}/v1/tables",
        json=created,
        status=201,
    )
    result, was_existing = client.create_table("customers", "mssql_prod.salesdb.dbo")
    assert result == created
    assert was_existing is False
    req = responses.calls[0].request
    body = json.loads(req.body)
    assert body == {
        "name": "customers",
        "databaseSchema": "mssql_prod.salesdb.dbo",
        "columns": [],
    }
    assert req.headers["Authorization"] == "Bearer t0"


@responses.activate
def test_create_table_409_falls_back_to_get():
    """U4: 409 → re-GET by FQN → returned as existing."""
    client = OpenMetadataClient(_cfg())
    existing = _table_entity()
    responses.add(responses.POST, f"{API}/v1/tables", status=409,
                  json={"message": "already exists"})
    responses.add(
        responses.GET,
        f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.customers",
        json=existing, status=200,
    )
    result, was_existing = client.create_table("customers", "mssql_prod.salesdb.dbo")
    assert result == existing
    assert was_existing is True


@responses.activate
@pytest.mark.parametrize("status", [401, 403])
def test_create_table_auth_is_fatal_regardless_of_policy(status):
    """U5: 401 / 403 raise FatalRunError regardless of on_create_failure."""
    client = OpenMetadataClient(_cfg(on_create_failure="skip-edge"))
    responses.add(responses.POST, f"{API}/v1/tables", status=status,
                  json={"message": "forbidden"})
    with pytest.raises(FatalRunError) as exc:
        client.create_table("customers", "mssql_prod.salesdb.dbo")
    assert "RBAC" in str(exc.value) or "permission" in str(exc.value)


@responses.activate
def test_create_table_5xx_retries_then_succeeds():
    """U6: 503 → 503 → 200 (two retries)."""
    client = OpenMetadataClient(_cfg())
    responses.add(responses.POST, f"{API}/v1/tables", status=503, body="busy")
    responses.add(responses.POST, f"{API}/v1/tables", status=503, body="busy")
    responses.add(responses.POST, f"{API}/v1/tables", status=201,
                  json=_table_entity())
    # Mock sleep so the test doesn't actually block.
    import gsp_openmetadata_sidecar.emitter as emitter
    slept: list[float] = []
    real_sleep = emitter.time.sleep
    emitter.time.sleep = lambda s: slept.append(s)  # type: ignore[assignment]
    try:
        client.create_table("customers", "mssql_prod.salesdb.dbo")
    finally:
        emitter.time.sleep = real_sleep
    assert slept == [0.5, 2.0]
    assert len(responses.calls) == 3


@responses.activate
def test_create_table_400_with_abort_policy_raises():
    """U8: 400 + on_create_failure='abort' → raises ValueError (caller aborts)."""
    client = OpenMetadataClient(_cfg())
    responses.add(responses.POST, f"{API}/v1/tables", status=400,
                  body="missing field")
    with pytest.raises(ValueError):
        client.create_table("customers", "mssql_prod.salesdb.dbo")


# ---------------------------------------------------------------------------
# U1 covered in test_entity_planner.py
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# U9, U10, U11 — preflight matrix
# ---------------------------------------------------------------------------

@responses.activate
def test_preflight_empty_plan_is_noop():
    """U11: empty plan → no POSTs."""
    client = OpenMetadataClient(_cfg())
    responses.add(
        responses.GET,
        f"{API}/v1/services/databaseServices/name/mssql_prod",
        json={"id": "svc", "name": "mssql_prod",
              "fullyQualifiedName": "mssql_prod"},
        status=200,
    )
    cache = EntityCache()
    client.preflight(CreatePlan(), "mssql_prod", cache)
    posts = [c for c in responses.calls if c.request.method == "POST"]
    assert posts == []
    assert cache.get_service("mssql_prod") is not None


@responses.activate
def test_preflight_auth_failure_raises_fatal():
    """U9: preflight 401/403 → FatalRunError."""
    client = OpenMetadataClient(_cfg())
    responses.add(
        responses.GET,
        f"{API}/v1/services/databaseServices/name/mssql_prod",
        json={"id": "svc", "name": "mssql_prod",
              "fullyQualifiedName": "mssql_prod"},
        status=200,
    )
    responses.add(responses.POST, f"{API}/v1/tables", status=403,
                  body="forbidden")
    plan = CreatePlan(tables=[TablePlan(
        fqn="mssql_prod.salesdb.dbo.customers",
        schema_fqn="mssql_prod.salesdb.dbo",
        name="customers",
    )])
    plan.skeletal_fqns.add("mssql_prod.salesdb.dbo.customers")
    with pytest.raises(FatalRunError):
        client.preflight(plan, "mssql_prod", EntityCache())


@responses.activate
def test_preflight_400_surfaces_compatibility_message():
    """U10: preflight 400 → FatalRunError with compatibility remediation."""
    client = OpenMetadataClient(_cfg())
    responses.add(
        responses.GET,
        f"{API}/v1/services/databaseServices/name/mssql_prod",
        json={"id": "svc", "name": "mssql_prod",
              "fullyQualifiedName": "mssql_prod"},
        status=200,
    )
    responses.add(responses.POST, f"{API}/v1/tables", status=400,
                  body="schema mismatch")
    plan = CreatePlan(tables=[TablePlan(
        fqn="mssql_prod.salesdb.dbo.customers",
        schema_fqn="mssql_prod.salesdb.dbo", name="customers",
    )])
    with pytest.raises(FatalRunError) as exc:
        client.preflight(plan, "mssql_prod", EntityCache())
    assert "minimal create payload" in str(exc.value)


@responses.activate
def test_preflight_missing_service_is_fatal():
    """Service 404 → never auto-created; aborts with remediation text."""
    client = OpenMetadataClient(_cfg())
    responses.add(
        responses.GET,
        f"{API}/v1/services/databaseServices/name/mssql_prod",
        status=404, body="not found",
    )
    with pytest.raises(FatalRunError) as exc:
        client.preflight(CreatePlan(), "mssql_prod", EntityCache())
    assert "does not create services" in str(exc.value)


# ---------------------------------------------------------------------------
# U12 — case-insensitive dedup across the three tiers
# ---------------------------------------------------------------------------

@responses.activate
def test_lookup_table_hits_canonical_via_search():
    """U12: existing 'Customers' → lookup for 'customers' resolves same entity."""
    client = OpenMetadataClient(_cfg())
    responses.add(
        responses.GET,
        f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.customers",
        status=404,
    )
    responses.add(
        responses.GET,
        f"{API}/v1/search/query",
        json={"hits": {"hits": [{"_source": {
            "id": "t1",
            "fullyQualifiedName": "mssql_prod.SalesDB.dbo.Customers",
        }}]}},
        status=200,
    )
    # Follow-up re-GET to enrich columns on the canonical path.
    responses.add(
        responses.GET,
        f"{API}/v1/tables/name/mssql_prod.SalesDB.dbo.Customers",
        json={"id": "t1", "fullyQualifiedName": "mssql_prod.SalesDB.dbo.Customers",
              "columns": [{"name": "Id"}]},
        status=200,
    )
    entity = client.lookup_table("mssql_prod.salesdb.dbo.customers")
    assert entity["fullyQualifiedName"] == "mssql_prod.SalesDB.dbo.Customers"


# ---------------------------------------------------------------------------
# U13 — EntityCache invariant: keyed on canonical FQN (lowercased)
# ---------------------------------------------------------------------------

def test_entity_cache_key_is_lowercased_canonical():
    cache = EntityCache()
    cache.put_table({"id": "t1",
                     "fullyQualifiedName": "mssql_prod.SalesDB.dbo.Customers",
                     "columns": []})
    assert cache.get_table("mssql_prod.salesdb.dbo.customers") is not None
    assert cache.get_table("MSSQL_PROD.SALESDB.DBO.CUSTOMERS") is not None


# ---------------------------------------------------------------------------
# U14, U15 — column lineage suppression on skeletal endpoints
# ---------------------------------------------------------------------------

@responses.activate
def test_column_lineage_suppressed_on_auto_created_endpoint():
    """U14: edge with auto-created (skeletal) endpoint → no columnsLineage in PUT."""
    cfg = _cfg()
    # Pre-existing service; SalesDB + dbo exist; customers missing, orders exists.
    # Use 3-part inputs so _build_fqn lowercases everything consistently.
    responses.add(responses.GET,
                  f"{API}/v1/services/databaseServices/name/mssql_prod",
                  status=200,
                  json={"id": "svc", "name": "mssql_prod",
                        "fullyQualifiedName": "mssql_prod"})
    responses.add(responses.GET, f"{API}/v1/databases/name/mssql_prod.salesdb",
                  status=200,
                  json={"id": "d1", "fullyQualifiedName": "mssql_prod.salesdb"})
    responses.add(responses.GET,
                  f"{API}/v1/databaseSchemas/name/mssql_prod.salesdb.dbo",
                  status=200,
                  json={"id": "s1", "fullyQualifiedName": "mssql_prod.salesdb.dbo"})
    responses.add(responses.GET,
                  f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.customers",
                  status=404)
    # Search fallback for customers also misses.
    responses.add(responses.GET, f"{API}/v1/search/query",
                  json={"hits": {"hits": []}}, status=200)
    # Orders exists with columns.
    responses.add(responses.GET,
                  f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.orders",
                  json=_table_entity(
                      fqn="mssql_prod.salesdb.dbo.orders", tid="t2",
                      columns=[{"name": "customer_id"}],
                  ),
                  status=200)

    # Preflight: create customers → 201
    new_customers = _table_entity(tid="t1", columns=[])
    responses.add(responses.POST, f"{API}/v1/tables", status=201, json=new_customers)
    # Lineage PUT
    responses.add(responses.PUT, f"{API}/v1/lineage", status=200, json={})

    lineages = [TableLineage(
        upstream_table="SalesDB.dbo.customers",
        downstream_table="SalesDB.dbo.orders",
        column_mappings=[("id", "customer_id")],
    )]
    summary = emit_lineage(lineages, "SELECT 1", cfg, dry_run=False)
    assert summary.emitted_edges == 1
    assert summary.created_tables == 1
    assert summary.column_lineage_suppressed_edges == 1

    put_call = [c for c in responses.calls if c.request.method == "PUT"][0]
    body = json.loads(put_call.request.body)
    assert "columnsLineage" not in body["edge"]["lineageDetails"]


# ---------------------------------------------------------------------------
# existing_* counters populated from planner + 409 re-GET (verification §1)
# ---------------------------------------------------------------------------

@responses.activate
def test_existing_counters_populated_from_planner():
    """Planner-resolved existing entities must be surfaced in EmissionSummary."""
    cfg = _cfg()
    # Service + both endpoints already exist; only their schema+db pre-exist.
    existing_up = _table_entity(fqn="mssql_prod.salesdb.dbo.customers", tid="t1")
    existing_down = _table_entity(fqn="mssql_prod.salesdb.dbo.orders", tid="t2")
    responses.add(responses.GET,
                  f"{API}/v1/services/databaseServices/name/mssql_prod",
                  status=200,
                  json={"id": "svc", "name": "mssql_prod",
                        "fullyQualifiedName": "mssql_prod"})
    responses.add(responses.GET, f"{API}/v1/databases/name/mssql_prod.salesdb",
                  status=200,
                  json={"id": "d1", "fullyQualifiedName": "mssql_prod.salesdb"})
    responses.add(responses.GET,
                  f"{API}/v1/databaseSchemas/name/mssql_prod.salesdb.dbo",
                  status=200,
                  json={"id": "s1", "fullyQualifiedName": "mssql_prod.salesdb.dbo"})
    responses.add(responses.GET,
                  f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.customers",
                  json=existing_up, status=200)
    responses.add(responses.GET,
                  f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.orders",
                  json=existing_down, status=200)
    responses.add(responses.PUT, f"{API}/v1/lineage", status=200, json={})

    lineages = [TableLineage(upstream_table="SalesDB.dbo.customers",
                             downstream_table="SalesDB.dbo.orders")]
    summary = emit_lineage(lineages, "SELECT 1", cfg, dry_run=False)
    assert summary.existing_databases == 1
    assert summary.existing_schemas == 1
    assert summary.existing_tables == 2
    assert summary.created_databases == 0
    assert summary.created_schemas == 0
    assert summary.created_tables == 0
    assert summary.emitted_edges == 1


@responses.activate
def test_409_re_get_increments_existing_not_created():
    """A 409 during materialize must count as existing, never created."""
    cfg = _cfg()
    responses.add(responses.GET,
                  f"{API}/v1/services/databaseServices/name/mssql_prod",
                  status=200,
                  json={"id": "svc", "name": "mssql_prod",
                        "fullyQualifiedName": "mssql_prod"})
    # Planner doesn't find the table → queues it; then race: another client
    # creates it first, so our POST returns 409 and re-GET yields the entity.
    responses.add(responses.GET, f"{API}/v1/databases/name/mssql_prod.salesdb",
                  status=200,
                  json={"id": "d1", "fullyQualifiedName": "mssql_prod.salesdb"})
    responses.add(responses.GET,
                  f"{API}/v1/databaseSchemas/name/mssql_prod.salesdb.dbo",
                  status=200,
                  json={"id": "s1", "fullyQualifiedName": "mssql_prod.salesdb.dbo"})
    # Initial planner lookup → 404 → search miss → queued.
    responses.add(responses.GET,
                  f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.customers",
                  status=404)
    responses.add(responses.GET, f"{API}/v1/search/query",
                  json={"hits": {"hits": []}}, status=200)
    # Downstream already exists to keep the test focused on the 409 path.
    existing_down = _table_entity(fqn="mssql_prod.salesdb.dbo.orders", tid="t2")
    responses.add(responses.GET,
                  f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.orders",
                  json=existing_down, status=200)
    # Preflight POST returns 409 → re-GET returns the existing entity.
    existing_up = _table_entity(fqn="mssql_prod.salesdb.dbo.customers", tid="t1")
    responses.add(responses.POST, f"{API}/v1/tables", status=409,
                  json={"message": "race"})
    responses.add(responses.GET,
                  f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.customers",
                  json=existing_up, status=200)
    responses.add(responses.PUT, f"{API}/v1/lineage", status=200, json={})

    lineages = [TableLineage(upstream_table="SalesDB.dbo.customers",
                             downstream_table="SalesDB.dbo.orders")]
    summary = emit_lineage(lineages, "SELECT 1", cfg, dry_run=False)
    # 409 outcome should fall in the existing bucket, not created.
    assert summary.existing_tables == 2  # 1 from planner + 1 from 409
    assert summary.created_tables == 0


# ---------------------------------------------------------------------------
# U20 — cap enforcement before any write
# ---------------------------------------------------------------------------

@responses.activate
def test_cap_exceeded_aborts_before_any_post():
    cfg = _cfg(max_entities_to_create=1)
    # Nothing exists → planner queues 1 DB, 1 schema, 2 tables (4 > 1).
    responses.add(responses.GET,
                  f"{API}/v1/services/databaseServices/name/mssql_prod",
                  status=200,
                  json={"id": "svc", "name": "mssql_prod"})
    # Any database/schema/table lookup returns 404
    for url in (
        f"{API}/v1/databases/name/mssql_prod.salesdb",
        f"{API}/v1/databaseSchemas/name/mssql_prod.salesdb.dbo",
        f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.customers",
        f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.orders",
    ):
        responses.add(responses.GET, url, status=404)
    # Search fallback for all four misses.
    for _ in range(4):
        responses.add(responses.GET, f"{API}/v1/search/query",
                      json={"hits": {"hits": []}}, status=200)

    lineages = [TableLineage(upstream_table="SalesDB.dbo.customers",
                             downstream_table="SalesDB.dbo.orders")]

    with pytest.raises(CapExceededError):
        emit_lineage(lineages, "SELECT 1", cfg, dry_run=False)
    # No POSTs made.
    assert [c for c in responses.calls if c.request.method == "POST"] == []


# ---------------------------------------------------------------------------
# U21 — feature off → legacy path, no extra traffic beyond lookup + PUT
# ---------------------------------------------------------------------------

@responses.activate
def test_feature_off_legacy_path_exact_log_lines(caplog):
    """U21: feature-off path preserves every legacy log line verbatim.

    Byte-for-byte snapshot with timestamps would be flaky, so we assert
    the exact emitted message strings (the formatter adds level/time around
    them but the payload is stable across runs).
    """
    cfg = _cfg(auto_create_entities=False)
    # Upstream missing (404 + empty search), downstream exists → tests both
    # the skip-upstream warning AND the emit-lineage INFO log.
    responses.add(responses.GET,
                  f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.customers",
                  status=404)
    responses.add(responses.GET, f"{API}/v1/search/query",
                  json={"hits": {"hits": []}}, status=200)
    responses.add(responses.GET,
                  f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.orders",
                  status=404)
    responses.add(responses.GET, f"{API}/v1/search/query",
                  json={"hits": {"hits": []}}, status=200)

    lineages = [TableLineage(upstream_table="SalesDB.dbo.customers",
                             downstream_table="SalesDB.dbo.orders")]
    with caplog.at_level(logging.DEBUG):
        summary = emit_lineage(lineages, "SELECT 1", cfg, dry_run=False)

    assert summary.emitted_edges == 0
    assert summary.skipped_edges == 1

    # Every legacy-path message the operator sees, in order. "Not found in
    # OpenMetadata" comes from the search fallback; "Skipping lineage:
    # upstream table not found" is the loop-level warning. Both are part of
    # the pre-feature output — changing either is a regression.
    messages = [r.getMessage() for r in caplog.records
                if r.name.startswith("gsp_openmetadata_sidecar.emitter")]
    assert "Table not found in OpenMetadata: mssql_prod.salesdb.dbo.customers" in messages
    assert "Skipping lineage: upstream table not found: mssql_prod.salesdb.dbo.customers" in messages
    assert "Lineage emission complete: 0 emitted, 1 skipped" in messages

    # EmissionSummary replaces the bare int return, but the legacy counters
    # it reports must match what the old `int` return used to communicate.
    assert summary.column_lineage_suppressed_edges == 0
    assert summary.column_pairs_filtered == 0
    assert summary.created_databases == 0
    assert summary.created_schemas == 0
    assert summary.created_tables == 0
    assert summary.existing_databases == 0
    assert summary.existing_schemas == 0
    assert summary.existing_tables == 0


@responses.activate
def test_feature_off_legacy_emit_path_exact_log_lines(caplog):
    """U21: happy-path emit (both endpoints exist) log lines are stable."""
    cfg = _cfg(auto_create_entities=False)
    existing_up = _table_entity(
        fqn="mssql_prod.salesdb.dbo.customers", tid="t1",
        columns=[{"name": "id"}],
    )
    existing_down = _table_entity(
        fqn="mssql_prod.salesdb.dbo.orders", tid="t2",
        columns=[{"name": "customer_id"}],
    )
    responses.add(responses.GET,
                  f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.customers",
                  json=existing_up, status=200)
    responses.add(responses.GET,
                  f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.orders",
                  json=existing_down, status=200)
    responses.add(responses.PUT, f"{API}/v1/lineage", status=200, json={})

    lineages = [TableLineage(upstream_table="SalesDB.dbo.customers",
                             downstream_table="SalesDB.dbo.orders",
                             column_mappings=[("id", "customer_id")])]
    with caplog.at_level(logging.INFO):
        summary = emit_lineage(lineages, "SELECT 1", cfg, dry_run=False)

    assert summary.emitted_edges == 1
    assert summary.skipped_edges == 0
    messages = [r.getMessage() for r in caplog.records
                if r.name.startswith("gsp_openmetadata_sidecar.emitter")]
    assert "Emitted lineage: mssql_prod.salesdb.dbo.customers --> mssql_prod.salesdb.dbo.orders" in messages
    assert "Lineage emission complete: 1 emitted, 0 skipped" in messages


def test_feature_off_dry_run_log_lines(caplog):
    """U21: dry-run legacy path prints the exact pre-feature message."""
    cfg = _cfg(auto_create_entities=False, column_lineage=True)
    lineages = [TableLineage(upstream_table="SalesDB.dbo.customers",
                             downstream_table="SalesDB.dbo.orders",
                             column_mappings=[("id", "customer_id")])]
    with caplog.at_level(logging.INFO):
        summary = emit_lineage(lineages, "SELECT 1", cfg, dry_run=True)

    assert summary.emitted_edges == 1
    messages = [r.getMessage() for r in caplog.records
                if r.name.startswith("gsp_openmetadata_sidecar.emitter")]
    assert "[DRY RUN] Would emit lineage: mssql_prod.salesdb.dbo.customers --> mssql_prod.salesdb.dbo.orders (1 column mappings)" in messages
    assert "[DRY RUN]   mssql_prod.salesdb.dbo.customers.id -> mssql_prod.salesdb.dbo.orders.customer_id" in messages


# ---------------------------------------------------------------------------
# U22 — dry-run emits zero POSTs
# ---------------------------------------------------------------------------

@responses.activate
def test_dry_run_makes_zero_writes():
    cfg = _cfg()
    for url in (
        f"{API}/v1/databases/name/mssql_prod.salesdb",
        f"{API}/v1/databaseSchemas/name/mssql_prod.salesdb.dbo",
        f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.customers",
        f"{API}/v1/tables/name/mssql_prod.salesdb.dbo.orders",
    ):
        responses.add(responses.GET, url, status=404)
    for _ in range(4):
        responses.add(responses.GET, f"{API}/v1/search/query",
                      json={"hits": {"hits": []}}, status=200)

    lineages = [TableLineage(upstream_table="SalesDB.dbo.customers",
                             downstream_table="SalesDB.dbo.orders")]
    summary = emit_lineage(lineages, "SELECT 1", cfg, dry_run=True)
    assert summary.emitted_edges == 1
    writes = [c for c in responses.calls
              if c.request.method in ("POST", "PUT", "PATCH")]
    assert writes == []


# ---------------------------------------------------------------------------
# U28, U29, U30 — column-pair filter
# ---------------------------------------------------------------------------

def test_column_pair_filter_drops_unknown_pair_preserves_valid():
    """U28: one pair references unknown column → dropped; valid pair kept."""
    up = {"id": "u", "fullyQualifiedName": "a.b.c.up",
          "columns": [{"name": "Id"}, {"name": "Name"}]}
    down = {"id": "d", "fullyQualifiedName": "a.b.c.down",
            "columns": [{"name": "customer_id"}]}
    pairs = [("id", "customer_id"), ("missing_col", "customer_id")]
    payload, filtered = _build_filtered_column_lineage(
        pairs, "a.b.c.up", "a.b.c.down", up, down,
    )
    assert filtered == 1
    assert payload == [{
        "fromColumns": ["a.b.c.up.Id"],
        "toColumn": "a.b.c.down.customer_id",
    }]


def test_column_pair_filter_all_dropped_yields_empty_payload():
    """U29: every pair invalid → caller receives empty payload + accurate count."""
    up = {"id": "u", "fullyQualifiedName": "a.b.c.up", "columns": [{"name": "Id"}]}
    down = {"id": "d", "fullyQualifiedName": "a.b.c.down",
            "columns": [{"name": "customer_id"}]}
    pairs = [("x", "y"), ("z", "w")]
    payload, filtered = _build_filtered_column_lineage(
        pairs, "a.b.c.up", "a.b.c.down", up, down,
    )
    assert filtered == 2
    assert payload == []


def test_column_pair_filter_case_insensitive_column_match():
    """U30: SQLFlow cust_id ↔ OM Cust_ID → retained, counter unchanged."""
    up = {"id": "u", "fullyQualifiedName": "a.b.c.up",
          "columns": [{"name": "Cust_ID"}]}
    down = {"id": "d", "fullyQualifiedName": "a.b.c.down",
            "columns": [{"name": "customer_id"}]}
    pairs = [("cust_id", "Customer_ID")]
    payload, filtered = _build_filtered_column_lineage(
        pairs, "a.b.c.up", "a.b.c.down", up, down,
    )
    assert filtered == 0
    assert payload == [{
        "fromColumns": ["a.b.c.up.Cust_ID"],
        "toColumn": "a.b.c.down.customer_id",
    }]


# ---------------------------------------------------------------------------
# Payload builder sanity (regression coverage for existing behavior)
# ---------------------------------------------------------------------------

def test_build_lineage_payload_omits_columns_lineage_when_none():
    payload = build_lineage_payload(
        from_entity_id="u", to_entity_id="d",
        sql_query="SELECT 1", column_lineage=None,
    )
    assert "columnsLineage" not in payload["edge"]["lineageDetails"]


def test_build_lineage_payload_includes_columns_lineage_when_provided():
    payload = build_lineage_payload(
        from_entity_id="u", to_entity_id="d", sql_query="SELECT 1",
        column_lineage=[{"fromColumns": ["a"], "toColumn": "b"}],
    )
    assert payload["edge"]["lineageDetails"]["columnsLineage"] == [
        {"fromColumns": ["a"], "toColumn": "b"},
    ]
