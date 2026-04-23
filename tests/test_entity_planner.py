"""Tests for the pure-function entity planner."""

from __future__ import annotations

from typing import Optional

import pytest

from gsp_openmetadata_sidecar.config import OpenMetadataConfig
from gsp_openmetadata_sidecar.entity_planner import (
    CreatePlan,
    EntityCache,
    ForeignServiceError,
    PartialFQNError,
    _split_table_fqn,
    build_plan,
)
from gsp_openmetadata_sidecar.lineage_mapper import TableLineage


class FakeClient:
    """Minimal stand-in for ``OpenMetadataClient`` that exposes a canned
    lookup table keyed on lowercased FQN. ``build_plan`` only needs
    ``_build_fqn``, ``lookup_database``, ``lookup_schema``, ``lookup_table``.
    """

    def __init__(
        self,
        service_name: str = "mssql_prod",
        database_name: str = "SalesDB",
        schema_name: str = "dbo",
        existing: Optional[dict[str, dict]] = None,
    ):
        self.service_name = service_name
        self.database_name = database_name
        self.schema_name = schema_name
        self._existing = {k.lower(): v for k, v in (existing or {}).items()}
        self.calls: list[tuple[str, str]] = []

    # Mirrors OpenMetadataClient._build_fqn signature.
    def _build_fqn(self, table_name: str) -> str:
        parts = [p.strip().strip("[]\"'`") for p in table_name.split(".")]
        if len(parts) >= 3:
            db, schema, table = parts[-3].lower(), parts[-2].lower(), parts[-1].lower()
        elif len(parts) == 2:
            db = self.database_name or ""
            schema, table = parts[-2].lower(), parts[-1].lower()
        else:
            db = self.database_name or ""
            schema = self.schema_name
            table = parts[0].lower()
        fqn_parts = [self.service_name]
        if db:
            fqn_parts.append(db)
        fqn_parts.append(schema)
        fqn_parts.append(table)
        return ".".join(fqn_parts)

    def _lookup(self, kind: str, fqn: str) -> Optional[dict]:
        self.calls.append((kind, fqn))
        return self._existing.get(fqn.lower())

    def lookup_database(self, fqn: str) -> Optional[dict]:
        return self._lookup("database", fqn)

    def lookup_schema(self, fqn: str) -> Optional[dict]:
        return self._lookup("schema", fqn)

    def lookup_table(self, fqn: str) -> Optional[dict]:
        return self._lookup("table", fqn)


def _cfg(service_name: str = "mssql_prod") -> OpenMetadataConfig:
    return OpenMetadataConfig(
        service_name=service_name,
        database_name="SalesDB",
        schema_name="dbo",
        auto_create_entities=True,
    )


# U1 — _split_table_fqn
def test_split_fqn_accepts_four_parts():
    assert _split_table_fqn("mssql_prod.SalesDB.dbo.customers") == (
        "mssql_prod", "SalesDB", "dbo", "customers",
    )


@pytest.mark.parametrize("bad", ["", "one", "a.b", "a.b.c", "a.b.c.d.e"])
def test_split_fqn_raises_on_non_four_parts(bad):
    with pytest.raises(PartialFQNError):
        _split_table_fqn(bad)


# U16 — multiple missing tables in one schema → single schema plan
def test_build_plan_fans_parents_in_once_per_fqn():
    client = FakeClient(existing={})
    lineages = [
        TableLineage(upstream_table="SalesDB.dbo.customers",
                     downstream_table="SalesDB.dbo.orders"),
        TableLineage(upstream_table="SalesDB.dbo.orders",
                     downstream_table="SalesDB.dbo.line_items"),
        TableLineage(upstream_table="SalesDB.dbo.customers",
                     downstream_table="SalesDB.dbo.addresses"),
    ]
    plan = build_plan(lineages, client, _cfg())
    assert len(plan.databases) == 1
    # SQLFlow-derived parts are lowercased by _build_fqn; config service name
    # is preserved.
    assert plan.databases[0].fqn == "mssql_prod.salesdb"
    assert len(plan.schemas) == 1
    assert plan.schemas[0].fqn == "mssql_prod.salesdb.dbo"
    # Four unique tables referenced → four table plans (customers, orders,
    # line_items, addresses).
    table_names = sorted(tp.name for tp in plan.tables)
    assert table_names == ["addresses", "customers", "line_items", "orders"]


# U17 — all-existing → empty plan, no skeletal flags
def test_build_plan_all_existing():
    existing = {
        "mssql_prod.SalesDB": {"id": "d1", "fullyQualifiedName": "mssql_prod.SalesDB"},
        "mssql_prod.SalesDB.dbo": {"id": "s1", "fullyQualifiedName": "mssql_prod.SalesDB.dbo"},
        "mssql_prod.SalesDB.dbo.customers": {
            "id": "t1",
            "fullyQualifiedName": "mssql_prod.SalesDB.dbo.customers",
            "columns": [{"name": "id"}],
        },
        "mssql_prod.SalesDB.dbo.orders": {
            "id": "t2",
            "fullyQualifiedName": "mssql_prod.SalesDB.dbo.orders",
            "columns": [{"name": "id"}],
        },
    }
    client = FakeClient(existing=existing)
    lineages = [
        TableLineage(upstream_table="customers", downstream_table="orders"),
    ]
    plan = build_plan(lineages, client, _cfg())
    assert plan.databases == [] and plan.schemas == [] and plan.tables == []
    assert plan.total == 0
    assert plan.skeletal_fqns == set()


# U17b — existing table with empty columns becomes skeletal
def test_build_plan_existing_empty_columns_marked_skeletal():
    existing = {
        "mssql_prod.SalesDB": {"id": "d1", "fullyQualifiedName": "mssql_prod.SalesDB"},
        "mssql_prod.SalesDB.dbo": {"id": "s1", "fullyQualifiedName": "mssql_prod.SalesDB.dbo"},
        "mssql_prod.SalesDB.dbo.customers": {
            "id": "t1", "fullyQualifiedName": "mssql_prod.SalesDB.dbo.customers",
            "columns": [],
        },
        "mssql_prod.SalesDB.dbo.orders": {
            "id": "t2", "fullyQualifiedName": "mssql_prod.SalesDB.dbo.orders",
            "columns": [{"name": "id"}],
        },
    }
    client = FakeClient(existing=existing)
    lineages = [TableLineage(upstream_table="customers", downstream_table="orders")]
    plan = build_plan(lineages, client, _cfg())
    assert plan.tables == []
    # Cache/plan keys are lowercased to prevent case-dup regressions.
    assert "mssql_prod.salesdb.dbo.customers" in plan.skeletal_fqns


# U18 — partial FQN ends up as unresolvable
def test_build_plan_partial_fqn_is_unresolvable(monkeypatch):
    client = FakeClient(service_name="mssql_prod")
    # Break _build_fqn output by giving a name that _build_fqn wouldn't fully
    # pad because the config has no defaults — simulate the missing-default
    # path by clearing the fake client's defaults.
    client.database_name = None
    client.schema_name = ""
    lineages = [TableLineage(upstream_table="legacy.users",
                             downstream_table="legacy.users_copy")]
    plan = build_plan(lineages, client, _cfg())
    # Both endpoints became something like "mssql_prod.legacy.users" → 3 parts.
    assert plan.unresolvable
    assert all(u.fqn.startswith("mssql_prod.") for u in plan.unresolvable)
    assert plan.tables == []


# U19 — foreign service FQN is fatal (defense-in-depth guard).
#
# NOTE: OpenMetadataClient._build_fqn always prepends the configured service
# name and drops any leading SQL-server segment from a 4-part input, so real
# SQL lineage flowing through the normal emit_lineage path cannot surface a
# foreign service FQN. The guard is kept active so a future code path that
# synthesizes TableLineage with pre-canonical OM FQNs (or a bug that injects
# one) still fails fast rather than polluting a foreign service's namespace.
# This test stubs _build_fqn to simulate that injection scenario.
def test_build_plan_foreign_service_is_fatal():
    client = FakeClient()
    lineages = [TableLineage(
        upstream_table="OTHER_SVC.SalesDB.dbo.customers",
        downstream_table="mssql_prod.SalesDB.dbo.orders",
    )]
    def foreign_fqn(name: str) -> str:
        if name.startswith("OTHER_SVC"):
            return "other_svc.salesdb.dbo.customers"
        return "mssql_prod.SalesDB.dbo.orders"
    client._build_fqn = foreign_fqn  # type: ignore[method-assign]
    with pytest.raises(ForeignServiceError):
        build_plan(lineages, client, _cfg())


# Ordering invariant: strict Database → Schema → Table within the plan
def test_build_plan_preserves_first_seen_order():
    client = FakeClient(existing={})
    lineages = [
        TableLineage(upstream_table="SalesDB.dbo.a",
                     downstream_table="ReportDB.dbo.b"),
        TableLineage(upstream_table="SalesDB.dbo.a",
                     downstream_table="ReportDB.dbo.c"),
    ]
    plan = build_plan(lineages, client, _cfg())
    # SQLFlow-derived names are lowercased; first-seen order preserved.
    assert [dp.name for dp in plan.databases] == ["salesdb", "reportdb"]


# Cache population — build_plan persists canonical entities in cache so
# materialize_plan/emit don't re-GET them.
def test_build_plan_populates_cache_on_existing_hits():
    existing = {
        "mssql_prod.SalesDB": {"id": "d1", "fullyQualifiedName": "mssql_prod.SalesDB"},
        "mssql_prod.SalesDB.dbo": {"id": "s1", "fullyQualifiedName": "mssql_prod.SalesDB.dbo"},
        "mssql_prod.SalesDB.dbo.customers": {
            "id": "t1", "fullyQualifiedName": "mssql_prod.SalesDB.dbo.customers",
            "columns": [{"name": "id"}],
        },
    }
    client = FakeClient(existing=existing)
    cache = EntityCache()
    lineages = [TableLineage(upstream_table="customers", downstream_table="customers")]
    build_plan(lineages, client, _cfg(), cache=cache)
    assert cache.get_database("mssql_prod.SalesDB") is not None
    assert cache.get_table("mssql_prod.SalesDB.dbo.customers") is not None
