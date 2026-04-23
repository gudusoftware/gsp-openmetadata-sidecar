"""Pre-pass planner for opt-in auto-creation of missing OpenMetadata entities.

`build_plan` walks every unique endpoint FQN referenced by the SQL lineage,
performs exact + case-insensitive lookups across Database / DatabaseSchema /
Table, and returns an ordered `CreatePlan`. It performs **no writes**.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from .lineage_mapper import TableLineage

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .config import OpenMetadataConfig
    from .emitter import OpenMetadataClient

logger = logging.getLogger(__name__)


class PartialFQNError(ValueError):
    """Raised when an FQN has fewer/more than 4 parts and cannot be safely auto-created."""


class ForeignServiceError(RuntimeError):
    """Raised when a lineage FQN references a service other than the configured one."""


class CapExceededError(RuntimeError):
    """Raised when a CreatePlan exceeds ``max_entities_to_create``."""


def _split_table_fqn(fqn: str) -> tuple[str, str, str, str]:
    """Split an OpenMetadata table FQN into (service, database, schema, table).

    Raises ``PartialFQNError`` on non-4-part FQNs. The sidecar refuses to
    auto-create from partial identifiers because they usually indicate a
    missing ``default_database`` / ``default_schema`` setting rather than a
    legitimate reference — blindly creating ghost entities at partial FQNs
    would pollute the catalog.
    """
    parts = fqn.split(".")
    if len(parts) != 4:
        raise PartialFQNError(
            f"Cannot auto-create from non-4-part FQN: {fqn!r}. This usually "
            f"means sqlflow.default_database / sqlflow.default_schema "
            f"(or openmetadata.database_name / schema_name) is unset or the "
            f"upstream SQLFlow mapper produced an incomplete identifier."
        )
    return parts[0], parts[1], parts[2], parts[3]


@dataclass(frozen=True)
class DatabasePlan:
    fqn: str            # "mssql_prod.SalesDB"
    service_name: str
    name: str


@dataclass(frozen=True)
class SchemaPlan:
    fqn: str            # "mssql_prod.SalesDB.dbo"
    database_fqn: str
    name: str


@dataclass(frozen=True)
class TablePlan:
    fqn: str            # "mssql_prod.SalesDB.dbo.customers"
    schema_fqn: str
    name: str


@dataclass(frozen=True)
class UnresolvableFQN:
    fqn: str
    reason: str


@dataclass
class CreatePlan:
    databases: list[DatabasePlan] = field(default_factory=list)
    schemas: list[SchemaPlan] = field(default_factory=list)
    tables: list[TablePlan] = field(default_factory=list)
    # Canonical FQNs (lowercased key) found during planning. Split by tier so
    # the emission summary can report per-tier "existing" counts without
    # classifying via dot-count at runtime.
    existing_database_fqns: set[str] = field(default_factory=set)
    existing_schema_fqns: set[str] = field(default_factory=set)
    existing_table_fqns: set[str] = field(default_factory=set)
    skeletal_fqns: set[str] = field(default_factory=set)     # auto-create targets ∪ existing-empty-columns
    unresolvable: list[UnresolvableFQN] = field(default_factory=list)

    @property
    def existing_fqns(self) -> set[str]:
        """Convenience: union of all per-tier existing FQNs."""
        return (
            self.existing_database_fqns
            | self.existing_schema_fqns
            | self.existing_table_fqns
        )

    @property
    def total(self) -> int:
        return len(self.databases) + len(self.schemas) + len(self.tables)


@dataclass
class EmissionSummary:
    emitted_edges: int = 0
    skipped_edges: int = 0
    created_databases: int = 0
    created_schemas: int = 0
    created_tables: int = 0
    existing_databases: int = 0
    existing_schemas: int = 0
    existing_tables: int = 0
    failed_entities: list[tuple[str, str]] = field(default_factory=list)
    unresolvable_fqns: list[UnresolvableFQN] = field(default_factory=list)
    column_lineage_suppressed_edges: int = 0
    column_pairs_filtered: int = 0
    tag_apply_failures: int = 0


class EntityCache:
    """Cache entity lookups keyed on **returned canonical FQN** (lowercased).

    Keying on the lowercased canonical FQN — never the query FQN — prevents
    case-duplicates: a lookup for ``customers`` that resolves to
    ``Customers`` caches under ``...customers`` and a follow-up lookup for
    either spelling hits the same entry.
    """

    def __init__(self) -> None:
        self.services: dict[str, dict] = {}
        self.databases: dict[str, dict] = {}
        self.schemas: dict[str, dict] = {}
        self.tables: dict[str, Optional[dict]] = {}

    @staticmethod
    def _key(fqn: str) -> str:
        return fqn.lower()

    def put_service(self, entity: dict) -> None:
        fqn = entity.get("fullyQualifiedName") or entity.get("name") or ""
        if fqn:
            self.services[self._key(fqn)] = entity

    def get_service(self, fqn: str) -> Optional[dict]:
        return self.services.get(self._key(fqn))

    def put_database(self, entity: dict) -> None:
        fqn = entity.get("fullyQualifiedName") or ""
        if fqn:
            self.databases[self._key(fqn)] = entity

    def get_database(self, fqn: str) -> Optional[dict]:
        return self.databases.get(self._key(fqn))

    def put_schema(self, entity: dict) -> None:
        fqn = entity.get("fullyQualifiedName") or ""
        if fqn:
            self.schemas[self._key(fqn)] = entity

    def get_schema(self, fqn: str) -> Optional[dict]:
        return self.schemas.get(self._key(fqn))

    def put_table(self, entity: dict) -> None:
        fqn = entity.get("fullyQualifiedName") or ""
        if fqn:
            self.tables[self._key(fqn)] = entity

    def get_table(self, fqn: str) -> Optional[dict]:
        return self.tables.get(self._key(fqn))

    def has_table_key(self, fqn: str) -> bool:
        return self._key(fqn) in self.tables


def build_plan(
    lineages: list[TableLineage],
    client: "OpenMetadataClient",
    config: "OpenMetadataConfig",
    cache: Optional[EntityCache] = None,
) -> CreatePlan:
    """Compute the ordered Database / Schema / Table entities to auto-create.

    Pure with respect to the OpenMetadata catalog (reads only). Looks up every
    unique endpoint FQN referenced by ``lineages``; anything missing (after
    exact + case-insensitive fallback) is queued for creation, with parents
    deduped across tables.

    Partial (non-4-part) FQNs land in ``plan.unresolvable`` — the affected
    edges will be skipped by the emitter.

    Raises ``ForeignServiceError`` if any referenced FQN's service segment
    differs from ``config.service_name`` (multi-service guard). Note that
    with the current ``OpenMetadataClient._build_fqn`` implementation,
    ordinary SQL input cannot surface a foreign service segment — every
    generated FQN is prefixed with the configured service name. The guard
    is defense-in-depth against future code paths that might synthesize
    ``TableLineage`` objects carrying already-canonical OM FQNs (e.g. a
    direct-import path that bypasses ``_build_fqn``).
    """
    if cache is None:
        cache = EntityCache()
    plan = CreatePlan()

    # Collect unique endpoint FQNs in first-seen order so the resulting plan
    # is deterministic for fixtures / golden files.
    seen: set[str] = set()
    ordered_fqns: list[str] = []
    for tl in lineages:
        for ref in (tl.upstream_table, tl.downstream_table):
            full_fqn = client._build_fqn(ref)
            key = full_fqn.lower()
            if key not in seen:
                seen.add(key)
                ordered_fqns.append(full_fqn)

    queued_dbs: dict[str, DatabasePlan] = {}
    queued_schemas: dict[str, SchemaPlan] = {}
    queued_tables: dict[str, TablePlan] = {}

    for table_fqn in ordered_fqns:
        try:
            svc, db, schema, table = _split_table_fqn(table_fqn)
        except PartialFQNError as exc:
            plan.unresolvable.append(
                UnresolvableFQN(fqn=table_fqn, reason=str(exc))
            )
            continue

        if svc != config.service_name:
            raise ForeignServiceError(
                f"Auto-create refuses foreign service {svc!r} "
                f"(configured: {config.service_name!r}). "
                f"The sidecar targets a single service by design; mixed-service "
                f"lineage indicates an upstream bug or misconfiguration."
            )

        db_fqn = f"{svc}.{db}"
        schema_fqn = f"{svc}.{db}.{schema}"

        # --- Database tier ---
        if (db_fqn.lower() not in plan.existing_database_fqns
                and db_fqn.lower() not in queued_dbs):
            entity = cache.get_database(db_fqn) or client.lookup_database(db_fqn)
            if entity:
                canonical = entity.get("fullyQualifiedName") or db_fqn
                cache.put_database(entity)
                plan.existing_database_fqns.add(canonical.lower())
            else:
                queued_dbs[db_fqn.lower()] = DatabasePlan(
                    fqn=db_fqn, service_name=svc, name=db,
                )

        # --- Schema tier ---
        if (schema_fqn.lower() not in plan.existing_schema_fqns
                and schema_fqn.lower() not in queued_schemas):
            entity = cache.get_schema(schema_fqn) or client.lookup_schema(schema_fqn)
            if entity:
                canonical = entity.get("fullyQualifiedName") or schema_fqn
                cache.put_schema(entity)
                plan.existing_schema_fqns.add(canonical.lower())
            else:
                queued_schemas[schema_fqn.lower()] = SchemaPlan(
                    fqn=schema_fqn, database_fqn=db_fqn, name=schema,
                )

        # --- Table tier ---
        if (table_fqn.lower() not in plan.existing_table_fqns
                and table_fqn.lower() not in queued_tables):
            entity = cache.get_table(table_fqn) or client.lookup_table(table_fqn)
            if entity:
                canonical = entity.get("fullyQualifiedName") or table_fqn
                cache.put_table(entity)
                plan.existing_table_fqns.add(canonical.lower())
                if not entity.get("columns"):
                    plan.skeletal_fqns.add(canonical.lower())
            else:
                queued_tables[table_fqn.lower()] = TablePlan(
                    fqn=table_fqn, schema_fqn=schema_fqn, name=table,
                )
                plan.skeletal_fqns.add(table_fqn.lower())

    # Preserve insertion (and therefore hierarchy) order.
    plan.databases = list(queued_dbs.values())
    plan.schemas = list(queued_schemas.values())
    plan.tables = list(queued_tables.values())
    return plan
