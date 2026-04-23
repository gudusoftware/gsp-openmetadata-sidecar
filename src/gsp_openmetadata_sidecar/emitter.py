"""Emit lineage to OpenMetadata via the REST API (PUT /api/v1/lineage).

Optionally auto-creates missing Database / DatabaseSchema / Table entities
before emission when ``OpenMetadataConfig.auto_create_entities`` is set.
DatabaseService is never auto-created.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

import requests

from .config import OpenMetadataConfig
from .entity_planner import (
    CapExceededError,
    CreatePlan,
    DatabasePlan,
    EmissionSummary,
    EntityCache,
    ForeignServiceError,
    PartialFQNError,
    SchemaPlan,
    TablePlan,
    build_plan,
)
from .lineage_mapper import TableLineage

logger = logging.getLogger(__name__)


# Retry backoff for 5xx responses on create endpoints. Short, two retries only
# (0.5s, 2.0s) — at expected volumes (dozens, not thousands) the third failure
# is a signal, not transient noise.
_CREATE_RETRY_DELAYS = (0.5, 2.0)


class FatalRunError(RuntimeError):
    """Raised when auto-create must abort the whole run (auth, payload, service)."""


class OpenMetadataClient:
    """Thin client for OpenMetadata REST API."""

    def __init__(self, config: OpenMetadataConfig):
        self.base_url = config.server.rstrip("/")
        self.token = config.token
        self.service_name = config.service_name
        self.database_name = config.database_name
        self.schema_name = config.schema_name
        # Populated by ``preflight`` so ``materialize_plan`` can tell whether
        # the single entity preflight touched was actually created vs.
        # resolved as existing via 409 → re-GET. Keyed on lowercased FQN.
        self._preflight_outcomes: dict[str, bool] = {}

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def _build_fqn(self, table_name: str) -> str:
        """Build a fully-qualified name for OpenMetadata table lookup.

        OpenMetadata FQN format: service.database.schema.table

        SQLFlow returns names like:
          - "DB.SCHEMA.TABLE"   (3-part)
          - "SCHEMA.TABLE"      (2-part)
          - "TABLE"             (1-part)
          - "SERVER.DB.SCHEMA.TABLE" (4-part, MSSQL linked-server style)

        We fill in missing parts from config defaults. For 4-part inputs the
        leading ``SERVER`` segment is a SQL Server hostname (linked-server
        reference), NOT an OpenMetadata service name — the two live in
        different namespaces. This method deliberately discards the leading
        segment and always prepends ``self.service_name``, because an OM
        service is always configured externally; its identity cannot be
        inferred from SQL text.

        Parts from config (service, database, schema) preserve their original
        case. Parts extracted from SQLFlow output are lowercased because
        SQLFlow uppercases identifiers for case-insensitive databases like
        MSSQL, while OpenMetadata typically stores them in lowercase.

        Consequence for the multi-service guard in ``build_plan``: ordinary
        SQL input can never surface a foreign ``service`` segment through
        this method. The guard fires only when a ``TableLineage`` has been
        synthesized with a pre-canonical 4-part FQN that bypasses this
        normalization — e.g. a future bulk-import code path or a bug that
        injects OM FQNs directly. It is defense-in-depth, not runtime
        input validation.
        """
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

        # Build FQN: service.database.schema.table
        fqn_parts = [self.service_name]
        if db:
            fqn_parts.append(db)
        fqn_parts.append(schema)
        fqn_parts.append(table)

        return ".".join(fqn_parts)

    # --- Lookups -----------------------------------------------------------

    def _get_by_fqn(
        self, entity_path: str, fqn: str, fields: str | None = None,
    ) -> Optional[dict[str, Any]]:
        """Generic ``GET /v1/{entity_path}/name/{fqn}`` with graceful 404."""
        url = f"{self.base_url}/v1/{entity_path}/name/{fqn}"
        if fields:
            url += f"?fields={fields}"
        try:
            resp = requests.get(url, headers=self._headers(), timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                return None
            logger.warning(
                "Unexpected status %d looking up %s/%s: %s",
                resp.status_code, entity_path, fqn, resp.text[:200],
            )
            return None
        except requests.RequestException as e:
            logger.error("Failed to lookup %s/%s: %s", entity_path, fqn, e)
            return None

    def _search(
        self, fqn: str, index: str,
    ) -> Optional[dict[str, Any]]:
        """Case-insensitive FQN search against a specific OpenMetadata search index.

        When multiple hits share the query FQN case-insensitively, prefer the
        one whose ``fullyQualifiedName`` matches exactly (case-insensitively).
        """
        url = (f"{self.base_url}/v1/search/query"
               f"?q=fullyQualifiedName:{fqn}&index={index}&size=10")
        try:
            resp = requests.get(url, headers=self._headers(), timeout=30)
            if resp.status_code != 200:
                return None
            hits = resp.json().get("hits", {}).get("hits", [])
            if not hits:
                return None
            fqn_lower = fqn.lower()
            for hit in hits:
                source = hit.get("_source", {})
                if source.get("fullyQualifiedName", "").lower() == fqn_lower:
                    logger.debug("Search resolved %s → %s (%s)",
                                 fqn, source.get("fullyQualifiedName"), index)
                    return source
            return hits[0].get("_source")
        except (requests.RequestException, KeyError, IndexError) as e:
            logger.warning("Search fallback failed for %s (%s): %s", fqn, index, e)
            return None

    def lookup_service(self, name: str) -> Optional[dict[str, Any]]:
        """Look up a DatabaseService by name. Returns None on 404."""
        return self._get_by_fqn("services/databaseServices", name)

    def lookup_database(self, fqn: str) -> Optional[dict[str, Any]]:
        """Exact GET → on 404, case-insensitive search fallback."""
        entity = self._get_by_fqn("databases", fqn)
        if entity is not None:
            return entity
        return self._search(fqn, "database_search_index")

    def lookup_schema(self, fqn: str) -> Optional[dict[str, Any]]:
        """Exact GET → on 404, case-insensitive search fallback."""
        entity = self._get_by_fqn("databaseSchemas", fqn)
        if entity is not None:
            return entity
        return self._search(fqn, "database_schema_search_index")

    def lookup_table(self, fqn: str) -> Optional[dict[str, Any]]:
        """Look up a Table; fetch columns for the column-pair filter.

        Exact FQN lookup first (most common path), then case-insensitive
        search fallback, matching the long-standing behavior that the rest of
        the codebase already relies on for lineage emission.
        """
        url = f"{self.base_url}/v1/tables/name/{fqn}?fields=columns"
        try:
            resp = requests.get(url, headers=self._headers(), timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                # Search index result doesn't reliably carry ``columns`` — if
                # we find a hit there, re-GET by canonical FQN to pick them up.
                hit = self._search(fqn, "table_search_index")
                if not hit:
                    logger.warning("Table not found in OpenMetadata: %s", fqn)
                    return None
                canonical = hit.get("fullyQualifiedName")
                if canonical and canonical.lower() != fqn.lower():
                    enriched = self._get_by_fqn("tables", canonical, fields="columns")
                    if enriched:
                        return enriched
                return hit
            logger.warning("Unexpected status %d looking up %s: %s",
                          resp.status_code, fqn, resp.text[:200])
            return None
        except requests.RequestException as e:
            logger.error("Failed to lookup table %s: %s", fqn, e)
            return None

    # --- Creates -----------------------------------------------------------

    def _post_create(
        self,
        entity_path: str,
        body: dict,
        lookup_fqn: str,
        lookup: "callable[[str], Optional[dict]]",
    ) -> tuple[dict[str, Any], bool]:
        """POST a minimal create body with 409→re-GET and limited 5xx retry.

        Returns ``(entity, was_existing)`` where ``was_existing`` is ``True``
        iff the entity was found via the 409 → re-GET path (i.e. already
        existed in the catalog). Callers use this flag to distinguish
        ``created_*`` from ``existing_*`` counter increments.

        * ``200`` / ``201`` → return ``(body, False)``.
        * ``409`` → re-lookup by FQN; treat as existing → ``(entity, True)``.
        * ``400`` → raise ``ValueError`` so ``preflight`` can convert it to
          ``FatalRunError`` (compatibility remediation), and otherwise
          propagate to callers that apply ``on_create_failure`` policy.
        * ``401`` / ``403`` → raise ``FatalRunError`` regardless of policy
          (RBAC fixup required; no point in continuing).
        * ``5xx`` / network → retry with backoff from ``_CREATE_RETRY_DELAYS``
          then raise ``requests.RequestException``-derived error.
        """
        url = f"{self.base_url}/v1/{entity_path}"
        last_exc: Optional[Exception] = None
        # Original attempt + len(_CREATE_RETRY_DELAYS) retries.
        for attempt in range(len(_CREATE_RETRY_DELAYS) + 1):
            try:
                resp = requests.post(url, json=body, headers=self._headers(), timeout=30)
            except requests.RequestException as e:
                last_exc = e
                if attempt < len(_CREATE_RETRY_DELAYS):
                    delay = _CREATE_RETRY_DELAYS[attempt]
                    logger.warning("POST %s network error: %s — retrying in %.1fs",
                                   entity_path, e, delay)
                    time.sleep(delay)
                    continue
                raise

            if resp.status_code in (200, 201):
                return resp.json(), False
            if resp.status_code == 409:
                existing = lookup(lookup_fqn)
                if existing is None:
                    raise RuntimeError(
                        f"POST /{entity_path} returned 409 but re-GET of "
                        f"{lookup_fqn!r} returned nothing — concurrent delete?"
                    )
                logger.debug("POST /%s: %s already exists (409 → re-GET)",
                             entity_path, lookup_fqn)
                return existing, True
            if resp.status_code in (401, 403):
                raise FatalRunError(
                    f"OpenMetadata rejected create for /{entity_path} with "
                    f"HTTP {resp.status_code}. The bot token lacks permission "
                    f"to create {entity_path}. See README §Auto-create — RBAC "
                    f"for the required Create/EditAll policy on "
                    f"{entity_path} scoped to service {self.service_name!r}. "
                    f"Body: {resp.text[:200]}"
                )
            if resp.status_code == 400:
                # Propagate as ValueError so per-entity vs preflight handlers
                # can choose: preflight treats as fatal-run; post-preflight
                # materialize applies ``on_create_failure``.
                raise ValueError(
                    f"OpenMetadata rejected create for /{entity_path} with "
                    f"HTTP 400: {resp.text[:500]}"
                )
            if 500 <= resp.status_code < 600:
                last_exc = RuntimeError(
                    f"POST /{entity_path} returned HTTP {resp.status_code}: "
                    f"{resp.text[:200]}"
                )
                if attempt < len(_CREATE_RETRY_DELAYS):
                    delay = _CREATE_RETRY_DELAYS[attempt]
                    logger.warning("POST /%s: HTTP %d — retrying in %.1fs",
                                   entity_path, resp.status_code, delay)
                    time.sleep(delay)
                    continue
                raise last_exc
            raise RuntimeError(
                f"POST /{entity_path} returned unexpected status "
                f"{resp.status_code}: {resp.text[:200]}"
            )
        # Unreachable; the loop always returns or raises.
        raise last_exc if last_exc else RuntimeError("Unreachable")

    def create_database(
        self, name: str, service_name: str,
    ) -> tuple[dict[str, Any], bool]:
        """Create a Database entity. Returns ``(entity, was_existing)``."""
        body = {"name": name, "service": service_name}
        lookup_fqn = f"{service_name}.{name}"
        return self._post_create("databases", body, lookup_fqn, self.lookup_database)

    def create_schema(
        self, name: str, database_fqn: str,
    ) -> tuple[dict[str, Any], bool]:
        """Create a DatabaseSchema entity. Returns ``(entity, was_existing)``."""
        body = {"name": name, "database": database_fqn}
        lookup_fqn = f"{database_fqn}.{name}"
        return self._post_create("databaseSchemas", body, lookup_fqn, self.lookup_schema)

    def create_table(
        self, name: str, schema_fqn: str, columns: list | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Create a Table entity. Returns ``(entity, was_existing)``."""
        body = {
            "name": name,
            "databaseSchema": schema_fqn,
            "columns": columns if columns is not None else [],
        }
        lookup_fqn = f"{schema_fqn}.{name}"
        return self._post_create("tables", body, lookup_fqn, self.lookup_table)

    def apply_tag(self, entity_path: str, entity_id: str, tag_fqn: str) -> bool:
        """Best-effort PATCH to tag a just-created entity. Returns False on 4xx."""
        url = f"{self.base_url}/v1/{entity_path}/{entity_id}"
        patch = [{
            "op": "add",
            "path": "/tags/-",
            "value": {"tagFQN": tag_fqn, "labelType": "Manual",
                      "source": "Classification", "state": "Confirmed"},
        }]
        headers = {
            **self._headers(),
            "Content-Type": "application/json-patch+json",
        }
        try:
            resp = requests.patch(url, json=patch, headers=headers, timeout=30)
            if resp.status_code in (200, 201, 204):
                return True
            logger.warning(
                "Tag apply failed for %s/%s (HTTP %d): %s",
                entity_path, entity_id, resp.status_code, resp.text[:200],
            )
            return False
        except requests.RequestException as e:
            logger.warning("Tag apply error for %s/%s: %s", entity_path, entity_id, e)
            return False

    def add_lineage(self, payload: dict) -> bool:
        """Push a lineage edge to OpenMetadata.

        Uses PUT /api/v1/lineage.
        Returns True on success, False on failure.
        """
        url = f"{self.base_url}/v1/lineage"
        try:
            resp = requests.put(url, json=payload, headers=self._headers(), timeout=30)
            if resp.status_code in (200, 201):
                return True
            logger.error("Failed to add lineage (HTTP %d): %s",
                        resp.status_code, resp.text[:500])
            return False
        except requests.RequestException as e:
            logger.error("Failed to add lineage: %s", e)
            return False

    # --- Preflight + materialize ------------------------------------------

    def preflight(
        self, plan: CreatePlan, service_name: str, cache: EntityCache,
    ) -> None:
        """Fail-fast validation: service exists, first planned write succeeds.

        * Service must exist (never auto-created).
        * Executes the first planned write (DB → Schema → Table order) as the
          probe. If it succeeds the entity is cached and materialize skips it.
        * 401/403 surfaces with RBAC remediation.
        * 400 is re-raised as FatalRunError with a compatibility-remediation
          message (payload shape drift signal).
        """
        svc = cache.get_service(service_name) or self.lookup_service(service_name)
        if svc is None:
            raise FatalRunError(
                f"Database service {service_name!r} not found in OpenMetadata "
                f"at {self.base_url}. auto_create_entities does not create "
                f"services — register the service first via the OpenMetadata "
                f"UI or a native ingestion connector."
            )
        cache.put_service(svc)

        # Probe = first planned write. No-op when plan has zero creates.
        if plan.total == 0:
            return

        try:
            if plan.databases:
                first = plan.databases[0]
                entity, was_existing = self.create_database(
                    first.name, first.service_name,
                )
                cache.put_database(entity)
                self._preflight_outcomes[first.fqn.lower()] = was_existing
            elif plan.schemas:
                first = plan.schemas[0]
                entity, was_existing = self.create_schema(
                    first.name, first.database_fqn,
                )
                cache.put_schema(entity)
                self._preflight_outcomes[first.fqn.lower()] = was_existing
            else:
                first = plan.tables[0]
                entity, was_existing = self.create_table(
                    first.name, first.schema_fqn,
                )
                cache.put_table(entity)
                self._preflight_outcomes[first.fqn.lower()] = was_existing
        except ValueError as exc:
            # HTTP 400 — payload shape incompatibility. No point pressing on.
            raise FatalRunError(
                f"OpenMetadata rejected the minimal create payload at "
                f"preflight: {exc}. Your OM version may have stricter "
                f"validation than the sidecar expects. File an issue "
                f"with the OM version and the 400 response body."
            ) from exc
        # FatalRunError from _post_create (401/403) propagates as-is.

    def materialize_plan(
        self,
        plan: CreatePlan,
        config: OpenMetadataConfig,
        summary: EmissionSummary,
        cache: EntityCache,
    ) -> None:
        """Walk DB → Schema → Table in strict order. Enforces the safety cap.

        Assumes ``preflight`` has already run (so the first planned entity is
        already in ``cache``). Handles per-entity failures per
        ``config.on_create_failure``; 401/403 always fatal.
        """
        if plan.total > config.max_entities_to_create:
            raise CapExceededError(
                f"Plan would create {plan.total} entities "
                f"(databases={len(plan.databases)}, schemas={len(plan.schemas)}, "
                f"tables={len(plan.tables)}); "
                f"max_entities_to_create={config.max_entities_to_create}. "
                f"Review the --dry-run output and raise the cap explicitly "
                f"if the size is intentional."
            )

        entity_path_by_tier = {
            "database": "databases",
            "schema": "databaseSchemas",
            "table": "tables",
        }
        tag_fqn = config.auto_created_tag_fqn

        def _apply_tag(tier: str, entity: dict) -> None:
            if not tag_fqn:
                return
            eid = entity.get("id")
            if not eid:
                return
            ok = self.apply_tag(entity_path_by_tier[tier], eid, tag_fqn)
            if not ok:
                summary.tag_apply_failures += 1

        def _handle_failure(fqn: str, exc: Exception) -> None:
            summary.failed_entities.append((fqn, str(exc)))
            if config.on_create_failure == "abort":
                raise FatalRunError(
                    f"Create failed for {fqn}: {exc}. Re-run with "
                    f"--on-create-failure=skip-edge to continue past "
                    f"per-entity failures."
                ) from exc

        # preflight_outcomes records whether the single entity preflight
        # touched was actually created vs. resolved via 409 → re-GET, so
        # ``materialize_plan`` can count it into the right bucket without
        # repeating the POST.
        preflight_outcomes: dict[str, bool] = getattr(
            self, "_preflight_outcomes", {},
        )

        for dp in plan.databases:
            cached = cache.get_database(dp.fqn)
            if cached:
                was_existing = preflight_outcomes.get(dp.fqn.lower(), False)
                if was_existing:
                    summary.existing_databases += 1
                else:
                    summary.created_databases += 1
                _apply_tag("database", cached)
                continue
            try:
                entity, was_existing = self.create_database(
                    dp.name, dp.service_name,
                )
            except FatalRunError:
                raise
            except (ValueError, RuntimeError) as exc:
                _handle_failure(dp.fqn, exc)
                continue
            cache.put_database(entity)
            if was_existing:
                summary.existing_databases += 1
                logger.debug("Database %s already existed (409 → re-GET)",
                             entity.get("fullyQualifiedName") or dp.fqn)
            else:
                summary.created_databases += 1
                logger.info("Created database %s",
                            entity.get("fullyQualifiedName") or dp.fqn)
            _apply_tag("database", entity)

        for sp in plan.schemas:
            cached = cache.get_schema(sp.fqn)
            if cached:
                was_existing = preflight_outcomes.get(sp.fqn.lower(), False)
                if was_existing:
                    summary.existing_schemas += 1
                else:
                    summary.created_schemas += 1
                _apply_tag("schema", cached)
                continue
            try:
                entity, was_existing = self.create_schema(
                    sp.name, sp.database_fqn,
                )
            except FatalRunError:
                raise
            except (ValueError, RuntimeError) as exc:
                _handle_failure(sp.fqn, exc)
                continue
            cache.put_schema(entity)
            if was_existing:
                summary.existing_schemas += 1
                logger.debug("Schema %s already existed (409 → re-GET)",
                             entity.get("fullyQualifiedName") or sp.fqn)
            else:
                summary.created_schemas += 1
                logger.info("Created schema %s",
                            entity.get("fullyQualifiedName") or sp.fqn)
            _apply_tag("schema", entity)

        for tp in plan.tables:
            cached = cache.get_table(tp.fqn)
            if cached:
                was_existing = preflight_outcomes.get(tp.fqn.lower(), False)
                if was_existing:
                    summary.existing_tables += 1
                else:
                    summary.created_tables += 1
                _apply_tag("table", cached)
                continue
            try:
                entity, was_existing = self.create_table(tp.name, tp.schema_fqn)
            except FatalRunError:
                raise
            except (ValueError, RuntimeError) as exc:
                _handle_failure(tp.fqn, exc)
                continue
            cache.put_table(entity)
            if was_existing:
                summary.existing_tables += 1
                logger.debug("Table %s already existed (409 → re-GET)",
                             entity.get("fullyQualifiedName") or tp.fqn)
            else:
                summary.created_tables += 1
                logger.info("Created table %s",
                            entity.get("fullyQualifiedName") or tp.fqn)
            _apply_tag("table", entity)


# --- Payload builder (unchanged) -------------------------------------------


def build_lineage_payload(
    from_entity_id: str,
    to_entity_id: str,
    sql_query: str,
    column_lineage: list[dict] | None = None,
) -> dict:
    """Build an OpenMetadata addLineage request payload.

    See: https://github.com/open-metadata/OpenMetadata/blob/main/
         openmetadata-spec/src/main/resources/json/schema/api/lineage/addLineage.json
    """
    edge: dict[str, Any] = {
        "fromEntity": {"id": from_entity_id, "type": "table"},
        "toEntity": {"id": to_entity_id, "type": "table"},
    }

    details: dict[str, Any] = {
        "sqlQuery": sql_query[:10000],  # truncate very long SQL
        "source": "QueryLineage",
    }

    if column_lineage:
        details["columnsLineage"] = column_lineage

    edge["lineageDetails"] = details
    return {"edge": edge}


# --- Dry-run renderer -----------------------------------------------------


def render_plan(plan: CreatePlan, config: OpenMetadataConfig) -> str:
    """Render a ``CreatePlan`` as a grouped Database → Schema → Table tree."""
    lines: list[str] = []
    lines.append(
        f"[DRY RUN] Would auto-create the following entities in OpenMetadata "
        f"(service {config.service_name!r} must exist)."
    )

    by_db: dict[str, list[SchemaPlan]] = {}
    by_schema: dict[str, list[TablePlan]] = {}
    for sp in plan.schemas:
        by_db.setdefault(sp.database_fqn, []).append(sp)
    for tp in plan.tables:
        by_schema.setdefault(tp.schema_fqn, []).append(tp)

    # All database FQNs referenced by the plan — either newly queued or the
    # parents of schemas/tables whose parents already exist.
    all_db_fqns: list[str] = [dp.fqn for dp in plan.databases]
    seen = set(fqn.lower() for fqn in all_db_fqns)
    for sp in plan.schemas:
        if sp.database_fqn.lower() not in seen:
            all_db_fqns.append(sp.database_fqn)
            seen.add(sp.database_fqn.lower())
    for tp in plan.tables:
        parent_db = ".".join(tp.schema_fqn.split(".")[:2])
        if parent_db.lower() not in seen:
            all_db_fqns.append(parent_db)
            seen.add(parent_db.lower())

    queued_db_keys = {dp.fqn.lower() for dp in plan.databases}

    for db_fqn in all_db_fqns:
        db_existing = db_fqn.lower() not in queued_db_keys
        lines.append(
            f"  Database {db_fqn}"
            f"{'    (already exists)' if db_existing else ''}"
        )
        # Schemas under this database
        schema_fqns: list[str] = []
        seen_s: set[str] = set()
        for sp in by_db.get(db_fqn, []):
            if sp.fqn.lower() not in seen_s:
                schema_fqns.append(sp.fqn)
                seen_s.add(sp.fqn.lower())
        for tp in plan.tables:
            parent_db = ".".join(tp.schema_fqn.split(".")[:2])
            if parent_db == db_fqn and tp.schema_fqn.lower() not in seen_s:
                schema_fqns.append(tp.schema_fqn)
                seen_s.add(tp.schema_fqn.lower())
        queued_schema_keys = {sp.fqn.lower() for sp in plan.schemas}
        for schema_fqn in schema_fqns:
            schema_existing = schema_fqn.lower() not in queued_schema_keys
            lines.append(
                f"    Schema   {schema_fqn}"
                f"{'    (already exists)' if schema_existing else ''}"
            )
            for tp in plan.tables:
                if tp.schema_fqn == schema_fqn:
                    lines.append(f"      Table  {tp.fqn}")

    if plan.unresolvable:
        lines.append("")
        lines.append("  Unresolvable FQNs (edges will be skipped):")
        for u in plan.unresolvable:
            lines.append(f"    {u.fqn}    → {u.reason}")

    lines.append("")
    lines.append(
        f"Plan size: {len(plan.databases)} database(s), "
        f"{len(plan.schemas)} schema(s), "
        f"{len(plan.tables)} table(s)  "
        f"(safety cap: {config.max_entities_to_create})."
    )
    lines.append(
        f"Column lineage will be suppressed on {len(plan.skeletal_fqns)} "
        f"endpoint FQN(s) (auto-created or empty-column tables)."
    )
    lines.append("Dry-run complete. No entities created.")
    return "\n".join(lines)


# --- emit_lineage orchestration -------------------------------------------


def emit_lineage(
    lineages: list[TableLineage],
    sql_query: str,
    config: OpenMetadataConfig,
    dry_run: bool = False,
) -> EmissionSummary:
    """Resolve endpoints and emit lineage to OpenMetadata.

    Returns an ``EmissionSummary`` with per-entity counters. When
    ``config.auto_create_entities`` is false, the behavior is identical to
    the legacy loop modulo the structured return value — the emitted/skipped
    log lines and ``Skipping lineage: ... not found`` warnings are preserved
    byte-for-byte (§10 acceptance criterion 1).
    """
    summary = EmissionSummary()
    if not lineages:
        logger.info("Lineage emission complete: 0 emitted, 0 skipped")
        return summary

    client = OpenMetadataClient(config)

    if not config.auto_create_entities:
        # --- Legacy path: unchanged behavior (feature off). ---
        return _emit_legacy(lineages, sql_query, client, config, dry_run, summary)

    # --- Auto-create path ---
    cache = EntityCache()
    plan = build_plan(lineages, client, config, cache)
    summary.unresolvable_fqns.extend(plan.unresolvable)
    # Existing-entity counts are known at planning time — every FQN already
    # in the catalog is recorded per-tier on the plan. materialize_plan will
    # add to these for any 409 → re-GET resolutions during create.
    summary.existing_databases = len(plan.existing_database_fqns)
    summary.existing_schemas = len(plan.existing_schema_fqns)
    summary.existing_tables = len(plan.existing_table_fqns)

    if dry_run:
        for line in render_plan(plan, config).splitlines():
            logger.info("%s", line)
        # Count what emission would produce, ignoring unresolvable edges.
        unresolvable_keys = {u.fqn.lower() for u in plan.unresolvable}
        for tl in lineages:
            up = client._build_fqn(tl.upstream_table).lower()
            down = client._build_fqn(tl.downstream_table).lower()
            if up in unresolvable_keys or down in unresolvable_keys:
                summary.skipped_edges += 1
            else:
                summary.emitted_edges += 1
        return summary

    # Cap check happens BEFORE preflight because preflight itself performs
    # the first planned write — we must never touch the catalog when the
    # plan exceeds the operator-allowed blast radius.
    if plan.total > config.max_entities_to_create:
        raise CapExceededError(
            f"Plan would create {plan.total} entities "
            f"(databases={len(plan.databases)}, schemas={len(plan.schemas)}, "
            f"tables={len(plan.tables)}); "
            f"max_entities_to_create={config.max_entities_to_create}. "
            f"Review the --dry-run output and raise the cap explicitly "
            f"if the size is intentional."
        )

    client.preflight(plan, config.service_name, cache)
    client.materialize_plan(plan, config, summary, cache)

    _emit_edges(lineages, sql_query, client, cache, config, plan, summary)
    logger.info(
        "Lineage emission complete: %d emitted, %d skipped "
        "(column lineage suppressed on %d edges, %d column-pair(s) filtered)",
        summary.emitted_edges, summary.skipped_edges,
        summary.column_lineage_suppressed_edges, summary.column_pairs_filtered,
    )
    return summary


def _emit_legacy(
    lineages: list[TableLineage],
    sql_query: str,
    client: OpenMetadataClient,
    config: OpenMetadataConfig,
    dry_run: bool,
    summary: EmissionSummary,
) -> EmissionSummary:
    """Pre-feature emit_lineage loop, preserved byte-for-byte for feature-off."""
    fqn_cache: dict[str, Optional[dict]] = {}
    emitted = 0
    skipped = 0

    for tl in lineages:
        upstream_fqn = client._build_fqn(tl.upstream_table)
        downstream_fqn = client._build_fqn(tl.downstream_table)

        if dry_run:
            col_count = len(tl.column_mappings) if config.column_lineage else 0
            logger.info("[DRY RUN] Would emit lineage: %s --> %s (%d column mappings)",
                       upstream_fqn, downstream_fqn, col_count)
            if config.column_lineage:
                for src_col, tgt_col in tl.column_mappings[:5]:
                    logger.info("[DRY RUN]   %s.%s -> %s.%s",
                               upstream_fqn, src_col.lower(), downstream_fqn, tgt_col.lower())
                if len(tl.column_mappings) > 5:
                    logger.info("[DRY RUN]   ... and %d more", len(tl.column_mappings) - 5)
            emitted += 1
            continue

        for fqn in (upstream_fqn, downstream_fqn):
            if fqn not in fqn_cache:
                fqn_cache[fqn] = client.lookup_table(fqn)

        upstream_entity = fqn_cache.get(upstream_fqn)
        downstream_entity = fqn_cache.get(downstream_fqn)

        if not upstream_entity:
            logger.warning("Skipping lineage: upstream table not found: %s", upstream_fqn)
            skipped += 1
            continue
        if not downstream_entity:
            logger.warning("Skipping lineage: downstream table not found: %s", downstream_fqn)
            skipped += 1
            continue

        canonical_up = upstream_entity.get("fullyQualifiedName", upstream_fqn)
        canonical_down = downstream_entity.get("fullyQualifiedName", downstream_fqn)
        col_lineage = None
        if config.column_lineage and tl.column_mappings:
            col_lineage = _build_column_lineage(
                tl.column_mappings, canonical_up, canonical_down,
                upstream_entity, downstream_entity,
            )

        payload = build_lineage_payload(
            from_entity_id=upstream_entity["id"],
            to_entity_id=downstream_entity["id"],
            sql_query=sql_query,
            column_lineage=col_lineage,
        )

        if client.add_lineage(payload):
            logger.info("Emitted lineage: %s --> %s", upstream_fqn, downstream_fqn)
            emitted += 1
        else:
            skipped += 1

    logger.info("Lineage emission complete: %d emitted, %d skipped", emitted, skipped)
    summary.emitted_edges = emitted
    summary.skipped_edges = skipped
    return summary


def _emit_edges(
    lineages: list[TableLineage],
    sql_query: str,
    client: OpenMetadataClient,
    cache: EntityCache,
    config: OpenMetadataConfig,
    plan: CreatePlan,
    summary: EmissionSummary,
) -> None:
    """Emit lineage edges for the auto-create path.

    Uses the shared ``cache`` populated by ``build_plan`` and
    ``materialize_plan``. Skips edges whose endpoints are ``unresolvable`` or
    failed to create; suppresses ``columnsLineage`` on any edge touching a
    skeletal endpoint; filters ``columnsLineage`` pairs that reference
    columns absent from the resolved ``Table.columns[]`` (§4.3).
    """
    unresolvable_keys = {u.fqn.lower() for u in plan.unresolvable}
    failed_keys = {fqn.lower() for fqn, _reason in summary.failed_entities}

    for tl in lineages:
        upstream_fqn = client._build_fqn(tl.upstream_table)
        downstream_fqn = client._build_fqn(tl.downstream_table)

        for fqn in (upstream_fqn, downstream_fqn):
            if not cache.has_table_key(fqn) and fqn.lower() not in unresolvable_keys:
                # Not seen by the planner (shouldn't happen) or lookup missed —
                # try an exact GET so the edge still has a chance.
                entity = client.lookup_table(fqn)
                if entity:
                    cache.put_table(entity)

        if upstream_fqn.lower() in unresolvable_keys:
            logger.warning("Skipping lineage: upstream FQN unresolvable: %s",
                           upstream_fqn)
            summary.skipped_edges += 1
            continue
        if downstream_fqn.lower() in unresolvable_keys:
            logger.warning("Skipping lineage: downstream FQN unresolvable: %s",
                           downstream_fqn)
            summary.skipped_edges += 1
            continue

        upstream_entity = cache.get_table(upstream_fqn)
        downstream_entity = cache.get_table(downstream_fqn)

        if not upstream_entity:
            logger.warning("Skipping lineage: upstream table not found: %s",
                           upstream_fqn)
            summary.skipped_edges += 1
            continue
        if not downstream_entity:
            logger.warning("Skipping lineage: downstream table not found: %s",
                           downstream_fqn)
            summary.skipped_edges += 1
            continue

        up_canonical = upstream_entity.get("fullyQualifiedName", upstream_fqn)
        down_canonical = downstream_entity.get("fullyQualifiedName", downstream_fqn)

        if (up_canonical.lower() in failed_keys
                or down_canonical.lower() in failed_keys):
            logger.warning(
                "Skipping lineage: endpoint create failed: %s → %s",
                up_canonical, down_canonical,
            )
            summary.skipped_edges += 1
            continue

        col_lineage: Optional[list[dict]] = None
        skeletal_edge = (
            up_canonical.lower() in plan.skeletal_fqns
            or down_canonical.lower() in plan.skeletal_fqns
        )

        if config.column_lineage and tl.column_mappings:
            if skeletal_edge:
                summary.column_lineage_suppressed_edges += 1
                logger.debug(
                    "Suppressing columnsLineage on %s → %s (skeletal endpoint)",
                    up_canonical, down_canonical,
                )
            else:
                col_lineage, filtered = _build_filtered_column_lineage(
                    tl.column_mappings, up_canonical, down_canonical,
                    upstream_entity, downstream_entity,
                )
                summary.column_pairs_filtered += filtered
                if not col_lineage:
                    # All pairs filtered away — omit the key entirely.
                    if tl.column_mappings:
                        summary.column_lineage_suppressed_edges += 1
                    col_lineage = None

        payload = build_lineage_payload(
            from_entity_id=upstream_entity["id"],
            to_entity_id=downstream_entity["id"],
            sql_query=sql_query,
            column_lineage=col_lineage,
        )

        if client.add_lineage(payload):
            logger.info("Emitted lineage: %s --> %s", up_canonical, down_canonical)
            summary.emitted_edges += 1
        else:
            summary.skipped_edges += 1


# --- Column lineage helpers ------------------------------------------------


def _build_column_name_map(entity: dict) -> dict[str, str]:
    """Build a lowercase->canonical column name map from an OM entity.

    Returns e.g. {"invoicedate": "InvoiceDate", "customerid": "CustomerId"}.
    """
    result: dict[str, str] = {}
    for col in entity.get("columns", []):
        name = col.get("name", "")
        result[name.lower()] = name
    return result


def _build_column_lineage(
    column_mappings: list[tuple[str, str]],
    upstream_fqn: str,
    downstream_fqn: str,
    upstream_entity: Optional[dict] = None,
    downstream_entity: Optional[dict] = None,
) -> list[dict]:
    """Build OpenMetadata columnsLineage array from column mapping pairs.

    OpenMetadata format:
      [{"fromColumns": ["service.db.schema.table.col"], "toColumn": "service.db.schema.table.col"}]

    Column names from SQLFlow are normalized to lowercase, then resolved to
    canonical names from the OM entity when available. Does NOT filter unknown
    columns — use ``_build_filtered_column_lineage`` in the auto-create path.
    """
    up_cols = _build_column_name_map(upstream_entity) if upstream_entity else {}
    down_cols = _build_column_name_map(downstream_entity) if downstream_entity else {}

    target_to_sources: dict[str, list[str]] = {}
    for src_col, tgt_col in column_mappings:
        src_clean = src_col.strip().strip("[]\"'`").lower()
        tgt_clean = tgt_col.strip().strip("[]\"'`").lower()
        if src_clean == "*" or tgt_clean == "*":
            continue
        src_canonical = up_cols.get(src_clean, src_clean)
        tgt_canonical = down_cols.get(tgt_clean, tgt_clean)
        src_fqn = f"{upstream_fqn}.{src_canonical}"
        tgt_fqn = f"{downstream_fqn}.{tgt_canonical}"
        target_to_sources.setdefault(tgt_fqn, []).append(src_fqn)

    return [
        {"fromColumns": sources, "toColumn": target}
        for target, sources in target_to_sources.items()
    ]


def _build_filtered_column_lineage(
    column_mappings: list[tuple[str, str]],
    upstream_fqn: str,
    downstream_fqn: str,
    upstream_entity: dict,
    downstream_entity: dict,
) -> tuple[list[dict], int]:
    """Like ``_build_column_lineage`` but drops pairs referencing unknown columns.

    Returns ``(payload, filtered_count)`` where ``filtered_count`` is the
    number of ``(src, tgt)`` pairs dropped because either side references a
    column not present on the endpoint table's ``columns[]``. Star patterns
    (``*``) are not counted as filtered — they are considered not applicable
    rather than invalid.
    """
    up_cols = _build_column_name_map(upstream_entity)
    down_cols = _build_column_name_map(downstream_entity)

    target_to_sources: dict[str, list[str]] = {}
    filtered = 0
    for src_col, tgt_col in column_mappings:
        src_clean = src_col.strip().strip("[]\"'`").lower()
        tgt_clean = tgt_col.strip().strip("[]\"'`").lower()
        if src_clean == "*" or tgt_clean == "*":
            continue
        if src_clean not in up_cols or tgt_clean not in down_cols:
            filtered += 1
            logger.debug(
                "Filtering columnsLineage pair: %s.%s → %s.%s (unknown column)",
                upstream_fqn, src_clean, downstream_fqn, tgt_clean,
            )
            continue
        src_canonical = up_cols[src_clean]
        tgt_canonical = down_cols[tgt_clean]
        src_fqn = f"{upstream_fqn}.{src_canonical}"
        tgt_fqn = f"{downstream_fqn}.{tgt_canonical}"
        target_to_sources.setdefault(tgt_fqn, []).append(src_fqn)

    return (
        [{"fromColumns": sources, "toColumn": target}
         for target, sources in target_to_sources.items()],
        filtered,
    )
