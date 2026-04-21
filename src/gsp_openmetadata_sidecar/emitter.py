"""Emit lineage to OpenMetadata via the REST API (PUT /api/v1/lineage)."""

import logging
from typing import Any, Optional

import requests

from .config import OpenMetadataConfig
from .lineage_mapper import TableLineage

logger = logging.getLogger(__name__)


class OpenMetadataClient:
    """Thin client for OpenMetadata REST API."""

    def __init__(self, config: OpenMetadataConfig):
        self.base_url = config.server.rstrip("/")
        self.token = config.token
        self.service_name = config.service_name
        self.database_name = config.database_name
        self.schema_name = config.schema_name

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

        We fill in missing parts from config defaults.
        """
        parts = [p.strip().strip("[]\"'`") for p in table_name.split(".")]

        if len(parts) >= 3:
            db, schema, table = parts[-3], parts[-2], parts[-1]
        elif len(parts) == 2:
            db = self.database_name or ""
            schema, table = parts[-2], parts[-1]
        else:
            db = self.database_name or ""
            schema = self.schema_name
            table = parts[0]

        # Build FQN: service.database.schema.table
        fqn_parts = [self.service_name]
        if db:
            fqn_parts.append(db)
        fqn_parts.append(schema)
        fqn_parts.append(table)

        return ".".join(p.lower() for p in fqn_parts)

    def lookup_table(self, fqn: str) -> Optional[dict[str, Any]]:
        """Look up a table entity in OpenMetadata by FQN.

        Returns the entity dict (with 'id', 'name', etc.) or None if not found.
        """
        url = f"{self.base_url}/v1/tables/name/{fqn}"
        try:
            resp = requests.get(url, headers=self._headers(), timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                logger.warning("Table not found in OpenMetadata: %s", fqn)
                return None
            logger.warning("Unexpected status %d looking up %s: %s",
                          resp.status_code, fqn, resp.text[:200])
            return None
        except requests.RequestException as e:
            logger.error("Failed to lookup table %s: %s", fqn, e)
            return None

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


def emit_lineage(
    lineages: list[TableLineage],
    sql_query: str,
    config: OpenMetadataConfig,
    dry_run: bool = False,
) -> int:
    """Resolve tables and emit lineage to OpenMetadata.

    Returns the number of lineage edges successfully emitted.
    """
    client = OpenMetadataClient(config)
    emitted = 0
    skipped = 0

    # Cache table lookups to avoid repeated API calls
    fqn_cache: dict[str, Optional[dict]] = {}

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

        # Look up entities
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

        # Build column lineage
        col_lineage = None
        if config.column_lineage and tl.column_mappings:
            col_lineage = _build_column_lineage(
                tl.column_mappings, upstream_fqn, downstream_fqn
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
    return emitted


def _build_column_lineage(
    column_mappings: list[tuple[str, str]],
    upstream_fqn: str,
    downstream_fqn: str,
) -> list[dict]:
    """Build OpenMetadata columnsLineage array from column mapping pairs.

    OpenMetadata format:
      [{"fromColumns": ["service.db.schema.table.col"], "toColumn": "service.db.schema.table.col"}]
    """
    # Group by target column
    target_to_sources: dict[str, list[str]] = {}
    for src_col, tgt_col in column_mappings:
        src_clean = src_col.strip().lower().strip("[]\"'`")
        tgt_clean = tgt_col.strip().lower().strip("[]\"'`")
        if src_clean == "*" or tgt_clean == "*":
            continue
        src_fqn = f"{upstream_fqn}.{src_clean}"
        tgt_fqn = f"{downstream_fqn}.{tgt_clean}"
        target_to_sources.setdefault(tgt_fqn, []).append(src_fqn)

    return [
        {"fromColumns": sources, "toColumn": target}
        for target, sources in target_to_sources.items()
    ]
