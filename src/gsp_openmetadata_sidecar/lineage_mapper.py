"""Map SQLFlow lineage JSON to structured table/column lineage objects."""

import logging
from collections import defaultdict
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# effectTypes that represent data movement between persistent objects.
# Mirrors gudusoft.gsqlparser.dlineage.dataflow.model.EffectType — MERGE in
# particular fans out into three emitted rows per statement (merge, the
# table-level marker; merge_insert, for the WHEN NOT MATCHED ... INSERT branch;
# merge_update, for WHEN MATCHED ... UPDATE), so all three must be included
# or column lineage from MERGE silently vanishes.
PERSISTENT_EFFECT_TYPES = {
    "create_view", "create_table",
    "insert", "update",
    "merge", "merge_insert", "merge_update", "merge_when",
}

# Power Query M has no explicit persistent target — a `let ... in <expr>`
# script IS the definition of a Power BI dataset/table. SQLFlow emits
# `effectType: select` for every nav-chain / Value.NativeQuery binding, with
# the downstream slot filled by intermediate result-sets (`rs-1`, `rs-2`...).
# Accept `select` exclusively on M so other dialects don't start emitting
# bogus lineage from every SELECT statement.
POWERQUERY_VENDOR_ALIASES = {"powerquery", "m", "powerbi", "dbvpowerquery"}

# Prefixes for intermediate result sets (not real tables). SQLFlow emits
# synthetic parents like ``RS-3`` for SELECT result sets and
# ``MERGE-INSERT-1`` / ``MERGE-UPDATE-1`` for the two branches of a MERGE
# statement — both should be resolved through to the real source/target, not
# emitted as lineage endpoints themselves.
INTERMEDIATE_PREFIXES = (
    "RS-", "RESULT_OF_",
    "INSERT-SELECT-",
    "MERGE-INSERT-", "MERGE-UPDATE-", "MERGE-DELETE-", "MERGE-WHEN-",
)

# Placeholder segments SQLFlow emits in the dbobjs tree when
# defaultServer/defaultDatabase/defaultSchema are not supplied. They must be
# dropped from any qualified name reconstruction; otherwise we'd produce
# garbage FQNs like ``DEFAULT_SERVER.DEFAULT.DEFAULT.customers``.
_PLACEHOLDER_SEGMENTS = {"DEFAULT_SERVER", "DEFAULT"}


def _vendor_is_powerquery(db_vendor: str) -> bool:
    return (db_vendor or "").strip().lower() in POWERQUERY_VENDOR_ALIASES


def _allowed_effects(db_vendor: str) -> set[str]:
    if _vendor_is_powerquery(db_vendor):
        return PERSISTENT_EFFECT_TYPES | {"select"}
    return PERSISTENT_EFFECT_TYPES


@dataclass
class ColumnLineage:
    """A single column-level lineage relationship."""
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    effect_type: str


@dataclass
class TableLineage:
    """Table-level lineage with column details."""
    upstream_table: str
    downstream_table: str
    column_mappings: list[tuple[str, str]] = field(default_factory=list)
    # (source_column, target_column) pairs


def _is_intermediate(name: str, function_names: set[str] | None = None) -> bool:
    """Check if a name refers to an intermediate result set rather than a real table.

    Intermediate means: RS-*/RESULT_OF_* result sets, MSSQL temp tables (#name),
    or function nodes (e.g. ARRAY_AGG, COUNT) that appear as parents in the
    relationship chain but are not real tables.
    """
    upper = name.upper()
    if any(upper.startswith(p) for p in INTERMEDIATE_PREFIXES):
        return True
    # MSSQL temp tables (#name) are intermediates — they don't exist in
    # OpenMetadata so lineage through them must be resolved transitively.
    bare = name.split(".")[-1]
    if bare.startswith("#"):
        return True
    if function_names and upper in function_names:
        return True
    return False


def _extract_function_names(sqlflow_response: dict) -> set[str]:
    """Extract names of function nodes from SQLFlow dbobjs.

    SQLFlow represents aggregate/scalar functions (ARRAY_AGG, COUNT, etc.)
    as 'others' entries with type='function'. These are intermediate nodes
    in the lineage chain and should be resolved through, not treated as tables.
    """
    names: set[str] = set()
    dbobjs = _find_key(sqlflow_response, "dbobjs")
    if not dbobjs:
        return names
    for server in dbobjs.get("servers", []):
        for db in server.get("databases", []):
            for schema in db.get("schemas", []):
                for other in schema.get("others", []):
                    if other.get("type") == "function":
                        names.add(other["name"].upper())
    return names


def _find_key(obj, key: str):
    """Recursively search a nested dict for a key. Returns the first match."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            result = _find_key(v, key)
            if result is not None:
                return result
    return None


def _build_id_to_fqn(sqlflow_response: dict) -> dict[str, str]:
    """Build a map from dbobjs entity id to a qualified name.

    An entry is emitted ONLY when the tree has real (non-placeholder) values
    for both the database and the schema. Server is optional — when it is
    real, it is prepended as a fourth segment (``server.db.schema.table``);
    ``_build_fqn`` takes the last three parts, so the extra server segment
    is harmlessly discarded.

    The strictness is deliberate. A naive per-segment strip collapses
    positional information: e.g. ``srv01.DEFAULT.dbo.T`` becoming
    ``srv01.dbo.T`` would be misread by ``_build_fqn`` as ``db=srv01`` since
    it keys off segment count rather than role. By refusing to emit a name
    unless db + schema are both present, the compact form always aligns with
    ``_build_fqn``'s ``database.schema.table`` interpretation. Ambiguous
    partial trees fall back to bare ``parentName`` — which is what a caller
    who did not set defaults would have seen before this change.
    """
    result: dict[str, str] = {}
    dbobjs = _find_key(sqlflow_response, "dbobjs")
    if not dbobjs:
        return result
    for server in dbobjs.get("servers", []) or []:
        s_name = server.get("name") or ""
        s_keep = bool(s_name) and s_name not in _PLACEHOLDER_SEGMENTS
        for db in server.get("databases", []) or []:
            d_name = db.get("name") or ""
            d_keep = bool(d_name) and d_name not in _PLACEHOLDER_SEGMENTS
            for schema in db.get("schemas", []) or []:
                sc_name = schema.get("name") or ""
                sc_keep = bool(sc_name) and sc_name not in _PLACEHOLDER_SEGMENTS
                if not (d_keep and sc_keep):
                    # Ambiguous or fully placeholder. Fall back to parentName.
                    continue
                entities = (schema.get("tables") or []) + (schema.get("views") or [])
                for ent in entities:
                    ent_id = ent.get("id")
                    ent_name = ent.get("name") or ""
                    if ent_id is None or not ent_name:
                        continue
                    parts: list[str] = []
                    if s_keep:
                        parts.append(s_name)
                    parts.append(d_name)
                    parts.append(sc_name)
                    parts.append(ent_name)
                    result[str(ent_id)] = ".".join(parts)
    return result


def _qualified_parent_name(node: dict, id_to_fqn: dict[str, str]) -> str:
    """Prefer the dbobjs-resolved qualified name when available.

    Falls back to bare ``parentName`` for intermediates (RS-*, MERGE-*) and for
    entities whose tree entry is all placeholders — preserving status-quo
    behavior for users who don't set default_server/database/schema.
    """
    pid = node.get("parentId")
    if pid is not None:
        resolved = id_to_fqn.get(str(pid))
        if resolved:
            return resolved
    return node["parentName"]


def extract_lineage(
    sqlflow_response: dict,
    db_vendor: str = "",
    downstream_override: str | None = None,
) -> list[TableLineage]:
    """Extract table-level lineage (with column mappings) from SQLFlow API response.

    Walks the 'relationships' array in the SQLFlow JSON. For relationships
    involving persistent objects (CREATE VIEW, INSERT, MERGE, etc.), maps
    source tables to target tables. Intermediate result sets (RS-*,
    RESULT_OF_*) are traversed to find the real source tables.

    ``db_vendor`` gates effect-type inclusion. Power Query M is special: the
    M script itself defines a Power BI dataset so every upstream lineage is
    carried on ``effectType: select`` rows whose target is an ``rs-N``
    intermediate. When ``db_vendor`` is one of the M aliases, ``select``
    becomes a persistent effect and ``downstream_override`` (if supplied) is
    stamped in as the target table — mirroring what a caller who already
    knows the Power BI dataset URN would do. Without the override, M-mode
    lineage still resolves the upstreams but has no downstream to anchor to
    and is dropped.

    Returns a list of TableLineage objects suitable for DataHub MCP emission.
    """
    relationships = _find_key(sqlflow_response, "relationships")
    if not relationships:
        logger.warning("No 'relationships' found in SQLFlow response")
        return []

    allowed_effects = _allowed_effects(db_vendor)
    is_m = _vendor_is_powerquery(db_vendor)

    # Identify function nodes so they are resolved through, not treated as tables
    function_names = _extract_function_names(sqlflow_response)
    if function_names:
        logger.debug("Function nodes (treated as intermediates): %s", function_names)

    # Build id -> qualified name map once. All downstream name accesses prefer
    # the tree-resolved name over bare ``parentName`` so that unqualified SQL
    # parsed with default_server/database/schema lands at real tables instead
    # of single-segment names.
    id_to_fqn = _build_id_to_fqn(sqlflow_response)

    # Phase 1: collect all fdd relationships
    all_rels = [r for r in relationships if r.get("type") == "fdd"]
    logger.debug("Total fdd relationships: %d", len(all_rels))

    # Phase 2: build a reverse lookup — for each intermediate column,
    # trace back to the real source columns
    # Key: (qualified_parent_name, column) -> list of (source_qualified, sourceColumn)
    reverse_map: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    for rel in all_rels:
        tgt = rel["target"]
        tgt_key = (_qualified_parent_name(tgt, id_to_fqn), tgt["column"])
        for src in rel.get("sources", []):
            reverse_map[tgt_key].append(
                (_qualified_parent_name(src, id_to_fqn), src["column"])
            )

    def resolve_sources(parent_name: str, column: str, visited: set | None = None) -> list[tuple[str, str]]:
        """Recursively resolve intermediate result sets to real source tables."""
        if visited is None:
            visited = set()

        key = (parent_name, column)
        if key in visited:
            return []  # cycle protection
        visited.add(key)

        if not _is_intermediate(parent_name, function_names):
            return [(parent_name, column)]

        # It's an intermediate — look up what feeds into it
        sources = reverse_map.get(key, [])
        if not sources:
            return [(parent_name, column)]  # can't resolve further

        real_sources = []
        for src_parent, src_col in sources:
            real_sources.extend(resolve_sources(src_parent, src_col, visited))
        return real_sources

    # Phase 3: for each "persistent effect" relationship, resolve sources
    # and build table-level lineage
    table_lineage_map: dict[tuple[str, str], TableLineage] = {}

    for rel in all_rels:
        effect = rel.get("effectType", "")
        if effect not in allowed_effects:
            continue

        target = rel["target"]
        target_table = _qualified_parent_name(target, id_to_fqn)
        target_column = target["column"]

        if _is_intermediate(target_table, function_names):
            if is_m and downstream_override:
                # Collapse the rs-N intermediate onto the caller-supplied
                # Power BI dataset name — the M script's "in" clause result.
                target_table = downstream_override
            else:
                continue  # target should be a real table

        for src in rel.get("sources", []):
            src_parent = _qualified_parent_name(src, id_to_fqn)
            src_column = src["column"]

            # Resolve through intermediates
            real_sources = resolve_sources(src_parent, src_column)

            for real_table, real_column in real_sources:
                if _is_intermediate(real_table, function_names):
                    continue
                if real_table == target_table:
                    continue  # skip self-references

                pair_key = (real_table, target_table)
                if pair_key not in table_lineage_map:
                    table_lineage_map[pair_key] = TableLineage(
                        upstream_table=real_table,
                        downstream_table=target_table,
                    )
                table_lineage_map[pair_key].column_mappings.append(
                    (real_column, target_column)
                )

    lineages = list(table_lineage_map.values())

    # Power Query navigator chains produce stub relationships for each step
    # (database -> schema -> table), so a single M-query typically emits
    # `DB` and `DB.SCHEMA` as upstreams alongside the real `DB.SCHEMA.TABLE`.
    # Those stubs aren't real DataHub datasets; drop any upstream that's a
    # strict dotted prefix of another upstream targeting the same downstream.
    if is_m:
        by_downstream: dict[str, list[TableLineage]] = defaultdict(list)
        for tl in lineages:
            by_downstream[tl.downstream_table].append(tl)
        keep: list[TableLineage] = []
        for group in by_downstream.values():
            names = [tl.upstream_table for tl in group]
            for tl in group:
                up = tl.upstream_table
                if any(other != up and other.startswith(up + ".") for other in names):
                    logger.debug(
                        "Power Query: dropping nav-stub upstream %r superseded by a more-qualified name",
                        up,
                    )
                    continue
                keep.append(tl)
        lineages = keep

    # Deduplicate column mappings within each table lineage
    for tl in lineages:
        tl.column_mappings = list(set(tl.column_mappings))

    logger.info("Extracted %d table-level lineage relationships", len(lineages))
    for tl in lineages:
        logger.info("  %s --> %s (%d columns)",
                     tl.upstream_table, tl.downstream_table, len(tl.column_mappings))

    return lineages
