"""Push lineage from a ``lineage-eval.v1`` JSON file (``--from-lineage-json``) into OpenMetadata.

The file is produced by an offline lineage evaluator that has already parsed the SQL, so this
mode never constructs a SQLFlow backend and sends no SQL anywhere: it only reads the file and
talks to the configured OpenMetadata server.

Contract (every fact carries a ``kind``):

- ``COLUMN``    table.column -> table.column. Becomes ``columnsLineage`` unless ``indirect``
                (FILTER / JOIN / CONDITION: decides rows or branch, does not supply the value).
- ``ROW_LEVEL`` one side is a table's row set. Table-level edge only.
- ``TABLE``     no column on either side. Table-level edge only.
- ``CONSTANT``  target computed from constants; no source table. Not pushed.
- ``CALL``      procedure calls procedure; an object dependency, not data movement. Not pushed.

Facts are first aggregated per (upstream table, downstream table) across ALL procedures, then
each edge is written with read-merge-write: OpenMetadata's ``PUT /v1/lineage`` replaces the
whole ``lineageDetails`` of an edge, so pushing procedure B's columns naively would erase
procedure A's — and lineage already present (from OpenMetadata's own ingestion or entered by
hand) must survive too. An edge whose merged details equal what the server already has is not
written again, so repeated runs are no-ops.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

import requests

from .config import SidecarConfig
from .emitter import OpenMetadataClient

logger = logging.getLogger(__name__)

CONTRACT = "lineage-eval.v1"
KINDS = {"COLUMN", "ROW_LEVEL", "TABLE", "CONSTANT", "CALL"}
INDIRECT_ROLES = {"FILTER", "JOIN", "CONDITION"}
# Written into lineageDetails.description so a later run can recognise (and extend) its own line
# instead of appending a duplicate.
DESCRIPTION_MARKER = "Column lineage recovered from stored procedures by gsp-openmetadata-sidecar: "
# Server-managed fields of lineageDetails: never sent back.
_SERVER_FIELDS = {"createdAt", "createdBy", "updatedAt", "updatedBy"}


class LineageJsonError(ValueError):
    """The input file is not a usable lineage-eval.v1 document."""


# --------------------------------------------------------------------------- planning (no I/O)


@dataclass(frozen=True)
class TableRef:
    database: Optional[str]
    schema: Optional[str]
    table: str

    def key(self) -> tuple:
        return tuple((p or "").lower() for p in (self.database, self.schema, self.table))

    def display(self) -> str:
        return ".".join(p for p in (self.database, self.schema, self.table) if p)


@dataclass
class EdgePlan:
    """Everything every procedure says about one upstream -> downstream table pair."""
    upstream: TableRef
    downstream: TableRef
    column_pairs: set[tuple[str, str]] = field(default_factory=set)   # (source column, target column)
    aggregate_pairs: set[tuple[str, str]] = field(default_factory=set)  # subset labelled AGGREGATE (Q1)
    indirect_pairs: set[tuple[str, str]] = field(default_factory=set)   # COLUMN facts kept out of columnsLineage
    contributors: set[str] = field(default_factory=set)


@dataclass
class PlanReport:
    procedures: int = 0
    failed_procedures: list[str] = field(default_factory=list)
    facts_by_kind: dict[str, int] = field(default_factory=dict)
    skipped_constant: int = 0
    skipped_call: int = 0
    skipped_self_loop: int = 0


def load_lineage_json(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        raise LineageJsonError(f"cannot read {path}: {e}") from e
    if not isinstance(doc, dict) or doc.get("contract") != CONTRACT:
        raise LineageJsonError(
            f"{path} is not a {CONTRACT} document (contract={doc.get('contract') if isinstance(doc, dict) else None!r})")
    if not isinstance(doc.get("procedures"), list):
        raise LineageJsonError(f"{path}: 'procedures' must be a list")
    return doc


def _table(endpoint: dict, default_db: Optional[str], default_schema: Optional[str]) -> Optional[TableRef]:
    table = endpoint.get("table")
    if not table:
        return None
    return TableRef(endpoint.get("database") or default_db, endpoint.get("schema") or default_schema, table)


def plan_edges(doc: dict, default_db: Optional[str] = None,
               default_schema: Optional[str] = None) -> tuple[list[EdgePlan], PlanReport]:
    """Aggregate every fact of every procedure into one EdgePlan per table pair."""
    report = PlanReport()
    plans: dict[tuple, EdgePlan] = {}
    for proc in doc["procedures"]:
        report.procedures += 1
        name = proc.get("name") or proc.get("sourceFile") or "?"
        if proc.get("status") == "FAILED":
            report.failed_procedures.append(name)
        for edge in proc.get("edges", []):
            kind = edge.get("kind")
            if kind not in KINDS:
                raise LineageJsonError(f"{name}: unknown fact kind {kind!r}")
            report.facts_by_kind[kind] = report.facts_by_kind.get(kind, 0) + 1
            if kind == "CONSTANT":
                report.skipped_constant += 1
                continue
            if kind == "CALL":
                report.skipped_call += 1
                continue
            up = _table(edge["source"], default_db, default_schema)
            down = _table(edge["target"], default_db, default_schema)
            if up is None or down is None:
                raise LineageJsonError(f"{name}: {kind} fact without a table on both sides")
            if up.key() == down.key():
                report.skipped_self_loop += 1   # e.g. UPDATE t ... JOIN on t's own key
                continue
            plan = plans.setdefault((up.key(), down.key()), EdgePlan(up, down))
            plan.contributors.add(name)
            if kind != "COLUMN":
                continue
            pair = (edge["source"]["column"], edge["target"]["column"])
            indirect = edge.get("indirect")
            if indirect is None:
                indirect = edge.get("role") in INDIRECT_ROLES
            if indirect:
                plan.indirect_pairs.add(pair)
                continue
            plan.column_pairs.add(pair)
            if edge.get("role") == "AGGREGATE":
                plan.aggregate_pairs.add(pair)
    return sorted(plans.values(), key=lambda p: (p.downstream.key(), p.upstream.key())), report


def render_plan(plans: list[EdgePlan], report: PlanReport) -> str:
    lines = [f"{report.procedures} procedure(s), {len(plans)} table-level edge(s); facts by kind: "
             + ", ".join(f"{k}={v}" for k, v in sorted(report.facts_by_kind.items()))]
    lines.append(f"not pushed: {report.skipped_constant} CONSTANT, {report.skipped_call} CALL, "
                 f"{report.skipped_self_loop} self-referencing")
    if report.failed_procedures:
        lines.append("procedures the evaluator could not analyze: " + ", ".join(report.failed_procedures))
    for p in plans:
        lines.append("")
        lines.append(f"{p.upstream.display()} -> {p.downstream.display()}   "
                     f"[{len(p.column_pairs)} column mapping(s); from {', '.join(sorted(p.contributors))}]")
        for src, tgt in sorted(p.column_pairs):
            flag = "   (AGGREGATE: may be a grouping/filter influence, see evaluator report)" \
                if (src, tgt) in p.aggregate_pairs else ""
            lines.append(f"    {src} -> {tgt}{flag}")
        for src, tgt in sorted(p.indirect_pairs):
            lines.append(f"    {src} -> {tgt}   (indirect: table-level only, not in columnsLineage)")
    return "\n".join(lines)


# --------------------------------------------------------------------------- merge (no I/O)


def _unquote(name: str) -> str:
    """[Addr.Loc.City] / "x" / `x` -> the bare identifier (quoting keeps dots and spaces literal)."""
    if len(name) >= 2 and (name[0], name[-1]) in {("[", "]"), ('"', '"'), ("`", "`")}:
        return name[1:-1]
    return name


def resolve_columns(pairs: set[tuple[str, str]], up_entity: dict, down_entity: dict
                    ) -> tuple[list[dict], list[str]]:
    """Map name pairs to column FQNs: exact name first, then a UNIQUE case-insensitive match.

    Returns (columnsLineage entries, human-readable rejections). A pair is rejected when either
    column is absent from the entity or matches several columns case-insensitively — pushing a
    guess would attach lineage to the wrong column.
    """
    def index(entity: dict) -> tuple[dict, dict]:
        exact, folded = {}, {}
        for col in entity.get("columns", []):
            exact[col["name"]] = col["fullyQualifiedName"]
            folded.setdefault(col["name"].lower(), []).append(col["fullyQualifiedName"])
        return exact, folded

    def find(name: str, idx: tuple[dict, dict]) -> Optional[str]:
        name = _unquote(name)
        exact, folded = idx
        if name in exact:
            return exact[name]
        hits = folded.get(name.lower(), [])
        return hits[0] if len(hits) == 1 else None

    up_idx, down_idx = index(up_entity), index(down_entity)
    by_target: dict[str, set[str]] = {}
    rejected = []
    for src, tgt in sorted(pairs):
        src_fqn, tgt_fqn = find(src, up_idx), find(tgt, down_idx)
        if src_fqn is None or tgt_fqn is None:
            missing = [n for n, f in ((src, src_fqn), (tgt, tgt_fqn)) if f is None]
            rejected.append(f"{src} -> {tgt} (no unique column for {', '.join(missing)})")
            continue
        by_target.setdefault(tgt_fqn, set()).add(src_fqn)
    return [{"fromColumns": sorted(s), "toColumn": t} for t, s in sorted(by_target.items())], rejected


def merge_details(existing: Optional[dict], ours: list[dict], contributors: set[str]) -> dict:
    """Union our column lineage and contributor note into the edge's existing lineageDetails.

    Everything already on the edge is kept (sqlQuery, source, pipeline, other columns, other
    description text); our entries only ever add fromColumns. The description carries one
    marker line listing contributing procedures, extended in place on later runs.
    """
    details = {k: v for k, v in (existing or {}).items() if k not in _SERVER_FIELDS}
    by_target: dict[str, dict] = {}
    for entry in details.get("columnsLineage") or []:
        by_target[entry["toColumn"]] = dict(entry, fromColumns=list(entry.get("fromColumns") or []))
    for entry in ours:
        cur = by_target.setdefault(entry["toColumn"], {"toColumn": entry["toColumn"], "fromColumns": []})
        cur["fromColumns"] = sorted(set(cur["fromColumns"]) | set(entry["fromColumns"]))
    if by_target:
        details["columnsLineage"] = [by_target[t] for t in sorted(by_target)]

    lines = (details.get("description") or "").splitlines()
    ours_line = [i for i, line in enumerate(lines) if line.startswith(DESCRIPTION_MARKER)]
    known = set()
    if ours_line:
        known = {p.strip() for p in lines[ours_line[0]][len(DESCRIPTION_MARKER):].split(",") if p.strip()}
    marker = DESCRIPTION_MARKER + ", ".join(sorted(known | contributors))
    if ours_line:
        lines[ours_line[0]] = marker
    else:
        lines.append(marker)
    details["description"] = "\n".join(lines)
    details.setdefault("source", "QueryLineage")
    return details


def _normalized(details: Optional[dict]) -> str:
    d = {k: v for k, v in (details or {}).items() if k not in _SERVER_FIELDS}
    cols = d.get("columnsLineage") or []
    d["columnsLineage"] = sorted(
        ({**c, "fromColumns": sorted(c.get("fromColumns") or [])} for c in cols), key=lambda c: c["toColumn"])
    return json.dumps(d, sort_keys=True)


# --------------------------------------------------------------------------- push


@dataclass
class PushResult:
    written: int = 0
    unchanged: int = 0
    failed: int = 0
    unresolved_tables: set[str] = field(default_factory=set)
    rejected_columns: list[str] = field(default_factory=list)


def _resolve_table(client: OpenMetadataClient, service: str, ref: TableRef) -> Optional[dict]:
    """Table entity whose FQN matches exactly (case-insensitively). A search hit for another
    table — the lookup's fallback can return one — is treated as not found."""
    fqn = ".".join(p for p in (service, ref.database, ref.schema, ref.table) if p)
    entity = client.lookup_table(fqn)
    if entity and entity.get("fullyQualifiedName", "").lower() == fqn.lower():
        return entity
    return None


# lineageDetails keys; getLineageEdge in OpenMetadata 2.0.x returns them FLAT under "edge"
# ({"edge": {"columnsLineage": [...], "source": ...}}), older payloads nest them under
# "edge.lineageDetails". Both shapes are accepted; an edge with none of these keys has no details.
_DETAIL_KEYS = {"sqlQuery", "columnsLineage", "pipeline", "description", "source", "assetEdges",
                "tempLineageTables", "createdAt", "createdBy", "updatedAt", "updatedBy"}


def edge_details_from_response(body: Optional[dict]) -> Optional[dict]:
    """The lineageDetails of a getLineageEdge response, or None when the edge carries none."""
    edge = (body or {}).get("edge", body) or {}
    if isinstance(edge.get("lineageDetails"), dict):
        return edge["lineageDetails"]
    details = {k: v for k, v in edge.items() if k in _DETAIL_KEYS}
    return details or None


def _get_edge_details(client: OpenMetadataClient, from_id: str, to_id: str) -> Optional[dict]:
    url = f"{client.base_url}/v1/lineage/getLineageEdge/{from_id}/{to_id}"
    resp = requests.get(url, headers=client._headers(), timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return edge_details_from_response(resp.json())


def push(client: OpenMetadataClient, service: str, plans: list[EdgePlan]) -> PushResult:
    result = PushResult()
    for plan in plans:
        up = _resolve_table(client, service, plan.upstream)
        down = _resolve_table(client, service, plan.downstream)
        for ref, entity in ((plan.upstream, up), (plan.downstream, down)):
            if entity is None:
                result.unresolved_tables.add(ref.display())
        if up is None or down is None:
            continue
        ours, rejected = resolve_columns(plan.column_pairs, up, down)
        result.rejected_columns += [f"{plan.upstream.display()} -> {plan.downstream.display()}: {r}"
                                    for r in rejected]
        try:
            existing = _get_edge_details(client, up["id"], down["id"])
            merged = merge_details(existing, ours, plan.contributors)
            if existing is not None and _normalized(existing) == _normalized(merged):
                result.unchanged += 1
                continue
            payload = {"edge": {"fromEntity": {"id": up["id"], "type": "table"},
                                "toEntity": {"id": down["id"], "type": "table"},
                                "lineageDetails": merged}}
            if client.add_lineage(payload):
                result.written += 1
            else:
                result.failed += 1
        except requests.RequestException as e:
            logger.error("edge %s -> %s failed: %s", plan.upstream.display(), plan.downstream.display(), e)
            result.failed += 1
    return result


def run(config: SidecarConfig, path: str, dry_run: bool) -> int:
    """Entry point for ``--from-lineage-json``. Returns the process exit code."""
    try:
        doc = load_lineage_json(path)
        plans, report = plan_edges(doc, config.openmetadata.database_name, config.openmetadata.schema_name)
    except LineageJsonError as e:
        logger.error("%s", e)
        return 1
    print(render_plan(plans, report))
    if dry_run:
        return 0
    result = push(OpenMetadataClient(config.openmetadata), config.openmetadata.service_name, plans)
    print(f"\nOpenMetadata: {result.written} edge(s) written, {result.unchanged} unchanged, "
          f"{result.failed} failed")
    if result.unresolved_tables:
        print("tables not found in OpenMetadata (edges skipped): " + ", ".join(sorted(result.unresolved_tables)))
    for r in result.rejected_columns:
        print("column mapping rejected: " + r)
    return 2 if result.failed else 0
