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

The whole document is validated before any network call. Table names are kept exactly as
written until they are resolved against the catalog; facts are then aggregated per resolved
(upstream entity, downstream entity) pair, so ``Customers`` and ``customers`` merge only when
the catalog says they are the same table.

Each edge is written with read-merge-write: OpenMetadata's ``PUT /v1/lineage`` replaces the
whole ``lineageDetails`` of an edge, so pushing procedure B's columns naively would erase
procedure A's — and lineage already present (from OpenMetadata's own ingestion or entered by
hand) must survive. Existing fields and column entries are never modified; this mode only appends
column entries and one contributor marker line. An edge whose merged details equal the server's is
not rewritten. (Verified in a sequential end-to-end run; not a guarantee under concurrent writers.)

Concurrency: read-merge-write is NOT atomic. Two writers updating the same edge at the same
time (two sidecar runs, or a sidecar run overlapping OpenMetadata's own lineage ingestion) can
lose one writer's additions. Run one writer at a time, after OpenMetadata's ingestion finished.
"""

from __future__ import annotations

import json
import re
import logging
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import quote

import requests

from .config import OpenMetadataConfig, SidecarConfig

logger = logging.getLogger(__name__)

CONTRACT = "lineage-eval.v1"
KINDS = {"COLUMN", "ROW_LEVEL", "TABLE", "CONSTANT", "CALL"}
INDIRECT_ROLES = {"FILTER", "JOIN", "CONDITION", "GROUP_BY"}
# The "function" text of a pushed indirect column mapping, per role; which roles are pushed at all
# depends on openmetadata.indirect_columns (condition / all / none).
INDIRECT_LABELS = {"CONDITION": "CASE WHEN condition", "GROUP_BY": "GROUP BY key",
                   "FILTER": "filter (WHERE/HAVING)", "JOIN": "join condition"}
PUSHED_INDIRECT = {"condition": {"CONDITION"}, "all": set(INDIRECT_LABELS), "none": set()}
_OUR_LABELS = set(INDIRECT_LABELS.values())
STATUSES = {"OK", "PARTIAL", "FAILED"}

# One description line records the contributing procedures as a JSON array, so names containing
# commas or quotes round-trip. A line with this prefix that is not valid JSON (e.g. edited by hand)
# is left untouched and no contributors are recorded for that edge.
MARKER_PREFIX = "gsp-openmetadata-sidecar contributors v1: "
MAX_CONTRIBUTORS = 200
# Edge identity and server-managed fields of a getLineageEdge response: never sent back.
_NOT_DETAILS = {"fromEntity", "toEntity", "createdAt", "createdBy", "updatedAt", "updatedBy"}

EXIT_OK, EXIT_INPUT, EXIT_FAILED, EXIT_INCOMPLETE = 0, 1, 2, 3


class LineageJsonError(ValueError):
    """The input file is not a usable lineage-eval.v1 document."""


class LookupFailed(RuntimeError):
    """OpenMetadata could not be asked (auth, network, 5xx) — distinct from "table not found"."""


# --------------------------------------------------------------------------- input validation
# Mirrors lineage-eval.v1.schema.json rule for rule (tests/test_lineage_json_input.py checks the two
# agree on generated documents): a file the schema rejects never reaches planning or the network.


def _nonblank(value: Any) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _opt_str(value: Any) -> bool:
    return value is None or isinstance(value, str)


def _json_integer(value: Any) -> bool:
    """JSON Schema "integer": any number with a zero fractional part (1 and 1.0), never a boolean."""
    if isinstance(value, bool):
        return False
    return isinstance(value, int) or (isinstance(value, float) and value.is_integer())


def _check(cond: bool, where: str, message: str) -> None:
    if not cond:
        raise LineageJsonError(f"{where}: {message}")


def validate_document(doc: Any) -> None:
    """Raise LineageJsonError on the first violation of lineage-eval.v1, before anything is sent."""
    if not isinstance(doc, dict) or doc.get("contract") != CONTRACT:
        found = doc.get("contract") if isinstance(doc, dict) else None
        raise LineageJsonError(f"not a {CONTRACT} document (contract={found!r})")
    for key in ("generatedAt", "dialect", "parserVersion"):
        _check(key not in doc or isinstance(doc[key], str), key, "must be a string")
    _check(_opt_str(doc.get("defaultDatabase")), "defaultDatabase", "must be a string or null")
    _check("settingsHash" not in doc or (isinstance(doc["settingsHash"], str)
                                        and re.search(r"^[0-9a-f]{64}$", doc["settingsHash"]) is not None),
           "settingsHash", "must be 64 lowercase hex characters")
    procs = doc.get("procedures")
    _check(isinstance(procs, list), "procedures", "must be a list")
    for i, proc in enumerate(procs):
        where = f"procedures[{i}]"
        _check(isinstance(proc, dict), where, "must be an object")
        for key in ("status", "edges", "issues"):
            _check(key in proc, where, f"'{key}' is required")
        _check(isinstance(proc["status"], str) and proc["status"] in STATUSES, where,
               f"status must be one of {sorted(STATUSES)}")
        for key in ("name", "moduleType", "sourceFile", "error"):
            _check(_opt_str(proc.get(key)), where, f"{key} must be a string or null")
        _check(isinstance(proc["issues"], list), where, "issues must be a list")
        for j, issue in enumerate(proc["issues"]):
            _check(isinstance(issue, dict), f"{where}.issues[{j}]", "must be an object")
            for key in ("stage", "severity", "reasonCode", "reason"):
                _check(_opt_str(issue.get(key)), f"{where}.issues[{j}]", f"{key} must be a string or null")
        _check(isinstance(proc["edges"], list), where, "edges must be a list")
        for j, e in enumerate(proc["edges"]):
            _validate_edge(e, f"{where}.edges[{j}]")


def _validate_endpoint(ep: Any, where: str) -> None:
    _check(isinstance(ep, dict), where, "must be an object")
    _check("table" in ep, where, "'table' is required")
    for key in ("database", "schema", "table", "column"):
        _check(_opt_str(ep.get(key)), where, f"{key} must be a string or null")


def _validate_edge(e: Any, where: str) -> None:
    _check(isinstance(e, dict), where, "must be an object")
    kind = e.get("kind")
    _check(isinstance(kind, str) and kind in KINDS, where, f"unknown fact kind {kind!r}")
    for side in ("source", "target"):
        _check(side in e, where, f"'{side}' is required")
        _validate_endpoint(e[side], f"{where}.{side}")
    for key in ("role", "transformation", "confidence"):
        _check(_opt_str(e.get(key)), where, f"{key} must be a string or null")
    _check("indirect" not in e or isinstance(e["indirect"], bool), where, "indirect must be a boolean")
    _check("statementIndex" not in e or _json_integer(e["statementIndex"]),
           where, "statementIndex must be an integer")
    src, tgt = e["source"], e["target"]
    has = lambda ep, key: _nonblank(ep.get(key))            # present, a string, not blank
    absent = lambda ep, key: ep.get(key) is None            # null or missing
    _check(has(tgt, "table"), where, "target.table is required")
    if kind == "COLUMN":
        _check(has(src, "table") and has(src, "column") and has(tgt, "column"), where,
               "COLUMN needs a table and a column on both sides")
    elif kind == "ROW_LEVEL":
        _check(has(src, "table"), where, "ROW_LEVEL needs source.table")
        _check((has(src, "column") and absent(tgt, "column")) or (absent(src, "column") and has(tgt, "column")),
               where, "ROW_LEVEL needs a column on exactly one side")
    elif kind in ("TABLE", "CALL"):
        _check(has(src, "table") and absent(src, "column") and absent(tgt, "column"), where,
               f"{kind} needs source.table and no columns")
    elif kind == "CONSTANT":
        _check(absent(src, "table") and absent(src, "column") and has(tgt, "column"), where,
               "CONSTANT needs no source table/column and a target column")


def load_lineage_json(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as e:
        raise LineageJsonError(f"cannot read {path}: {e}") from e
    try:
        validate_document(doc)
    except LineageJsonError as e:
        raise LineageJsonError(f"{path}: {e}") from None
    return doc


# --------------------------------------------------------------------------- planning (no I/O)


@dataclass(frozen=True)
class TableRef:
    """A table exactly as the file names it — never case-folded before catalog resolution."""
    database: Optional[str]
    schema: Optional[str]
    table: str

    def display(self) -> str:
        return ".".join(p for p in (self.database, self.schema, self.table) if p)


@dataclass
class EdgePlan:
    """Everything every procedure says about one (as-written) upstream -> downstream pair."""
    upstream: TableRef
    downstream: TableRef
    column_pairs: set[tuple[str, str]] = field(default_factory=set)
    labelled_pairs: dict[str, set[tuple[str, str]]] = field(default_factory=dict)  # function -> pairs
    indirect_pairs: set[tuple[str, str]] = field(default_factory=set)   # kept out of columnsLineage
    contributors: set[str] = field(default_factory=set)


@dataclass
class PlanReport:
    procedures: int = 0
    failed_procedures: list[str] = field(default_factory=list)
    facts_by_kind: dict[str, int] = field(default_factory=dict)
    skipped_constant: int = 0
    skipped_call: int = 0
    skipped_self_loop: int = 0


def _table(endpoint: dict, default_db: Optional[str], default_schema: Optional[str]) -> TableRef:
    return TableRef(endpoint.get("database") or default_db, endpoint.get("schema") or default_schema,
                    endpoint["table"])


def plan_edges(doc: dict, default_db: Optional[str] = None, default_schema: Optional[str] = None,
               indirect_columns: str = "condition") -> tuple[list[EdgePlan], PlanReport]:
    """Aggregate the facts of every procedure per as-written table pair (doc must be validated).

    Value lineage becomes plain column mappings. An indirect COLUMN fact becomes a mapping labelled
    with its role (INDIRECT_LABELS) when ``indirect_columns`` pushes that role, and is otherwise
    kept out of columnsLineage; it still contributes the table-level edge."""
    pushed = PUSHED_INDIRECT[indirect_columns]
    report = PlanReport()
    plans: dict[tuple[TableRef, TableRef], EdgePlan] = {}
    for proc in doc["procedures"]:
        report.procedures += 1
        name = proc.get("name") or proc.get("sourceFile") or "?"
        if proc.get("status") == "FAILED":
            report.failed_procedures.append(name)
        for edge in proc.get("edges", []):
            kind = edge["kind"]
            report.facts_by_kind[kind] = report.facts_by_kind.get(kind, 0) + 1
            if kind == "CONSTANT":
                report.skipped_constant += 1
                continue
            if kind == "CALL":
                report.skipped_call += 1
                continue
            up = _table(edge["source"], default_db, default_schema)
            down = _table(edge["target"], default_db, default_schema)
            if up == down:
                report.skipped_self_loop += 1   # e.g. UPDATE t ... JOIN on t's own key
                continue
            plan = plans.setdefault((up, down), EdgePlan(up, down))
            plan.contributors.add(name)
            if kind != "COLUMN":
                continue
            pair = (edge["source"]["column"], edge["target"]["column"])
            indirect = edge.get("indirect")
            if indirect is None:
                indirect = edge.get("role") in INDIRECT_ROLES
            if indirect:
                role = edge.get("role")
                if role in pushed:
                    plan.labelled_pairs.setdefault(INDIRECT_LABELS[role], set()).add(pair)
                else:
                    plan.indirect_pairs.add(pair)
                continue
            plan.column_pairs.add(pair)
    ordered = sorted(plans.values(), key=lambda p: (p.downstream.display(), p.upstream.display()))
    return ordered, report


def render_plan(plans: list[EdgePlan], report: PlanReport, column_lineage: bool = True) -> str:
    lines = [f"{report.procedures} procedure(s), {len(plans)} table pair(s) as written; facts by kind: "
             + ", ".join(f"{k}={v}" for k, v in sorted(report.facts_by_kind.items()))]
    lines.append(f"not pushed: {report.skipped_constant} CONSTANT, {report.skipped_call} CALL, "
                 f"{report.skipped_self_loop} self-referencing")
    if not column_lineage:
        lines.append("--no-column-lineage: table-level edges only; existing column lineage is kept")
    if report.failed_procedures:
        lines.append("procedures the evaluator could not analyze: " + ", ".join(report.failed_procedures))
    for p in plans:
        lines.append("")
        labelled = sum(len(v) for v in p.labelled_pairs.values())
        lines.append(f"{p.upstream.display()} -> {p.downstream.display()}   "
                     f"[{len(p.column_pairs) + labelled} column mapping(s); "
                     f"from {', '.join(sorted(p.contributors))}]")
        for src, tgt in sorted(p.column_pairs):
            lines.append(f"    {src} -> {tgt}")
        for label, pairs in sorted(p.labelled_pairs.items()):
            for src, tgt in sorted(pairs):
                lines.append(f"    {src} -> {tgt}   ({label})")
        for src, tgt in sorted(p.indirect_pairs):
            lines.append(f"    {src} -> {tgt}   (indirect: table-level only, not in columnsLineage)")
    return "\n".join(lines)


# --------------------------------------------------------------------------- column resolution


def _decode_identifier(name: str) -> Optional[str]:
    """SQL-quoted identifier -> its bare value ([a]]b] -> a]b, "a""b" -> a"b, `a``b` -> a`b)."""
    for open_, close in (("[", "]"), ('"', '"'), ("`", "`")):
        if len(name) >= 2 and name[0] == open_ and name[-1] == close:
            return name[1:-1].replace(close * 2, close)
    return None


def resolve_columns(pairs: set[tuple[str, str]], up_entity: dict, down_entity: dict,
                    function: Optional[str] = None) -> tuple[list[dict], list[str]]:
    """Map name pairs to column FQNs (entries carry ``function`` when one is given).

    Order: the name exactly as written; then its SQL-decoded form (so a literal column named
    ``[x]`` wins over ``x``); then a case-insensitive match, accepted only when both
    interpretations together match exactly ONE column. A pair is rejected when a column is absent
    or ambiguous — pushing a guess would attach lineage to the wrong column.
    """
    def index(entity: dict) -> tuple[dict, dict]:
        exact, folded = {}, {}
        for col in entity.get("columns", []):
            exact[col["name"]] = col["fullyQualifiedName"]
            folded.setdefault(col["name"].lower(), []).append(col["fullyQualifiedName"])
        return exact, folded

    def find(name: str, idx: tuple[dict, dict]) -> Optional[str]:
        exact, folded = idx
        candidates = [name] + [d for d in (_decode_identifier(name),) if d is not None]
        for c in candidates:
            if c in exact:
                return exact[c]
        # fallback: the case-insensitive matches of BOTH interpretations together must name exactly
        # one column (catalog columns "[X]" and "X" make "[x]" ambiguous)
        hits = {h for c in candidates for h in folded.get(c.lower(), [])}
        return hits.pop() if len(hits) == 1 else None

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
    return [_entry(sorted(s), t, function) for t, s in sorted(by_target.items())], rejected


def _entry(from_columns: list[str], to_column: str, function: Optional[str]) -> dict:
    entry = {"fromColumns": from_columns, "toColumn": to_column}
    if function:
        entry["function"] = function
    return entry


# --------------------------------------------------------------------------- merge (no I/O)


def _contributor_line(names: list[str]) -> str:
    return MARKER_PREFIX + json.dumps(names, ensure_ascii=False)


def _merge_description(description: Optional[str], contributors: set[str]) -> Optional[str]:
    lines = (description or "").splitlines()
    idx = next((i for i, line in enumerate(lines) if line.startswith(MARKER_PREFIX)), None)
    if idx is None:
        names = sorted(contributors)[:MAX_CONTRIBUTORS]
        return "\n".join(lines + [_contributor_line(names)]) if names else description
    try:
        known = json.loads(lines[idx][len(MARKER_PREFIX):])
        if not isinstance(known, list) or not all(isinstance(n, str) for n in known):
            raise ValueError
    except ValueError:
        logger.warning("contributor marker line is not valid JSON; leaving it untouched")
        return description
    merged = sorted(set(known) | contributors)
    if len(merged) > MAX_CONTRIBUTORS:
        logger.warning("more than %d contributing procedures; list truncated", MAX_CONTRIBUTORS)
        merged = (sorted(set(known)) + sorted(contributors - set(known)))[:MAX_CONTRIBUTORS]
    lines[idx] = _contributor_line(merged)
    return "\n".join(lines)


def merge_details(existing: Optional[dict], ours: list[dict], contributors: set[str]) -> dict:
    """Add our column lineage and contributor marker to the edge's existing lineageDetails.

    Every existing field and every existing ``columnsLineage`` entry is kept verbatim — none is
    modified, merged or re-ordered. For each target, the source columns no existing entry already
    covers are appended as ONE new entry of their own (one per ``function`` label); mappings already
    present are not repeated. Unlabelled (value) entries go first, so a column that is both a value
    and, say, a CASE WHEN condition is recorded once, as a value. Across pushes, a value mapping is
    not covered by an entry carrying one of THIS tool's indirect labels (it is appended next to that
    unmodified entry); any other entry — a value, or a native one with a SQL function — covers it.
    """
    details = {k: v for k, v in (existing or {}).items() if k not in _NOT_DETAILS}
    entries = list(details.get("columnsLineage") or [])
    for entry in sorted(ours, key=lambda e: (e.get("function") is not None, e.get("function") or "",
                                             e["toColumn"])):
        target = entry["toColumn"]
        # a value mapping is not "covered" by an indirect label this tool wrote earlier (another
        # push): it is appended as a value entry of its own; the labelled entry stays unmodified
        labelled = entry.get("function") is not None
        covered = {f for e in entries if e.get("toColumn") == target
                   and (labelled or e.get("function") not in _OUR_LABELS)
                   for f in (e.get("fromColumns") or [])}
        new = sorted(set(entry["fromColumns"]) - covered)
        if new:
            entries.append(_entry(new, target, entry.get("function")))
    if entries:
        details["columnsLineage"] = entries
    description = _merge_description(details.get("description"), contributors)
    if description is not None:
        details["description"] = description
    details.setdefault("source", "QueryLineage")
    return details


def _normalized(details: Optional[dict]) -> str:
    """Order-insensitive fingerprint of details, ignoring server-managed fields."""
    d = {k: v for k, v in (details or {}).items() if k not in _NOT_DETAILS}
    d["columnsLineage"] = sorted(
        json.dumps({**c, "fromColumns": sorted(c.get("fromColumns") or [])}, sort_keys=True)
        for c in d.get("columnsLineage") or [])
    return json.dumps(d, sort_keys=True)


def edge_details_from_response(body: Optional[dict]) -> Optional[dict]:
    """The lineageDetails of a getLineageEdge response, or None when the edge carries none.

    OpenMetadata 2.0.x returns them FLAT under "edge" ({"edge": {"columnsLineage": ...}});
    older payloads nest them under "edge.lineageDetails". Both shapes are accepted, and every
    field except the edge's endpoints and server timestamps is kept — including ones this
    version does not know — so a round trip cannot silently drop them.
    """
    edge = (body or {}).get("edge", body) or {}
    if isinstance(edge.get("lineageDetails"), dict):
        return edge["lineageDetails"] or None
    details = {k: v for k, v in edge.items() if k not in _NOT_DETAILS}
    return details or None


# --------------------------------------------------------------------------- OpenMetadata I/O


def fqn_part(name: str) -> str:
    """OpenMetadata FQN quoting: a segment containing '.' is wrapped in double quotes."""
    if "." in name and not (name.startswith('"') and name.endswith('"')):
        return f'"{name}"'
    return name


class Catalog:
    """Table lookups with one shared HTTP session and a per-run cache.

    ``table()`` returns the entity (with columns), ``None`` when the table does not exist or its
    name is ambiguous, and raises LookupFailed when OpenMetadata could not be asked.
    """

    def __init__(self, config: OpenMetadataConfig):
        self.base_url = config.server.rstrip("/")
        self.session = requests.Session()
        if config.token:
            self.session.headers["Authorization"] = f"Bearer {config.token}"
        self._tables: dict[str, Optional[dict]] = {}
        self.ambiguous: set[str] = set()

    def _get(self, path: str, **params) -> requests.Response:
        try:
            resp = self.session.get(f"{self.base_url}{path}", params=params or None, timeout=30)
        except requests.RequestException as e:
            raise LookupFailed(f"GET {path}: {e}") from e
        if resp.status_code not in (200, 404):
            raise LookupFailed(f"GET {path}: HTTP {resp.status_code} {resp.text[:200]}")
        return resp

    def _by_fqn(self, fqn: str) -> Optional[dict]:
        resp = self._get(f"/v1/tables/name/{quote(fqn, safe='')}", fields="columns")
        return resp.json() if resp.status_code == 200 else None

    def table(self, fqn: str) -> Optional[dict]:
        if fqn in self._tables:
            return self._tables[fqn]
        entity = self._by_fqn(fqn)
        if entity is None:
            # The catalog may store another case (SQL Server is usually case-insensitive): accept a
            # search hit only when exactly one table matches case-insensitively, and re-read it by
            # its canonical FQN so its columns are present.
            escaped = fqn.replace("\\", "\\\\").replace('"', '\\"')
            resp = self._get("/v1/search/query", q=f'fullyQualifiedName:"{escaped}"',
                             index="table_search_index", size=10)
            hits = (resp.json().get("hits", {}).get("hits", []) if resp.status_code == 200 else [])
            names = {h.get("_source", {}).get("fullyQualifiedName", "") for h in hits}
            matches = sorted(n for n in names if n.lower() == fqn.lower())
            if len(matches) == 1:
                entity = self._by_fqn(matches[0])
            elif len(matches) > 1:
                self.ambiguous.add(fqn)
        self._tables[fqn] = entity
        return entity

    def edge_details(self, from_id: str, to_id: str) -> Optional[dict]:
        resp = self._get(f"/v1/lineage/getLineageEdge/{from_id}/{to_id}")
        return edge_details_from_response(resp.json()) if resp.status_code == 200 else None

    def put_edge(self, payload: dict) -> bool:
        try:
            resp = self.session.put(f"{self.base_url}/v1/lineage", json=payload, timeout=30)
        except requests.RequestException as e:
            logger.error("PUT /v1/lineage failed: %s", e)
            return False
        if resp.status_code not in (200, 201):
            logger.error("PUT /v1/lineage: HTTP %d %s", resp.status_code, resp.text[:500])
            return False
        return True


@dataclass
class PushResult:
    written: int = 0
    unchanged: int = 0
    failed: int = 0
    unresolved_tables: set[str] = field(default_factory=set)
    rejected_columns: list[str] = field(default_factory=list)
    lookup_errors: list[str] = field(default_factory=list)

    def exit_code(self) -> int:
        if self.failed or self.lookup_errors:
            return EXIT_FAILED
        if self.unresolved_tables or self.rejected_columns:
            return EXIT_INCOMPLETE
        return EXIT_OK


@dataclass
class _Group:
    # (toColumn fqn, function label or None) -> fromColumn fqns
    columns: dict[tuple[str, Optional[str]], set[str]] = field(default_factory=dict)
    contributors: set[str] = field(default_factory=set)


def push(catalog: Catalog, service: str, plans: list[EdgePlan], column_lineage: bool = True) -> PushResult:
    result = PushResult()
    groups: dict[tuple[str, str], _Group] = {}
    for plan in plans:
        ends = []
        for ref in (plan.upstream, plan.downstream):
            fqn = ".".join(fqn_part(p) for p in (service, ref.database, ref.schema, ref.table) if p)
            try:
                entity = catalog.table(fqn)
                if entity is None:
                    result.unresolved_tables.add(
                        ref.display() + (" (ambiguous)" if fqn in catalog.ambiguous else ""))
            except LookupFailed as e:
                result.lookup_errors.append(str(e))
                entity = None
            ends.append(entity)
        up, down = ends
        if up is None or down is None or up["id"] == down["id"]:
            continue
        group = groups.setdefault((up["id"], down["id"]), _Group())
        group.contributors |= plan.contributors
        if column_lineage:
            for function, pairs in [(None, plan.column_pairs)] + sorted(plan.labelled_pairs.items()):
                ours, rejected = resolve_columns(pairs, up, down, function)
                result.rejected_columns += [f"{plan.upstream.display()} -> {plan.downstream.display()}: {r}"
                                            for r in rejected]
                for entry in ours:
                    group.columns.setdefault((entry["toColumn"], function), set()).update(entry["fromColumns"])

    for (up_id, down_id), g in groups.items():
        ours = [_entry(sorted(s), t, f) for (t, f), s in sorted(g.columns.items(), key=lambda kv: (kv[0][0], kv[0][1] or ""))]
        try:
            existing = catalog.edge_details(up_id, down_id)
        except LookupFailed as e:
            result.lookup_errors.append(str(e))
            continue
        merged = merge_details(existing, ours, g.contributors)
        if existing is not None and _normalized(existing) == _normalized(merged):
            result.unchanged += 1
            continue
        payload = {"edge": {"fromEntity": {"id": up_id, "type": "table"},
                            "toEntity": {"id": down_id, "type": "table"},
                            "lineageDetails": merged}}
        if catalog.put_edge(payload):
            result.written += 1
        else:
            result.failed += 1
    return result


def run(config: SidecarConfig, path: str, dry_run: bool) -> int:
    """Entry point for ``--from-lineage-json``. Returns the process exit code:
    0 all written, 1 unusable input, 2 a lookup or write failed, 3 some tables/columns unresolved."""
    om = config.openmetadata
    if om.indirect_columns not in PUSHED_INDIRECT:
        logger.error("indirect_columns must be one of %s, got %r", sorted(PUSHED_INDIRECT), om.indirect_columns)
        return EXIT_INPUT
    try:
        doc = load_lineage_json(path)
    except LineageJsonError as e:
        logger.error("%s", e)
        return EXIT_INPUT
    plans, report = plan_edges(doc, om.database_name, om.schema_name, om.indirect_columns)
    print(render_plan(plans, report, om.column_lineage))
    if dry_run:
        return EXIT_OK
    result = push(Catalog(om), om.service_name, plans, om.column_lineage)
    print(f"\nOpenMetadata: {result.written} edge(s) written, {result.unchanged} unchanged, "
          f"{result.failed} failed")
    for e in result.lookup_errors:
        print("lookup failed: " + e)
    if result.unresolved_tables:
        print("tables not found in OpenMetadata (edges skipped): " + ", ".join(sorted(result.unresolved_tables)))
    for r in result.rejected_columns:
        print("column mapping rejected: " + r)
    return result.exit_code()
