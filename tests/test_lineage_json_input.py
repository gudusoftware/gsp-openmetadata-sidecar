"""--from-lineage-json: validation, planning, column resolution, merge, push and the CLI path."""

from __future__ import annotations

import json
import sys
from urllib.parse import quote

import pytest
import responses

from gsp_openmetadata_sidecar import cli, lineage_json_input as lj
from gsp_openmetadata_sidecar.config import OpenMetadataConfig, SidecarConfig

OM = "http://om.test/api"


def ep(db, schema, table, column=None):
    return {"database": db, "schema": schema, "table": table, "column": column}


def edge(kind, source, target, role="DIRECT", indirect=None):
    e = {"kind": kind, "source": source, "target": target, "role": role}
    if indirect is not None:
        e["indirect"] = indirect
    return e


def doc(*procedures):
    return {"contract": lj.CONTRACT, "procedures": [
        {"name": name, "status": "OK", "edges": list(edges), "issues": []} for name, edges in procedures]}


C = ("SalesDB", "dbo", "customers")
P = ("SalesDB", "dbo", "customer_profile")

TWO_PROCS_ONE_PAIR = doc(
    ("SalesDB.dbo.usp_ProfileNames", [
        edge("COLUMN", ep(*C, "customer_id"), ep(*P, "customer_id")),
        edge("COLUMN", ep(*C, "customer_name"), ep(*P, "display_name")),
    ]),
    ("SalesDB.dbo.usp_ProfileRegion", [
        edge("COLUMN", ep(*C, "region"), ep(*P, "region"), role="TRANSFORM"),
        edge("COLUMN", ep(*C, "credit_limit"), ep(*P, "credit_band"), role="CONDITION", indirect=True),
        edge("ROW_LEVEL", ep(*C, "customer_id"), ep(*P), role="JOIN", indirect=True),
        edge("ROW_LEVEL", ep(*P, "customer_id"), ep(*P), role="JOIN", indirect=True),  # self-loop
    ]),
)


# --------------------------------------------------------------------------- validation

@pytest.mark.parametrize("bad, message", [
    ({"contract": "lineage-eval.v2", "procedures": []}, "not a lineage-eval.v1"),
    ({"contract": lj.CONTRACT, "procedures": {}}, "must be a list"),
    (doc(("p", [edge("SOMETHING", ep(*C, "a"), ep(*P, "b"))])), "unknown fact kind"),
    (doc(("p", [edge("COLUMN", ep(*C, None), ep(*P, "b"))])), "column on both sides"),
    (doc(("p", [edge("COLUMN", ep(*C, "a"), ep(*P, "  "))])), "column on both sides"),
    (doc(("p", [edge("ROW_LEVEL", ep(*C, "a"), ep(*P, "b"))])), "exactly one side"),
    (doc(("p", [edge("TABLE", ep(*C, "a"), ep(*P))])), "must not carry columns"),
    (doc(("p", [edge("CONSTANT", ep(None, None, None), ep(*P))])), "CONSTANT needs a target column"),
    (doc(("p", [edge("TABLE", ep(*C), ep(None, None, None))])), "target.table is required"),
    (doc(("p", [edge("COLUMN", ep(*C, "a"), ep(*P, "b"), indirect="yes")])), "must be a boolean"),
    ({"contract": lj.CONTRACT, "procedures": [{"status": "MAYBE", "edges": []}]}, "status must be"),
])
def test_malformed_documents_are_rejected_before_anything_is_sent(bad, message):
    with pytest.raises(lj.LineageJsonError, match=message):
        lj.validate_document(bad)


@responses.activate  # no URL registered: any HTTP call would raise
def test_invalid_file_exits_1_without_network(tmp_path):
    f = tmp_path / "x.json"
    f.write_text(json.dumps(doc(("p", [edge("COLUMN", ep(*C, None), ep(*P, "b"))]))))
    assert lj.run(SidecarConfig(), str(f), dry_run=False) == lj.EXIT_INPUT
    assert len(responses.calls) == 0


def test_undecodable_file_is_an_input_error(tmp_path):
    f = tmp_path / "x.json"
    f.write_bytes(b"\xff\xfe\x00garbage")
    with pytest.raises(lj.LineageJsonError, match="cannot read"):
        lj.load_lineage_json(str(f))


# --------------------------------------------------------------------------- planning

def test_two_procedures_on_one_table_pair_become_one_plan_with_all_columns():
    plans, report = lj.plan_edges(TWO_PROCS_ONE_PAIR)
    assert len(plans) == 1
    p = plans[0]
    assert p.column_pairs == {("customer_id", "customer_id"), ("customer_name", "display_name"), ("region", "region")}
    assert p.indirect_pairs == {("credit_limit", "credit_band")}
    assert p.contributors == {"SalesDB.dbo.usp_ProfileNames", "SalesDB.dbo.usp_ProfileRegion"}
    assert report.skipped_self_loop == 1


def test_constant_and_call_facts_are_never_planned():
    plans, report = lj.plan_edges(doc(("p", [
        edge("CONSTANT", ep(None, None, None), ep(*P, "credit_band"), role="DERIVED"),
        edge("CALL", ep("SalesDB", "dbo", "callee"), ep("SalesDB", "dbo", "caller"), role="LOOKUP"),
    ])))
    assert plans == []
    assert (report.skipped_constant, report.skipped_call) == (1, 1)


def test_names_differing_only_in_case_stay_separate_until_the_catalog_decides():
    plans, _ = lj.plan_edges(doc(
        ("a", [edge("COLUMN", ep("SalesDB", "dbo", "Customers", "x"), ep(*P, "x"))]),
        ("b", [edge("COLUMN", ep("SalesDB", "dbo", "customers", "y"), ep(*P, "y"))])))
    assert len(plans) == 2      # a case-sensitive database may really have both tables


def test_aggregate_pairs_are_pushed_but_labelled():
    plans, _ = lj.plan_edges(doc(("v", [
        edge("COLUMN", ep("Sales", "dbo", "Invoices", "InvoiceDate"),
             ep("Analytics", "dbo", "vw", "TotalRevenue"), role="AGGREGATE")])))
    assert plans[0].column_pairs == plans[0].aggregate_pairs == {("InvoiceDate", "TotalRevenue")}
    assert "AGGREGATE" in lj.render_plan(plans, lj.PlanReport())


def test_indirect_falls_back_to_role_when_the_flag_is_absent():
    plans, _ = lj.plan_edges(doc(("p", [edge("COLUMN", ep(*C, "a"), ep(*P, "b"), role="FILTER")])))
    assert plans[0].column_pairs == set() and plans[0].indirect_pairs == {("a", "b")}


def test_missing_database_and_schema_use_config_defaults():
    plans, _ = lj.plan_edges(doc(("p", [edge("TABLE", ep(None, None, "t1"), ep(None, None, "t2"))])),
                             default_db="SalesDB", default_schema="dbo")
    assert plans[0].upstream == lj.TableRef("SalesDB", "dbo", "t1")


# --------------------------------------------------------------------------- column resolution

def entity(fqn, *cols, id_=None):
    return {"id": id_ or fqn, "fullyQualifiedName": fqn,
            "columns": [{"name": c, "fullyQualifiedName": f"{fqn}.{c}"} for c in cols]}


def test_columns_resolve_exactly_then_by_unique_case_insensitive_match():
    up, down = entity("svc.db.dbo.a", "CustomerID", "Name"), entity("svc.db.dbo.b", "customer_id", "name")
    cols, rejected = lj.resolve_columns({("CustomerID", "CUSTOMER_ID"), ("name", "Name")}, up, down)
    assert cols == [{"fromColumns": ["svc.db.dbo.a.CustomerID"], "toColumn": "svc.db.dbo.b.customer_id"},
                    {"fromColumns": ["svc.db.dbo.a.Name"], "toColumn": "svc.db.dbo.b.name"}]
    assert rejected == []


def test_ambiguous_or_missing_columns_are_rejected_not_guessed():
    up, down = entity("svc.db.dbo.a", "Code", "CODE"), entity("svc.db.dbo.b", "code")
    cols, rejected = lj.resolve_columns({("code", "code"), ("missing", "code")}, up, down)
    assert cols == []
    assert len(rejected) == 2 and all("no unique column" in r for r in rejected)


def test_a_literal_bracketed_column_name_wins_over_the_unquoted_one():
    up, down = entity("s.d.o.a", "[x]", "x"), entity("s.d.o.b", "y")
    cols, _ = lj.resolve_columns({("[x]", "y")}, up, down)
    assert cols[0]["fromColumns"] == ["s.d.o.a.[x]"]


def test_sql_quoting_and_escapes_are_decoded():
    up = entity("s.d.o.a", "a]b", "Resume")
    down = entity("s.d.o.v", "Addr.Loc.City", 'q"t')
    cols, rejected = lj.resolve_columns({("[a]]b]", "[Addr.Loc.City]"), ("Resume", '"q""t"')}, up, down)
    assert rejected == []
    assert {c["toColumn"]: c["fromColumns"] for c in cols} == {
        "s.d.o.v.Addr.Loc.City": ["s.d.o.a.a]b"], 's.d.o.v.q"t': ["s.d.o.a.Resume"]}


# --------------------------------------------------------------------------- merge

EXISTING = {
    "sqlQuery": "INSERT INTO b SELECT a FROM a",
    "source": "ViewLineage",
    "description": "added by the connector",
    "pipeline": {"id": "p1", "type": "pipeline"},
    "assetEdges": [{"a": 1}], "tempLineageTables": ["#t"], "futureField": 42,
    "columnsLineage": [{"fromColumns": ["s.a.x"], "toColumn": "s.b.x", "function": "UPPER"},
                       {"fromColumns": ["s.a.w"], "toColumn": "s.b.x", "function": "LOWER"}],
    "createdAt": 1, "updatedBy": "ingestion-bot",
}


def test_merge_keeps_every_existing_field_and_entry_verbatim():
    merged = lj.merge_details(EXISTING, [], {"p1"})
    for k in ("sqlQuery", "source", "pipeline", "assetEdges", "tempLineageTables", "futureField"):
        assert merged[k] == EXISTING[k]
    assert merged["columnsLineage"] == EXISTING["columnsLineage"]      # both entries for s.b.x survive
    assert "createdAt" not in merged and "updatedBy" not in merged


def test_merge_never_extends_an_entry_that_has_a_function():
    merged = lj.merge_details(EXISTING, [{"fromColumns": ["s.a.y"], "toColumn": "s.b.x"}], {"p"})
    assert merged["columnsLineage"][:2] == EXISTING["columnsLineage"]
    assert merged["columnsLineage"][2] == {"fromColumns": ["s.a.y"], "toColumn": "s.b.x"}


def test_merge_does_not_repeat_a_mapping_already_present_and_extends_plain_entries():
    existing = {"columnsLineage": [{"fromColumns": ["s.a.x"], "toColumn": "s.b.x"}]}
    merged = lj.merge_details(existing, [{"fromColumns": ["s.a.x", "s.a.z"], "toColumn": "s.b.x"}], {"p"})
    assert merged["columnsLineage"] == [{"fromColumns": ["s.a.x", "s.a.z"], "toColumn": "s.b.x"}]


def test_merge_is_idempotent_and_contributors_with_commas_round_trip():
    once = lj.merge_details(EXISTING, [{"fromColumns": ["s.a.y"], "toColumn": "s.b.z"}], {"db.dbo.p,a"})
    twice = lj.merge_details(once, [{"fromColumns": ["s.a.y"], "toColumn": "s.b.z"}], {"db.dbo.p,a"})
    assert lj._normalized(once) == lj._normalized(twice)
    more = lj.merge_details(once, [], {'q"2'})
    marker = [line for line in more["description"].splitlines() if line.startswith(lj.MARKER_PREFIX)]
    assert len(marker) == 1 and json.loads(marker[0][len(lj.MARKER_PREFIX):]) == ["db.dbo.p,a", 'q"2']
    assert more["description"].startswith("added by the connector\n")


def test_a_hand_edited_marker_line_is_left_untouched():
    existing = {"description": lj.MARKER_PREFIX + "p1, p2 (edited)"}
    assert lj.merge_details(existing, [], {"p3"})["description"] == existing["description"]


def test_new_edge_gets_query_lineage_source():
    assert lj.merge_details(None, [], {"p"})["source"] == "QueryLineage"


# Recorded from OpenMetadata 2.0.2 (GET /v1/lineage/getLineageEdge/{fromId}/{toId}).
LIVE_2_0_2_EDGE = {"edge": {
    "columnsLineage": [{"fromColumns": ["svc.SalesDB.dbo.customers.customer_id"],
                        "toColumn": "svc.SalesDB.dbo.customer_profile.customer_id"}],
    "description": "d", "source": "QueryLineage",
    "createdAt": 1790440859735, "createdBy": "ingestion-bot",
    "updatedAt": 1790441556980, "updatedBy": "ingestion-bot"}}


def test_edge_details_are_read_from_the_flat_and_the_nested_shape():
    flat = lj.edge_details_from_response(LIVE_2_0_2_EDGE)
    assert flat == {"columnsLineage": LIVE_2_0_2_EDGE["edge"]["columnsLineage"], "description": "d",
                    "source": "QueryLineage"}
    assert lj.edge_details_from_response({"edge": {"fromEntity": {}, "toEntity": {},
                                                   "lineageDetails": {"source": "ViewLineage"}}}) == {"source": "ViewLineage"}
    assert lj.edge_details_from_response({"edge": {}}) is None
    assert lj.edge_details_from_response({"edge": {"fromEntity": {}, "toEntity": {}}}) is None
    assert lj.edge_details_from_response({"edge": {"futureField": 1}}) == {"futureField": 1}


# --------------------------------------------------------------------------- push (HTTP mocked)

def _table(fqn, *cols, id_=None):
    responses.get(f"{OM}/v1/tables/name/{quote(fqn, safe='')}", json=entity(fqn, *cols, id_=id_))


def _profile_tables():
    _table("svc.SalesDB.dbo.customers", "customer_id", "customer_name", "region", "credit_limit", id_="customers")
    _table("svc.SalesDB.dbo.customer_profile", "customer_id", "display_name", "region", "credit_band",
           id_="customer_profile")


def catalog():
    return lj.Catalog(OpenMetadataConfig(server=OM, service_name="svc", token="t"))


@responses.activate
def test_push_merges_with_existing_edge_then_is_a_no_op_on_rerun():
    _profile_tables()
    existing = {"source": "QueryLineage", "sqlQuery": "native",
                "columnsLineage": [{"fromColumns": ["svc.SalesDB.dbo.customers.credit_limit"],
                                    "toColumn": "svc.SalesDB.dbo.customer_profile.credit_band"}]}
    # the FLAT shape OpenMetadata 2.0.2 really returns
    responses.get(f"{OM}/v1/lineage/getLineageEdge/customers/customer_profile",
                  json={"edge": {**existing, "createdAt": 1, "createdBy": "ingestion-bot"}})
    put = responses.put(f"{OM}/v1/lineage", json={})
    plans, _ = lj.plan_edges(TWO_PROCS_ONE_PAIR)

    result = lj.push(catalog(), "svc", plans)
    assert (result.written, result.unchanged, result.failed, result.exit_code()) == (1, 0, 0, lj.EXIT_OK)
    sent = json.loads(put.calls[0].request.body)["edge"]["lineageDetails"]
    assert sent["sqlQuery"] == "native"
    assert {c["toColumn"].rsplit(".", 1)[1] for c in sent["columnsLineage"]} == {
        "credit_band", "customer_id", "display_name", "region"}   # native credit_band kept + ours
    assert json.loads(sent["description"][len(lj.MARKER_PREFIX):]) == [
        "SalesDB.dbo.usp_ProfileNames", "SalesDB.dbo.usp_ProfileRegion"]

    responses.replace(responses.GET, f"{OM}/v1/lineage/getLineageEdge/customers/customer_profile",
                      json={"edge": {**sent, "updatedAt": 2, "updatedBy": "ingestion-bot"}})
    again = lj.push(catalog(), "svc", plans)
    assert (again.written, again.unchanged) == (0, 1)
    assert len(put.calls) == 1


@responses.activate
def test_case_variants_that_resolve_to_one_table_are_written_as_one_edge():
    _profile_tables()
    responses.get(f"{OM}/v1/tables/name/{quote('svc.SalesDB.dbo.Customers', safe='')}", status=404)
    responses.get(f"{OM}/v1/search/query", json={"hits": {"hits": [
        {"_source": {"fullyQualifiedName": "svc.SalesDB.dbo.customers"}}]}})
    responses.get(f"{OM}/v1/lineage/getLineageEdge/customers/customer_profile", status=404)
    put = responses.put(f"{OM}/v1/lineage", json={})
    plans, _ = lj.plan_edges(doc(
        ("a", [edge("COLUMN", ep("SalesDB", "dbo", "Customers", "region"), ep(*P, "region"))]),
        ("b", [edge("COLUMN", ep("SalesDB", "dbo", "customers", "customer_id"), ep(*P, "customer_id"))])))
    result = lj.push(catalog(), "svc", plans)
    assert (result.written, result.exit_code()) == (1, lj.EXIT_OK)
    sent = json.loads(put.calls[0].request.body)["edge"]["lineageDetails"]
    assert {c["toColumn"].rsplit(".", 1)[1] for c in sent["columnsLineage"]} == {"region", "customer_id"}


@responses.activate
def test_no_column_lineage_adds_no_columns_but_keeps_existing_ones():
    _profile_tables()
    existing = {"columnsLineage": [{"fromColumns": ["svc.SalesDB.dbo.customers.region"],
                                    "toColumn": "svc.SalesDB.dbo.customer_profile.region"}]}
    responses.get(f"{OM}/v1/lineage/getLineageEdge/customers/customer_profile", json={"edge": existing})
    put = responses.put(f"{OM}/v1/lineage", json={})
    plans, _ = lj.plan_edges(TWO_PROCS_ONE_PAIR)
    lj.push(catalog(), "svc", plans, column_lineage=False)
    sent = json.loads(put.calls[0].request.body)["edge"]["lineageDetails"]
    assert sent["columnsLineage"] == existing["columnsLineage"]


@responses.activate
def test_ambiguous_case_variants_and_foreign_search_hits_are_unresolved():
    responses.get(f"{OM}/v1/tables/name/{quote('svc.SalesDB.dbo.customers', safe='')}", status=404)
    responses.get(f"{OM}/v1/tables/name/{quote('svc.SalesDB.dbo.customer_profile', safe='')}", status=404)
    responses.get(f"{OM}/v1/search/query", json={"hits": {"hits": [
        {"_source": {"fullyQualifiedName": "svc.SalesDB.dbo.Customers"}},
        {"_source": {"fullyQualifiedName": "svc.SalesDB.dbo.CUSTOMERS"}},
        {"_source": {"fullyQualifiedName": "svc.OtherDB.dbo.customer_profile_archive"}}]}})
    plans, _ = lj.plan_edges(TWO_PROCS_ONE_PAIR)
    result = lj.push(catalog(), "svc", plans)
    assert result.unresolved_tables == {"SalesDB.dbo.customers (ambiguous)", "SalesDB.dbo.customer_profile"}
    assert result.exit_code() == lj.EXIT_INCOMPLETE


@responses.activate
def test_a_lookup_failure_is_a_failure_not_a_missing_table():
    responses.get(f"{OM}/v1/tables/name/{quote('svc.SalesDB.dbo.customers', safe='')}", status=500)
    _table("svc.SalesDB.dbo.customer_profile", "region", id_="customer_profile")
    plans, _ = lj.plan_edges(TWO_PROCS_ONE_PAIR)
    result = lj.push(catalog(), "svc", plans)
    assert result.unresolved_tables == set() and len(result.lookup_errors) == 1
    assert result.exit_code() == lj.EXIT_FAILED


@responses.activate
def test_rejected_columns_make_the_run_incomplete():
    _table("svc.SalesDB.dbo.customers", "customer_id", id_="customers")
    _table("svc.SalesDB.dbo.customer_profile", "customer_id", id_="customer_profile")
    responses.get(f"{OM}/v1/lineage/getLineageEdge/customers/customer_profile", status=404)
    responses.put(f"{OM}/v1/lineage", json={})
    plans, _ = lj.plan_edges(TWO_PROCS_ONE_PAIR)
    result = lj.push(catalog(), "svc", plans)
    assert result.written == 1 and result.rejected_columns
    assert result.exit_code() == lj.EXIT_INCOMPLETE


@responses.activate
def test_names_with_dots_are_fqn_quoted_and_url_encoded():
    fqn = 'svc.SalesDB.dbo."a.b c"'
    responses.get(f"{OM}/v1/tables/name/{quote(fqn, safe='')}", json=entity(fqn, "x", id_="ab"))
    assert lj.fqn_part("a.b c") == '"a.b c"' and lj.fqn_part("plain") == "plain"
    assert catalog().table(fqn)["id"] == "ab"


@responses.activate
def test_each_table_is_looked_up_once_per_run():
    _profile_tables()
    responses.get(f"{OM}/v1/lineage/getLineageEdge/customers/customer_profile", status=404)
    responses.put(f"{OM}/v1/lineage", json={})
    plans, _ = lj.plan_edges(doc(
        ("a", [edge("COLUMN", ep(*C, "region"), ep(*P, "region"))]),
        ("b", [edge("TABLE", ep(*C), ep(*P))])))
    lj.push(catalog(), "svc", plans)
    assert sum("/v1/tables/name/" in c.request.url for c in responses.calls) == 2


# --------------------------------------------------------------------------- CLI

def _cli(monkeypatch, tmp_path, *extra):
    f = tmp_path / "lineage.json"
    f.write_text(json.dumps(TWO_PROCS_ONE_PAIR))
    monkeypatch.setattr(sys, "argv", ["gsp-openmetadata-sidecar", "--config", str(tmp_path / "none.yaml"),
                                      "--from-lineage-json", str(f), *extra])
    with pytest.raises(SystemExit) as exit_:
        cli.main()
    return exit_.value.code


def test_json_mode_never_creates_a_sqlflow_backend(tmp_path, monkeypatch, capsys):
    def forbidden(*a, **k):
        raise AssertionError("--from-lineage-json must not construct a SQLFlow backend")
    monkeypatch.setattr(cli, "create_backend", forbidden)
    assert _cli(monkeypatch, tmp_path, "--dry-run") == 0
    assert "SalesDB.dbo.customers -> SalesDB.dbo.customer_profile" in capsys.readouterr().out


def test_json_mode_rejects_auto_create(tmp_path, monkeypatch):
    assert _cli(monkeypatch, tmp_path, "--auto-create-entities") == 1


def test_no_column_lineage_reaches_the_push_from_the_cli(tmp_path, monkeypatch):
    seen = {}

    def fake_push(catalog, service, plans, column_lineage=True):
        seen["column_lineage"] = column_lineage
        return lj.PushResult()
    monkeypatch.setattr(lj, "push", fake_push)
    assert _cli(monkeypatch, tmp_path, "--no-column-lineage") == 0
    assert seen == {"column_lineage": False}
