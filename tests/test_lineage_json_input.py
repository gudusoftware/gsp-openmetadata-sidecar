"""--from-lineage-json: planning, column resolution, read-merge-write, and the no-backend guarantee."""

from __future__ import annotations

import json
import sys

import pytest
import responses

from gsp_openmetadata_sidecar import cli, lineage_json_input as lj
from gsp_openmetadata_sidecar.config import OpenMetadataConfig
from gsp_openmetadata_sidecar.emitter import OpenMetadataClient

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


# --------------------------------------------------------------------------- loading / planning

def test_rejects_other_contracts(tmp_path):
    f = tmp_path / "x.json"
    f.write_text(json.dumps({"contract": "lineage-eval.v2", "procedures": []}))
    with pytest.raises(lj.LineageJsonError, match="not a lineage-eval.v1"):
        lj.load_lineage_json(str(f))


def test_two_procedures_on_one_table_pair_become_one_edge_with_all_columns():
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


def test_table_names_differing_only_in_case_are_one_table():
    plans, _ = lj.plan_edges(doc(
        ("a", [edge("COLUMN", ep("SalesDB", "dbo", "Customers", "x"), ep(*P, "x"))]),
        ("b", [edge("COLUMN", ep("SalesDB", "dbo", "customers", "y"), ep(*P, "y"))])))
    assert len(plans) == 1 and len(plans[0].column_pairs) == 2


def test_aggregate_pairs_are_pushed_but_labelled():
    plans, _ = lj.plan_edges(doc(("v", [
        edge("COLUMN", ep("Sales", "dbo", "Invoices", "InvoiceDate"),
             ep("Analytics", "dbo", "vw", "TotalRevenue"), role="AGGREGATE")])))
    assert plans[0].column_pairs == plans[0].aggregate_pairs == {("InvoiceDate", "TotalRevenue")}
    assert "AGGREGATE" in lj.render_plan(plans, lj.PlanReport())


def test_indirect_falls_back_to_role_when_the_flag_is_absent():
    plans, _ = lj.plan_edges(doc(("p", [edge("COLUMN", ep(*C, "a"), ep(*P, "b"), role="FILTER")])))
    assert plans[0].column_pairs == set() and plans[0].indirect_pairs == {("a", "b")}


def test_unknown_kind_is_an_error():
    with pytest.raises(lj.LineageJsonError, match="unknown fact kind"):
        lj.plan_edges(doc(("p", [edge("SOMETHING", ep(*C, "a"), ep(*P, "b"))])))


def test_missing_database_and_schema_use_config_defaults():
    plans, _ = lj.plan_edges(doc(("p", [edge("TABLE", ep(None, None, "t1"), ep(None, None, "t2"))])),
                             default_db="SalesDB", default_schema="dbo")
    assert plans[0].upstream == lj.TableRef("SalesDB", "dbo", "t1")


# --------------------------------------------------------------------------- column resolution

def entity(fqn, *cols):
    return {"id": fqn, "fullyQualifiedName": fqn,
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


# --------------------------------------------------------------------------- merge

EXISTING = {
    "sqlQuery": "INSERT INTO b SELECT a FROM a",
    "source": "ViewLineage",
    "description": "added by the connector",
    "columnsLineage": [{"fromColumns": ["s.a.x"], "toColumn": "s.b.x", "function": "UPPER"}],
    "createdAt": 1, "updatedBy": "ingestion-bot",
}


def test_merge_keeps_everything_already_on_the_edge():
    merged = lj.merge_details(EXISTING, [{"fromColumns": ["s.a.y"], "toColumn": "s.b.x"},
                                         {"fromColumns": ["s.a.z"], "toColumn": "s.b.z"}], {"p1"})
    assert merged["sqlQuery"] == EXISTING["sqlQuery"] and merged["source"] == "ViewLineage"
    assert merged["columnsLineage"] == [
        {"fromColumns": ["s.a.x", "s.a.y"], "toColumn": "s.b.x", "function": "UPPER"},
        {"fromColumns": ["s.a.z"], "toColumn": "s.b.z"}]
    assert merged["description"] == "added by the connector\n" + lj.DESCRIPTION_MARKER + "p1"
    assert "createdAt" not in merged and "updatedBy" not in merged


def test_merge_is_idempotent_and_extends_the_contributor_line():
    once = lj.merge_details(EXISTING, [{"fromColumns": ["s.a.y"], "toColumn": "s.b.x"}], {"p1"})
    twice = lj.merge_details(once, [{"fromColumns": ["s.a.y"], "toColumn": "s.b.x"}], {"p1"})
    assert lj._normalized(once) == lj._normalized(twice)
    more = lj.merge_details(once, [], {"p2"})
    assert more["description"].count(lj.DESCRIPTION_MARKER) == 1
    assert more["description"].endswith(lj.DESCRIPTION_MARKER + "p1, p2")


def test_new_edge_gets_query_lineage_source():
    assert lj.merge_details(None, [], {"p"})["source"] == "QueryLineage"


# --------------------------------------------------------------------------- push (HTTP mocked)

def _mock_tables(rsps):
    for fqn, cols in (("svc.SalesDB.dbo.customers", ("customer_id", "customer_name", "region", "credit_limit")),
                      ("svc.SalesDB.dbo.customer_profile", ("customer_id", "display_name", "region", "credit_band"))):
        rsps.get(f"{OM}/v1/tables/name/{fqn}", json={**entity(fqn, *cols), "id": fqn.split(".")[-1]})


@responses.activate
def test_push_merges_with_existing_edge_then_is_a_no_op_on_rerun():
    _mock_tables(responses)
    # existing lineage from OpenMetadata's own ingestion: only customer_id
    existing = {"source": "QueryLineage", "sqlQuery": "native",
                "columnsLineage": [{"fromColumns": ["svc.SalesDB.dbo.customers.customer_id"],
                                    "toColumn": "svc.SalesDB.dbo.customer_profile.customer_id"}]}
    responses.get(f"{OM}/v1/lineage/getLineageEdge/customers/customer_profile",
                  json={"edge": {"lineageDetails": existing}})
    put = responses.put(f"{OM}/v1/lineage", json={})
    client = OpenMetadataClient(OpenMetadataConfig(server=OM, service_name="svc"))
    plans, _ = lj.plan_edges(TWO_PROCS_ONE_PAIR)

    result = lj.push(client, "svc", plans)
    assert (result.written, result.unchanged, result.failed) == (1, 0, 0)
    sent = json.loads(put.calls[0].request.body)["edge"]["lineageDetails"]
    assert sent["sqlQuery"] == "native"                        # existing details survive
    assert {c["toColumn"].rsplit(".", 1)[1] for c in sent["columnsLineage"]} == {
        "customer_id", "display_name", "region"}               # both procedures' columns, credit_band excluded

    # second run: the server now holds exactly what we sent -> nothing is written
    responses.replace(responses.GET, f"{OM}/v1/lineage/getLineageEdge/customers/customer_profile",
                      json={"edge": {"lineageDetails": sent}})
    again = lj.push(client, "svc", plans)
    assert (again.written, again.unchanged) == (0, 1)
    assert len(put.calls) == 1


@responses.activate
def test_search_hit_for_another_table_is_not_used():
    responses.get(f"{OM}/v1/tables/name/svc.SalesDB.dbo.customers", status=404)
    responses.get(f"{OM}/v1/search/query", json={"hits": {"hits": [
        {"_source": {"id": "x", "fullyQualifiedName": "svc.OtherDB.dbo.customers_archive"}}]}})
    client = OpenMetadataClient(OpenMetadataConfig(server=OM, service_name="svc"))
    assert lj._resolve_table(client, "svc", lj.TableRef("SalesDB", "dbo", "customers")) is None


# --------------------------------------------------------------------------- CLI: no backend, ever

def test_json_mode_never_creates_a_sqlflow_backend(tmp_path, monkeypatch, capsys):
    f = tmp_path / "lineage.json"
    f.write_text(json.dumps(TWO_PROCS_ONE_PAIR))

    def forbidden(*a, **k):
        raise AssertionError("--from-lineage-json must not construct a SQLFlow backend")
    monkeypatch.setattr(cli, "create_backend", forbidden)
    monkeypatch.setattr(sys, "argv", ["gsp-openmetadata-sidecar", "--config", str(tmp_path / "none.yaml"),
                                      "--from-lineage-json", str(f), "--dry-run"])
    with pytest.raises(SystemExit) as exit_:
        cli.main()
    assert exit_.value.code == 0
    assert "SalesDB.dbo.customers -> SalesDB.dbo.customer_profile" in capsys.readouterr().out


def test_json_mode_rejects_auto_create(tmp_path, monkeypatch):
    f = tmp_path / "lineage.json"
    f.write_text(json.dumps(TWO_PROCS_ONE_PAIR))
    monkeypatch.setattr(sys, "argv", ["gsp-openmetadata-sidecar", "--config", str(tmp_path / "none.yaml"),
                                      "--from-lineage-json", str(f), "--auto-create-entities"])
    with pytest.raises(SystemExit) as exit_:
        cli.main()
    assert exit_.value.code == 1
