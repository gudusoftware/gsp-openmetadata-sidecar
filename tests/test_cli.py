"""Tests for the CLI flag mutex pair and CLI > env > YAML > default precedence.

These tests exercise the argument parser setup and the post-parse merge
logic in ``cli.main`` by invoking ``main()`` with ``sys.argv`` monkeypatched
and a heavily mocked ``emit_lineage`` / backend to short-circuit the actual
network work.
"""

from __future__ import annotations

import json

import pytest

from gsp_openmetadata_sidecar import cli
from gsp_openmetadata_sidecar.entity_planner import EmissionSummary
from gsp_openmetadata_sidecar.lineage_mapper import TableLineage


ENV_VARS_TO_CLEAR = (
    "GSP_OM_AUTO_CREATE_ENTITIES",
    "GSP_OM_ON_CREATE_FAILURE",
    "GSP_OM_MAX_ENTITIES_TO_CREATE",
    "GSP_OM_AUTO_CREATED_TAG_FQN",
    "GSP_DEFAULT_DATABASE",
    "GSP_DEFAULT_SCHEMA",
    "GSP_OM_DATABASE_NAME",
    "GSP_OM_SCHEMA_NAME",
    "GSP_OM_SERVICE_NAME",
    "GSP_OM_SERVER",
    "GSP_OM_TOKEN",
)


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    for name in ENV_VARS_TO_CLEAR:
        monkeypatch.delenv(name, raising=False)
    yield


def _write_yaml(tmp_path, body: str) -> str:
    p = tmp_path / "sidecar.yaml"
    p.write_text(body)
    return str(p)


def _stub_pipeline(monkeypatch, captured: dict):
    """Short-circuit the pipeline so ``main`` doesn't call SQLFlow / OM."""
    class FakeBackend:
        def get_lineage(self, **kw):
            return {"code": 200}

    monkeypatch.setattr(cli, "create_backend", lambda cfg: FakeBackend())
    monkeypatch.setattr(cli, "extract_lineage", lambda *a, **k: [
        TableLineage(upstream_table="a.b.c", downstream_table="a.b.d"),
    ])

    def fake_emit(lineages, sql_query, config, dry_run=False):
        captured["config"] = config
        captured["dry_run"] = dry_run
        return EmissionSummary(emitted_edges=len(lineages))

    monkeypatch.setattr(cli, "emit_lineage", fake_emit)


def test_auto_create_cli_flag_overrides_yaml_false(tmp_path, monkeypatch):
    """CLI --auto-create-entities beats yaml `auto_create_entities: false`."""
    cfg_path = _write_yaml(
        tmp_path,
        """
sqlflow:
  mode: anonymous
  default_database: SalesDB
  default_schema: dbo
openmetadata:
  service_name: mssql_prod
  auto_create_entities: false
""",
    )
    captured: dict = {}
    _stub_pipeline(monkeypatch, captured)
    monkeypatch.setattr(
        "sys.argv",
        ["gsp-openmetadata-sidecar", "--config", cfg_path,
         "--sql", "SELECT 1", "--auto-create-entities", "--dry-run"],
    )
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 0
    assert captured["config"].auto_create_entities is True


def test_no_auto_create_cli_flag_disables_yaml_true(tmp_path, monkeypatch):
    cfg_path = _write_yaml(
        tmp_path,
        """
sqlflow:
  mode: anonymous
  default_database: SalesDB
  default_schema: dbo
openmetadata:
  service_name: mssql_prod
  auto_create_entities: true
""",
    )
    captured: dict = {}
    _stub_pipeline(monkeypatch, captured)
    monkeypatch.setattr(
        "sys.argv",
        ["gsp-openmetadata-sidecar", "--config", cfg_path,
         "--sql", "SELECT 1", "--no-auto-create-entities", "--dry-run"],
    )
    with pytest.raises(SystemExit):
        cli.main()
    assert captured["config"].auto_create_entities is False


def test_on_create_failure_and_cap_cli_overrides(tmp_path, monkeypatch):
    cfg_path = _write_yaml(
        tmp_path,
        """
sqlflow:
  mode: anonymous
  default_database: SalesDB
  default_schema: dbo
openmetadata:
  service_name: mssql_prod
  auto_create_entities: true
  on_create_failure: abort
  max_entities_to_create: 100
""",
    )
    captured: dict = {}
    _stub_pipeline(monkeypatch, captured)
    monkeypatch.setattr(
        "sys.argv",
        ["gsp-openmetadata-sidecar", "--config", cfg_path,
         "--sql", "SELECT 1",
         "--on-create-failure", "skip-edge",
         "--max-entities-to-create", "25",
         "--dry-run"],
    )
    with pytest.raises(SystemExit):
        cli.main()
    assert captured["config"].on_create_failure == "skip-edge"
    assert captured["config"].max_entities_to_create == 25


def test_cli_env_yaml_precedence(tmp_path, monkeypatch):
    """CLI > env > YAML — all three sources set different values; CLI wins."""
    cfg_path = _write_yaml(
        tmp_path,
        """
sqlflow:
  mode: anonymous
  default_database: SalesDB
  default_schema: dbo
openmetadata:
  service_name: mssql_prod
  auto_create_entities: false
  max_entities_to_create: 10
""",
    )
    monkeypatch.setenv("GSP_OM_AUTO_CREATE_ENTITIES", "true")
    monkeypatch.setenv("GSP_OM_MAX_ENTITIES_TO_CREATE", "20")
    captured: dict = {}
    _stub_pipeline(monkeypatch, captured)
    monkeypatch.setattr(
        "sys.argv",
        ["gsp-openmetadata-sidecar", "--config", cfg_path,
         "--sql", "SELECT 1",
         "--max-entities-to-create", "30",
         "--dry-run"],
    )
    with pytest.raises(SystemExit):
        cli.main()
    # YAML: auto-create off. Env: on. CLI: doesn't touch the flag → env wins.
    assert captured["config"].auto_create_entities is True
    # YAML: 10. Env: 20. CLI: 30. CLI wins.
    assert captured["config"].max_entities_to_create == 30


def test_mutex_auto_create_flags_argparse_error(tmp_path, monkeypatch, capsys):
    cfg_path = _write_yaml(tmp_path, "sqlflow:\n  mode: anonymous\n")
    monkeypatch.setattr(
        "sys.argv",
        ["gsp-openmetadata-sidecar", "--config", cfg_path,
         "--sql", "SELECT 1",
         "--auto-create-entities", "--no-auto-create-entities"],
    )
    with pytest.raises(SystemExit) as exc:
        cli.main()
    # argparse mutex violation exits 2.
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "not allowed with" in err or "usage:" in err


def test_auto_create_without_defaults_fails_at_cli(tmp_path, monkeypatch, caplog):
    """CLI-enabled feature with no default_database/schema → exits 1 with error."""
    cfg_path = _write_yaml(
        tmp_path,
        """
sqlflow:
  mode: anonymous
openmetadata:
  service_name: mssql_prod
  database_name: null
  schema_name: ""
""",
    )
    captured: dict = {}
    _stub_pipeline(monkeypatch, captured)
    monkeypatch.setattr(
        "sys.argv",
        ["gsp-openmetadata-sidecar", "--config", cfg_path,
         "--sql", "SELECT 1", "--auto-create-entities", "--dry-run"],
    )
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 1
    assert "auto-create-entities requires" in caplog.text
