"""Tests for default_server / default_database / default_schema config wiring."""

import os

import pytest

from gsp_openmetadata_sidecar.config import load_config


DEFAULT_ENV_VARS = (
    "GSP_DEFAULT_SERVER",
    "GSP_DEFAULT_DATABASE",
    "GSP_DEFAULT_SCHEMA",
    "GSP_OM_DATABASE_NAME",
    "GSP_OM_AUTO_CREATE_ENTITIES",
    "GSP_OM_ON_CREATE_FAILURE",
    "GSP_OM_MAX_ENTITIES_TO_CREATE",
    "GSP_OM_AUTO_CREATED_TAG_FQN",
)


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch):
    """Strip the new env vars for every test so the YAML path is measured cleanly."""
    for name in DEFAULT_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    yield


def _write_yaml(tmp_path, body: str) -> str:
    p = tmp_path / "sidecar.yaml"
    p.write_text(body)
    return str(p)


def test_yaml_loads_three_new_keys(tmp_path):
    cfg_path = _write_yaml(
        tmp_path,
        """
sqlflow:
  mode: anonymous
  default_server: srv01
  default_database: SalesDB
  default_schema: dbo
""",
    )
    cfg = load_config(cfg_path)
    assert cfg.sqlflow.default_server == "srv01"
    assert cfg.sqlflow.default_database == "SalesDB"
    assert cfg.sqlflow.default_schema == "dbo"


def test_env_vars_override_yaml(tmp_path, monkeypatch):
    cfg_path = _write_yaml(
        tmp_path,
        """
sqlflow:
  mode: anonymous
  default_server: from-yaml-server
  default_database: from-yaml-db
  default_schema: from-yaml-schema
""",
    )
    monkeypatch.setenv("GSP_DEFAULT_SERVER", "env-server")
    monkeypatch.setenv("GSP_DEFAULT_DATABASE", "env-db")
    monkeypatch.setenv("GSP_DEFAULT_SCHEMA", "env-schema")

    cfg = load_config(cfg_path)
    assert cfg.sqlflow.default_server == "env-server"
    assert cfg.sqlflow.default_database == "env-db"
    assert cfg.sqlflow.default_schema == "env-schema"


def test_no_implicit_fallback_from_openmetadata_database_name(tmp_path):
    """Setting openmetadata.database_name must NOT populate sqlflow.default_*."""
    cfg_path = _write_yaml(
        tmp_path,
        """
sqlflow:
  mode: anonymous
openmetadata:
  database_name: SalesDB
  schema_name: dbo
  service_name: mssql_prod
""",
    )
    cfg = load_config(cfg_path)
    assert cfg.openmetadata.database_name == "SalesDB"
    # SQLFlow-side defaults must remain None — independence rule.
    assert cfg.sqlflow.default_server is None
    assert cfg.sqlflow.default_database is None
    assert cfg.sqlflow.default_schema is None


# ----- Auto-create entity config wiring -----


def test_auto_create_requires_default_database_and_schema(tmp_path):
    """U25: feature on without defaults → ValueError at load time."""
    cfg_path = _write_yaml(
        tmp_path,
        """
sqlflow:
  mode: anonymous
openmetadata:
  auto_create_entities: true
  service_name: mssql_prod
  database_name: null
  schema_name: ""
""",
    )
    with pytest.raises(ValueError, match="requires sqlflow.default_database"):
        load_config(cfg_path)


def test_auto_create_default_database_via_sqlflow_satisfies_requirement(tmp_path):
    """Either sqlflow.default_database OR openmetadata.database_name is fine."""
    cfg_path = _write_yaml(
        tmp_path,
        """
sqlflow:
  mode: anonymous
  default_database: SalesDB
  default_schema: dbo
openmetadata:
  auto_create_entities: true
  service_name: mssql_prod
  database_name: null
""",
    )
    cfg = load_config(cfg_path)
    assert cfg.openmetadata.auto_create_entities is True


def test_max_entities_to_create_negative_rejected(tmp_path):
    """U26: negative cap → ValueError."""
    cfg_path = _write_yaml(
        tmp_path,
        """
sqlflow:
  mode: anonymous
  default_database: SalesDB
  default_schema: dbo
openmetadata:
  auto_create_entities: true
  service_name: mssql_prod
  max_entities_to_create: -1
""",
    )
    with pytest.raises(ValueError, match="non-negative"):
        load_config(cfg_path)


def test_on_create_failure_invalid_value_rejected(tmp_path):
    """U27: on_create_failure outside {abort, skip-edge} → ValueError."""
    cfg_path = _write_yaml(
        tmp_path,
        """
sqlflow:
  mode: anonymous
  default_database: SalesDB
  default_schema: dbo
openmetadata:
  auto_create_entities: true
  service_name: mssql_prod
  on_create_failure: retry-forever
""",
    )
    with pytest.raises(ValueError, match="on_create_failure"):
        load_config(cfg_path)


def test_auto_create_defaults_stay_off_and_do_not_require_defaults(tmp_path):
    """Feature off: no validation added — regression parity with legacy behavior."""
    cfg_path = _write_yaml(
        tmp_path,
        """
sqlflow:
  mode: anonymous
openmetadata:
  service_name: mssql_prod
""",
    )
    cfg = load_config(cfg_path)
    assert cfg.openmetadata.auto_create_entities is False
    assert cfg.openmetadata.on_create_failure == "abort"
    assert cfg.openmetadata.max_entities_to_create == 100


def test_auto_create_env_vars_override_yaml(tmp_path, monkeypatch):
    """Env-var precedence: GSP_OM_* > YAML for the four new keys."""
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
  on_create_failure: abort
  max_entities_to_create: 100
""",
    )
    monkeypatch.setenv("GSP_OM_AUTO_CREATE_ENTITIES", "true")
    monkeypatch.setenv("GSP_OM_ON_CREATE_FAILURE", "skip-edge")
    monkeypatch.setenv("GSP_OM_MAX_ENTITIES_TO_CREATE", "42")
    monkeypatch.setenv("GSP_OM_AUTO_CREATED_TAG_FQN", "AutoCreated.gsp-sidecar")
    cfg = load_config(cfg_path)
    assert cfg.openmetadata.auto_create_entities is True
    assert cfg.openmetadata.on_create_failure == "skip-edge"
    assert cfg.openmetadata.max_entities_to_create == 42
    assert cfg.openmetadata.auto_created_tag_fqn == "AutoCreated.gsp-sidecar"
