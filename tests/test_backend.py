"""Tests for SQLFlow payload construction and local_jar warning behavior."""

import logging

import pytest

from gsp_openmetadata_sidecar.backend import AnonymousBackend, create_backend
from gsp_openmetadata_sidecar.config import SQLFlowConfig


def test_build_payload_includes_defaults_when_set():
    backend = AnonymousBackend(url="https://example/anonymous")
    payload = backend._build_payload(
        sql="SELECT 1",
        db_vendor="dbvmssql",
        default_server="srv01",
        default_database="SalesDB",
        default_schema="dbo",
    )
    assert payload["defaultServer"] == "srv01"
    assert payload["defaultDatabase"] == "SalesDB"
    assert payload["defaultSchema"] == "dbo"


def test_build_payload_omits_defaults_when_unset():
    backend = AnonymousBackend(url="https://example/anonymous")
    payload = backend._build_payload(sql="SELECT 1", db_vendor="dbvmssql")
    assert "defaultServer" not in payload
    assert "defaultDatabase" not in payload
    assert "defaultSchema" not in payload
    # Baseline fields still present.
    assert payload["sqltext"] == "SELECT 1"
    assert payload["dbvendor"] == "dbvmssql"


def test_build_payload_skips_empty_string_defaults():
    """Truthy-only forward: empty strings don't appear in the payload either."""
    backend = AnonymousBackend(url="https://example/anonymous")
    payload = backend._build_payload(
        sql="SELECT 1",
        db_vendor="dbvmssql",
        default_server="",
        default_database=None,
        default_schema="dbo",
    )
    assert "defaultServer" not in payload
    assert "defaultDatabase" not in payload
    assert payload["defaultSchema"] == "dbo"


def test_local_jar_warning_fires_when_defaults_set(caplog):
    cfg = SQLFlowConfig(
        mode="local_jar",
        jar_path="/tmp/does-not-matter.jar",
        default_database="SalesDB",
    )
    with caplog.at_level(logging.WARNING, logger="gsp_openmetadata_sidecar.backend"):
        create_backend(cfg)
    messages = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("default_server/default_database/default_schema" in m for m in messages), (
        f"expected local_jar warning, got {messages!r}"
    )


def test_local_jar_warning_silent_when_no_defaults(caplog):
    cfg = SQLFlowConfig(mode="local_jar", jar_path="/tmp/does-not-matter.jar")
    with caplog.at_level(logging.WARNING, logger="gsp_openmetadata_sidecar.backend"):
        create_backend(cfg)
    warnings = [
        r.getMessage() for r in caplog.records
        if r.levelno == logging.WARNING
        and "default_server/default_database/default_schema" in r.getMessage()
    ]
    assert warnings == []
