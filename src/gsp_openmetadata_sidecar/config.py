"""Configuration loading with YAML file + environment variable overrides."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


# Default API URLs per mode.
DEFAULT_URLS = {
    "anonymous": "https://api.gudusoft.com/gspLive_backend/api/anonymous/lineage",
    "authenticated": "https://api.gudusoft.com/gspLive_backend/sqlflow/generation/sqlflow/exportFullLineageAsJson",
    "self_hosted": "http://localhost:8165/api/gspLive_backend/sqlflow/generation/sqlflow/exportFullLineageAsJson",
}


@dataclass
class SQLFlowConfig:
    mode: str = "anonymous"
    url: Optional[str] = None
    user_id: Optional[str] = None
    secret_key: Optional[str] = None
    db_vendor: str = "dbvmssql"
    show_relation_type: str = "fdd"
    # local_jar mode only:
    jar_path: Optional[str] = None
    java_bin: str = "java"

    @property
    def effective_url(self) -> str:
        if self.url:
            return self.url
        return DEFAULT_URLS[self.mode]


@dataclass
class OpenMetadataConfig:
    server: str = "http://localhost:8585/api"
    token: Optional[str] = None
    service_name: str = "mssql"
    database_name: Optional[str] = None
    schema_name: str = "dbo"
    column_lineage: bool = True


@dataclass
class InputConfig:
    sql_file: Optional[str] = None
    sql_text: Optional[str] = None


@dataclass
class SidecarConfig:
    sqlflow: SQLFlowConfig = field(default_factory=SQLFlowConfig)
    openmetadata: OpenMetadataConfig = field(default_factory=OpenMetadataConfig)
    input: InputConfig = field(default_factory=InputConfig)


def load_config(config_path: Optional[str] = None) -> SidecarConfig:
    """Load configuration from YAML file, then override with environment variables.

    Priority (highest wins):
      1. Environment variables (GSP_BACKEND_MODE, GSP_SQLFLOW_URL, etc.)
      2. YAML config file
      3. Built-in defaults
    """
    cfg = SidecarConfig()

    # --- Load YAML if provided ---
    if config_path and Path(config_path).exists():
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}

        sf = raw.get("sqlflow", {})
        cfg.sqlflow.mode = sf.get("mode", cfg.sqlflow.mode)
        cfg.sqlflow.url = sf.get("url", cfg.sqlflow.url)
        cfg.sqlflow.user_id = sf.get("user_id", cfg.sqlflow.user_id)
        cfg.sqlflow.secret_key = sf.get("secret_key", cfg.sqlflow.secret_key)
        cfg.sqlflow.db_vendor = sf.get("db_vendor", cfg.sqlflow.db_vendor)
        cfg.sqlflow.show_relation_type = sf.get("show_relation_type", cfg.sqlflow.show_relation_type)
        cfg.sqlflow.jar_path = sf.get("jar_path", cfg.sqlflow.jar_path)
        cfg.sqlflow.java_bin = sf.get("java_bin", cfg.sqlflow.java_bin)

        om = raw.get("openmetadata", {})
        cfg.openmetadata.server = om.get("server", cfg.openmetadata.server)
        cfg.openmetadata.token = om.get("token", cfg.openmetadata.token)
        cfg.openmetadata.service_name = om.get("service_name", cfg.openmetadata.service_name)
        cfg.openmetadata.database_name = om.get("database_name", cfg.openmetadata.database_name)
        cfg.openmetadata.schema_name = om.get("schema_name", cfg.openmetadata.schema_name)
        if "column_lineage" in om:
            cfg.openmetadata.column_lineage = bool(om["column_lineage"])

        inp = raw.get("input", {})
        cfg.input.sql_file = inp.get("sql_file", cfg.input.sql_file)
        cfg.input.sql_text = inp.get("sql_text", cfg.input.sql_text)

    # --- Environment variable overrides ---
    env_map = {
        "GSP_BACKEND_MODE": ("sqlflow", "mode"),
        "GSP_SQLFLOW_URL": ("sqlflow", "url"),
        "GSP_SQLFLOW_USER_ID": ("sqlflow", "user_id"),
        "GSP_SQLFLOW_SECRET_KEY": ("sqlflow", "secret_key"),
        "GSP_DB_VENDOR": ("sqlflow", "db_vendor"),
        "GSP_JAR_PATH": ("sqlflow", "jar_path"),
        "GSP_JAVA_BIN": ("sqlflow", "java_bin"),
        "GSP_OM_SERVER": ("openmetadata", "server"),
        "GSP_OM_TOKEN": ("openmetadata", "token"),
        "GSP_OM_SERVICE_NAME": ("openmetadata", "service_name"),
        "GSP_OM_DATABASE_NAME": ("openmetadata", "database_name"),
        "GSP_OM_SCHEMA_NAME": ("openmetadata", "schema_name"),
        "GSP_COLUMN_LINEAGE": ("openmetadata", "column_lineage"),
        "GSP_SQL_FILE": ("input", "sql_file"),
        "GSP_SQL_TEXT": ("input", "sql_text"),
    }
    for env_var, (section, attr) in env_map.items():
        val = os.environ.get(env_var)
        if val is not None:
            current = getattr(getattr(cfg, section), attr, None)
            if isinstance(current, bool):
                val = val.strip().lower() in ("1", "true", "yes", "on")
            setattr(getattr(cfg, section), attr, val)

    # --- Validate ---
    valid_modes = {"anonymous", "authenticated", "self_hosted", "local_jar"}
    if cfg.sqlflow.mode not in valid_modes:
        raise ValueError(
            f"Invalid sqlflow.mode '{cfg.sqlflow.mode}'. Must be one of: {valid_modes}"
        )

    if cfg.sqlflow.mode == "authenticated" and (
        not cfg.sqlflow.user_id or not cfg.sqlflow.secret_key
    ):
        raise ValueError(
            "sqlflow.user_id and sqlflow.secret_key are both required when mode is "
            "'authenticated'."
        )

    if cfg.sqlflow.mode == "local_jar" and not cfg.sqlflow.jar_path:
        raise ValueError(
            "sqlflow.jar_path is required when mode is 'local_jar'."
        )

    return cfg
