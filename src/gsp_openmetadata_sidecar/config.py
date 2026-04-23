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
    # Parse-time default qualifiers sent to SQLFlow. Independent from the
    # openmetadata.* defaults below.
    default_server: Optional[str] = None
    default_database: Optional[str] = None
    default_schema: Optional[str] = None
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
    # Opt-in auto-creation of missing Database / DatabaseSchema / Table
    # entities before lineage emission. Default off preserves byte-for-byte
    # legacy behavior.
    auto_create_entities: bool = False
    on_create_failure: str = "abort"          # {"abort", "skip-edge"}
    max_entities_to_create: int = 100
    auto_created_tag_fqn: Optional[str] = None  # e.g. "AutoCreated.gsp-sidecar"


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
        cfg.sqlflow.default_server = sf.get("default_server", cfg.sqlflow.default_server)
        cfg.sqlflow.default_database = sf.get("default_database", cfg.sqlflow.default_database)
        cfg.sqlflow.default_schema = sf.get("default_schema", cfg.sqlflow.default_schema)
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
        if "auto_create_entities" in om:
            cfg.openmetadata.auto_create_entities = bool(om["auto_create_entities"])
        if "on_create_failure" in om:
            cfg.openmetadata.on_create_failure = str(om["on_create_failure"])
        if "max_entities_to_create" in om:
            cfg.openmetadata.max_entities_to_create = int(om["max_entities_to_create"])
        if "auto_created_tag_fqn" in om:
            cfg.openmetadata.auto_created_tag_fqn = om["auto_created_tag_fqn"]

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
        "GSP_DEFAULT_SERVER": ("sqlflow", "default_server"),
        "GSP_DEFAULT_DATABASE": ("sqlflow", "default_database"),
        "GSP_DEFAULT_SCHEMA": ("sqlflow", "default_schema"),
        "GSP_JAR_PATH": ("sqlflow", "jar_path"),
        "GSP_JAVA_BIN": ("sqlflow", "java_bin"),
        "GSP_OM_SERVER": ("openmetadata", "server"),
        "GSP_OM_TOKEN": ("openmetadata", "token"),
        "GSP_OM_SERVICE_NAME": ("openmetadata", "service_name"),
        "GSP_OM_DATABASE_NAME": ("openmetadata", "database_name"),
        "GSP_OM_SCHEMA_NAME": ("openmetadata", "schema_name"),
        "GSP_COLUMN_LINEAGE": ("openmetadata", "column_lineage"),
        "GSP_OM_AUTO_CREATE_ENTITIES": ("openmetadata", "auto_create_entities"),
        "GSP_OM_ON_CREATE_FAILURE": ("openmetadata", "on_create_failure"),
        "GSP_OM_MAX_ENTITIES_TO_CREATE": ("openmetadata", "max_entities_to_create"),
        "GSP_OM_AUTO_CREATED_TAG_FQN": ("openmetadata", "auto_created_tag_fqn"),
        "GSP_SQL_FILE": ("input", "sql_file"),
        "GSP_SQL_TEXT": ("input", "sql_text"),
    }
    for env_var, (section, attr) in env_map.items():
        val = os.environ.get(env_var)
        if val is not None:
            current = getattr(getattr(cfg, section), attr, None)
            if isinstance(current, bool):
                val = val.strip().lower() in ("1", "true", "yes", "on")
            elif isinstance(current, int) and not isinstance(current, bool):
                try:
                    val = int(val)
                except ValueError:
                    raise ValueError(
                        f"Environment variable {env_var} must be an integer, got {val!r}"
                    )
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

    if cfg.openmetadata.auto_create_entities:
        if cfg.openmetadata.on_create_failure not in {"abort", "skip-edge"}:
            raise ValueError(
                f"openmetadata.on_create_failure must be 'abort' or 'skip-edge', "
                f"got {cfg.openmetadata.on_create_failure!r}."
            )
        if cfg.openmetadata.max_entities_to_create < 0:
            raise ValueError(
                "openmetadata.max_entities_to_create must be non-negative."
            )
        has_db_default = bool(
            cfg.sqlflow.default_database or cfg.openmetadata.database_name
        )
        has_schema_default = bool(
            cfg.sqlflow.default_schema or cfg.openmetadata.schema_name
        )
        if not has_db_default or not has_schema_default:
            raise ValueError(
                "openmetadata.auto_create_entities=true requires "
                "sqlflow.default_database (or openmetadata.database_name) AND "
                "sqlflow.default_schema (or openmetadata.schema_name) to "
                "prevent partial-FQN ghost entities."
            )

    return cfg
