"""SQLFlow backend — calls the lineage API in anonymous, authenticated, self-hosted, or local-JAR mode."""

import json
import logging
import os
import shutil
import subprocess
import tempfile
from abc import ABC, abstractmethod
from typing import Any

import requests

from .config import SQLFlowConfig

logger = logging.getLogger(__name__)


class SQLFlowBackend(ABC):
    """Base class for SQLFlow API backends."""

    @abstractmethod
    def get_lineage(self, sql: str, db_vendor: str, **kwargs) -> dict[str, Any]:
        """Send SQL to SQLFlow and return the parsed lineage JSON.

        Returns the full response dict (with 'code' and 'data' keys).
        Raises SQLFlowError on failure.
        """

    def _build_payload(self, sql: str, db_vendor: str, **kwargs) -> dict:
        return {
            "sqltext": sql,
            "dbvendor": db_vendor,
            "showRelationType": kwargs.get("show_relation_type", "fdd"),
        }


class SQLFlowError(Exception):
    """Raised when the SQLFlow API returns an error."""

    def __init__(self, message: str, status_code: int = 0, response_body: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body or {}


class RateLimitError(SQLFlowError):
    """Raised when the anonymous tier rate limit is exceeded (HTTP 429)."""

    def __init__(self, response_body: dict):
        upgrade = response_body.get("upgrade", {})
        personal_url = upgrade.get("personal_key", {}).get("url", "https://docs.gudusoft.com/sign-up/")
        docker_url = upgrade.get("self_hosted", {}).get("url", "https://docs.gudusoft.com/docker/")
        message = (
            f"Anonymous API rate limit exceeded. "
            f"To continue:\n"
            f"  1. Get a free personal key (10k/month): {personal_url}\n"
            f"  2. Deploy self-hosted (unlimited):    {docker_url}\n"
            f"  See sidecar.yaml.example for configuration."
        )
        super().__init__(message, status_code=429, response_body=response_body)


class AnonymousBackend(SQLFlowBackend):
    """Tier 1: No auth. Rate-limited per IP (50/day)."""

    def __init__(self, url: str):
        self.url = url

    def get_lineage(self, sql: str, db_vendor: str, **kwargs) -> dict[str, Any]:
        payload = self._build_payload(sql, db_vendor, **kwargs)
        resp = requests.post(self.url, json=payload, timeout=120)

        if resp.status_code == 429:
            raise RateLimitError(resp.json())
        resp.raise_for_status()
        return resp.json()


class TokenExchangeBackend(SQLFlowBackend):
    """Shared base for ``authenticated`` (cloud) and ``self_hosted`` (Docker) modes.

    Both tiers use SQLFlow's two-step protocol (see SQLFlow's
    https://github.com/sqlparser/sqlflow_public/blob/master/api/python/basic/GenerateToken.py):

    1. POST ``.../gspLive_backend/user/generateToken`` with ``userId`` +
       ``secretKey`` (form-encoded) -> receive a short-lived JWT ``token``.
    2. POST the lineage endpoint with ``userId`` + ``token`` (form-encoded) —
       NOT the raw ``secretKey``.

    The demo user ``gudu|0123456789`` is a special case: the literal string
    ``"token"`` is accepted without calling generateToken.

    Subclasses differ only in their default URL and log label.
    """

    # Used only in log/error messages to distinguish tiers.
    label: str = "SQLFlow"

    def __init__(self, url: str, user_id: str | None = None, secret_key: str | None = None):
        self.url = url
        self.user_id = user_id
        self.secret_key = secret_key
        self._token: str | None = None

    def _token_url(self) -> str:
        """Derive the generateToken URL from the lineage URL.

        Given ``.../gspLive_backend/sqlflow/generation/sqlflow/exportFullLineageAsJson``
        this returns ``.../gspLive_backend/user/generateToken``. Works for both
        api.gudusoft.com (no ``/api/`` prefix) and self-hosted Docker
        (``/api/gspLive_backend/...``).
        """
        marker = "/gspLive_backend/"
        idx = self.url.find(marker)
        if idx == -1:
            raise SQLFlowError(
                f"Cannot derive generateToken URL from {self.url} — "
                f"expected '/gspLive_backend/' in the path."
            )
        return self.url[: idx + len(marker)] + "user/generateToken"

    def _get_token(self) -> str:
        """Fetch (and cache) a token for the configured user."""
        if self._token:
            return self._token
        if self.user_id == "gudu|0123456789":
            # Demo user — SQLFlow accepts the literal string "token".
            self._token = "token"
            return self._token
        if not self.user_id or not self.secret_key:
            raise SQLFlowError(
                f"{self.label} requires user_id + secret_key to generate a token. "
                f"Set sqlflow.user_id / sqlflow.secret_key, or pass --user-id / --secret-key."
            )

        token_url = self._token_url()
        logger.debug("Requesting %s token from %s", self.label, token_url)
        resp = requests.post(
            token_url,
            data={"userId": self.user_id, "secretKey": self.secret_key},
            timeout=60,
        )
        if resp.status_code != 200:
            raise SQLFlowError(
                f"{self.label} token request to {token_url} returned HTTP {resp.status_code}. "
                f"Response: {resp.text[:500]}",
                status_code=resp.status_code,
            )
        body = resp.json()
        # generateToken returns code as a string ("200"), unlike other endpoints.
        if str(body.get("code")) != "200" or not body.get("token"):
            raise SQLFlowError(
                f"{self.label} token generation failed: {body.get('error') or body}",
                response_body=body,
            )
        self._token = body["token"]
        return self._token

    def get_lineage(self, sql: str, db_vendor: str, **kwargs) -> dict[str, Any]:
        payload = self._build_payload(sql, db_vendor, **kwargs)
        if self.user_id:
            payload["userId"] = self.user_id
            payload["token"] = self._get_token()

        resp = requests.post(self.url, data=payload, timeout=120)

        if resp.status_code != 200:
            raise SQLFlowError(
                f"{self.label} returned HTTP {resp.status_code} from {self.url}. "
                f"Response: {resp.text[:500]}",
                status_code=resp.status_code,
            )

        body = resp.json()
        # SQLFlow returns 200 with an in-body error code for auth / validation failures.
        code = body.get("code") if isinstance(body, dict) else None
        if code not in (None, 200, "200"):
            # If the token expired, retry once with a fresh token.
            if str(code) == "401" and self._token is not None:
                logger.info("%s token rejected — refreshing and retrying once.", self.label)
                self._token = None
                payload["token"] = self._get_token()
                resp = requests.post(self.url, data=payload, timeout=120)
                body = resp.json()
                code = body.get("code") if isinstance(body, dict) else None
            if code not in (None, 200, "200"):
                raise SQLFlowError(
                    f"{self.label} returned error code {code}: "
                    f"{body.get('error') or body}",
                    status_code=int(code) if str(code).isdigit() else 0,
                    response_body=body,
                )
        return body


class AuthenticatedBackend(TokenExchangeBackend):
    """Tier 2: Personal API key on api.gudusoft.com. 10k calls/month.

    Uses the same token-exchange protocol as the self-hosted tier — the only
    difference is the default URL points at api.gudusoft.com instead of a
    local Docker.
    """

    label = "Authenticated SQLFlow"


class SelfHostedBackend(TokenExchangeBackend):
    """Tier 3: Self-hosted SQLFlow Docker. Unlimited, data stays in VPC."""

    label = "Self-hosted SQLFlow"


# SQLFlow's internal enum names (``dbvbigquery``) vs. the CLI's short aliases
# (``bigquery``). ``DataFlowAnalyzer /t <name>`` calls ``EDbVendor.fromAlias``,
# which only matches the short form — passing ``dbvbigquery`` silently falls
# back to ``dbvoracle`` and every non-Oracle statement becomes a syntax error.
# Strip the ``dbv`` prefix when present.
def _cli_vendor_name(db_vendor: str) -> str:
    name = (db_vendor or "").strip().lower()
    if name.startswith("dbv"):
        name = name[3:]
    return name or "generic"


class LocalJarBackend(SQLFlowBackend):
    """Tier 4: Embedded gsp.jar. No HTTP, no Docker — SQL never leaves the process.

    Shells out to SQLFlow's bundled CLI entry point::

        java -cp <jar> gudusoft.gsqlparser.dlineage.DataFlowAnalyzer \\
             /f <sql-file> /t <vendor> /json

    The CLI prints the raw ``Dataflow`` JSON (``{dbobjs, relationships,
    processes, errors?}``). We wrap it as ``{"code": 200, "data": ...}`` so
    the downstream mapper and the ``code == 200`` check in ``cli.py`` remain
    identical to the HTTP backends.

    Trade-off: the JVM cold-starts on every call (~0.5-1 s). Fine for
    ad-hoc files; for log-file ingestion with hundreds of statements, prefer
    a batched approach (see README).
    """

    label = "Local JAR"

    def __init__(self, jar_path: str, java_bin: str = "java", timeout: int = 120):
        self.jar_path = jar_path
        self.java_bin = java_bin
        self.timeout = timeout

    def get_lineage(self, sql: str, db_vendor: str, **kwargs) -> dict[str, Any]:
        if not os.path.isfile(self.jar_path):
            raise SQLFlowError(
                f"{self.label}: jar not found at {self.jar_path!r}. "
                f"The sidecar does not bundle the SQLFlow JAR — set "
                f"sqlflow.jar_path (or --jar-path / GSP_JAR_PATH) to the "
                f"absolute path of a licensed gsqlparser-*-shaded.jar."
            )
        if shutil.which(self.java_bin) is None and not os.path.isfile(self.java_bin):
            raise SQLFlowError(
                f"{self.label}: java executable '{self.java_bin}' not found on PATH. "
                f"Install a JRE (8+) or set sqlflow.java_bin / --java-bin."
            )

        # DataFlowAnalyzer only accepts /f <file> or /d <dir>; no stdin mode.
        # Stage the SQL in a temp file and clean up after.
        with tempfile.NamedTemporaryFile(
            "w", suffix=".sql", prefix="gsp_", delete=False, encoding="utf-8"
        ) as f:
            f.write(sql)
            tmp_path = f.name
        try:
            cmd = [
                self.java_bin, "-cp", self.jar_path,
                "gudusoft.gsqlparser.dlineage.DataFlowAnalyzer",
                "/f", tmp_path,
                "/t", _cli_vendor_name(db_vendor),
                "/json",
            ]
            logger.debug("Invoking %s: %s", self.label, " ".join(cmd))
            try:
                proc = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=self.timeout,
                )
            except subprocess.TimeoutExpired as e:
                raise SQLFlowError(
                    f"{self.label}: JAR invocation timed out after {self.timeout}s"
                ) from e
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

        if proc.returncode != 0:
            raise SQLFlowError(
                f"{self.label}: java exited with code {proc.returncode}. "
                f"stderr: {proc.stderr.strip()[:500]}",
                status_code=proc.returncode,
            )

        try:
            dataflow = json.loads(proc.stdout)
        except json.JSONDecodeError as e:
            raise SQLFlowError(
                f"{self.label}: could not parse JAR stdout as JSON: {e}. "
                f"First 200 chars of stdout: {proc.stdout[:200]!r}"
            ) from e

        # Cloud SQLFlow reports syntax errors inside an ``errors`` array while
        # still returning any relationships it managed to parse. Mirror that:
        # log a warning and let the mapper harvest what it can.
        errs = dataflow.get("errors") or []
        if errs:
            logger.warning(
                "%s: parser reported %d error(s); first: %s",
                self.label, len(errs),
                (errs[0].get("errorMessage") if isinstance(errs[0], dict) else errs[0]),
            )

        return {"code": 200, "data": dataflow}


def create_backend(config: SQLFlowConfig) -> SQLFlowBackend:
    """Factory: create the right backend based on config mode."""
    if config.mode == "local_jar":
        logger.info("Using local-JAR backend: %s", config.jar_path)
        return LocalJarBackend(
            jar_path=config.jar_path or "",
            java_bin=config.java_bin or "java",
        )

    url = config.effective_url

    if config.mode == "anonymous":
        logger.info("Using anonymous backend: %s (50 calls/day per IP)", url)
        return AnonymousBackend(url=url)

    if config.mode == "authenticated":
        logger.info("Using authenticated backend: %s", url)
        return AuthenticatedBackend(url=url, user_id=config.user_id, secret_key=config.secret_key)

    if config.mode == "self_hosted":
        logger.info("Using self-hosted backend: %s", url)
        return SelfHostedBackend(url=url, user_id=config.user_id, secret_key=config.secret_key)

    raise ValueError(f"Unknown backend mode: {config.mode}")
