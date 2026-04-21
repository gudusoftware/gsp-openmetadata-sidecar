"""Parse SQL input from files or inline text."""

import logging
import re
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class SQLStatement:
    """A SQL statement to analyze."""
    sql: str
    source: str  # where it came from (file path or "cli input")


# Keywords that indicate procedural SQL — the file should be sent as one block
_PROCEDURAL_KEYWORDS = re.compile(
    r'\b(DECLARE|BEGIN|IF\s+.+\s+THEN|END\s+IF|CALL|EXCEPTION\s+WHEN|LOOP|END\s+LOOP|WHILE)\b',
    re.IGNORECASE,
)


def parse_sql_file(sql_path: str) -> list[SQLStatement]:
    """Read a SQL file and return statements for analysis.

    If the file contains procedural keywords (DECLARE, IF/THEN, CALL, BEGIN/END),
    the entire file is sent as a single statement — splitting on semicolons would
    break the procedural block. Otherwise, splits on semicolons.
    """
    path = Path(sql_path)
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    content = path.read_text(encoding="utf-8")

    # Procedural SQL: send as one block (semicolons are inside the block)
    if _PROCEDURAL_KEYWORDS.search(content):
        logger.info("Detected procedural SQL in %s — sending as single statement", sql_path)
        return [SQLStatement(sql=content.strip(), source=sql_path)]

    # Non-procedural: split on semicolons
    raw_stmts = [s.strip() for s in content.split(";") if s.strip()]

    if len(raw_stmts) <= 1:
        return [SQLStatement(sql=content.strip(), source=sql_path)]

    return [SQLStatement(sql=s, source=sql_path) for s in raw_stmts]


def parse_sql_text(sql_text: str) -> list[SQLStatement]:
    """Wrap a direct SQL string as a SQLStatement."""
    return [SQLStatement(sql=sql_text.strip(), source="cli input")]
