"""CLI entry point for gsp-openmetadata-sidecar."""

import argparse
import json
import logging
import sys

from . import __version__
from .backend import RateLimitError, SQLFlowError, create_backend
from .config import load_config
from .emitter import emit_lineage
from .lineage_mapper import extract_lineage
from .sql_input import parse_sql_file, parse_sql_text


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    parser = argparse.ArgumentParser(
        prog="gsp-openmetadata-sidecar",
        description=(
            "Recover SQL lineage that OpenMetadata's parser misses.\n\n"
            "Parses SQL statements (MSSQL stored procedures, BigQuery procedural SQL, etc.) "
            "using Gudu SQLFlow, and pushes the lineage to OpenMetadata via its REST API."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  # Analyze a MSSQL stored procedure (dry run, anonymous mode):\n"
            "  gsp-openmetadata-sidecar --sql-file stored_proc.sql --dry-run\n\n"
            "  # Analyze inline SQL:\n"
            '  gsp-openmetadata-sidecar --sql "CREATE PROC p AS BEGIN INSERT INTO t2 SELECT a FROM t1 END"\n\n'
            "  # Push lineage to OpenMetadata:\n"
            "  gsp-openmetadata-sidecar --config sidecar.yaml --sql-file proc.sql\n\n"
            "  # Use self-hosted SQLFlow Docker:\n"
            "  gsp-openmetadata-sidecar --mode self_hosted --sql-file proc.sql --dry-run\n"
        ),
    )

    # --- Input sources (mutually exclusive) ---
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument(
        "--sql-file",
        help="Path to a SQL file to analyze.",
    )
    input_group.add_argument(
        "--sql",
        help="Inline SQL text to analyze.",
    )

    # --- Config ---
    parser.add_argument(
        "--config", "-c",
        help="Path to sidecar.yaml config file (default: ./sidecar.yaml).",
        default="sidecar.yaml",
    )

    # --- SQLFlow overrides ---
    parser.add_argument(
        "--mode",
        choices=["anonymous", "authenticated", "self_hosted", "local_jar"],
        help="SQLFlow backend mode (overrides config file).",
    )
    parser.add_argument(
        "--sqlflow-url",
        help="SQLFlow API URL (overrides config file).",
    )
    parser.add_argument(
        "--user-id",
        help="SQLFlow userId.",
    )
    parser.add_argument(
        "--secret-key",
        help="SQLFlow secret key for authenticated or self_hosted mode.",
    )
    parser.add_argument(
        "--db-vendor",
        help="SQL dialect (default: dbvmssql).",
    )
    parser.add_argument(
        "--jar-path",
        help="Path to a licensed gsqlparser-*-shaded.jar (for --mode local_jar).",
    )
    parser.add_argument(
        "--java-bin",
        help="java executable for local_jar mode (default: 'java' on PATH).",
    )

    # --- OpenMetadata ---
    parser.add_argument(
        "--om-server",
        help="OpenMetadata API server URL (default: http://localhost:8585/api).",
    )
    parser.add_argument(
        "--om-token",
        help="OpenMetadata JWT auth token.",
    )
    parser.add_argument(
        "--service-name",
        help="OpenMetadata database service name (e.g. 'mssql_prod').",
    )
    parser.add_argument(
        "--database-name",
        help="Default database name for FQN resolution.",
    )
    parser.add_argument(
        "--schema-name",
        help="Default schema name for FQN resolution (default: dbo).",
    )

    # --- Output control ---
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and analyze SQL but don't push to OpenMetadata.",
    )
    column_lineage_group = parser.add_mutually_exclusive_group()
    column_lineage_group.add_argument(
        "--column-lineage",
        action="store_true",
        default=None,
        help="Emit column-level lineage (default).",
    )
    column_lineage_group.add_argument(
        "--no-column-lineage",
        action="store_false",
        dest="column_lineage",
        help="Emit table-level lineage only.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output raw SQLFlow lineage JSON to stdout.",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)
    logger = logging.getLogger("gsp_openmetadata_sidecar")

    # --- Load config ---
    config = load_config(args.config)

    # --- Apply CLI overrides ---
    if args.mode:
        config.sqlflow.mode = args.mode
    if args.sqlflow_url:
        config.sqlflow.url = args.sqlflow_url
    if args.user_id:
        config.sqlflow.user_id = args.user_id
    if args.secret_key:
        config.sqlflow.secret_key = args.secret_key
    if args.db_vendor:
        config.sqlflow.db_vendor = args.db_vendor
    if args.jar_path:
        config.sqlflow.jar_path = args.jar_path
    if args.java_bin:
        config.sqlflow.java_bin = args.java_bin
    if args.om_server:
        config.openmetadata.server = args.om_server
    if args.om_token:
        config.openmetadata.token = args.om_token
    if args.service_name:
        config.openmetadata.service_name = args.service_name
    if args.database_name:
        config.openmetadata.database_name = args.database_name
    if args.schema_name:
        config.openmetadata.schema_name = args.schema_name
    if args.column_lineage is not None:
        config.openmetadata.column_lineage = args.column_lineage
    if args.sql_file:
        config.input.sql_file = args.sql_file
    if args.sql:
        config.input.sql_text = args.sql

    # --- Determine input source ---
    if config.input.sql_text:
        statements = parse_sql_text(config.input.sql_text)
    elif config.input.sql_file:
        statements = parse_sql_file(config.input.sql_file)
    else:
        logger.error(
            "No input provided. Use --sql or --sql-file.\n"
            "Run with --help for usage examples."
        )
        sys.exit(1)

    if not statements:
        logger.info("No SQL statements to process.")
        sys.exit(0)

    logger.info("Processing %d SQL statement(s) in '%s' mode...",
                len(statements), config.sqlflow.mode)

    # --- Create backend ---
    try:
        backend = create_backend(config.sqlflow)
    except ValueError as e:
        logger.error("Configuration error: %s", e)
        sys.exit(1)

    # --- Process each statement ---
    all_lineages = []
    errors = 0

    for i, stmt in enumerate(statements, 1):
        logger.info("[%d/%d] Analyzing SQL (%d chars) from %s...",
                    i, len(statements), len(stmt.sql), stmt.source)

        try:
            response = backend.get_lineage(
                sql=stmt.sql,
                db_vendor=config.sqlflow.db_vendor,
                show_relation_type=config.sqlflow.show_relation_type,
            )

            if args.output_json:
                print(json.dumps(response, indent=2))

            code = response.get("code", 0)
            if code != 200:
                logger.error("[%d/%d] SQLFlow returned code %d: %s",
                            i, len(statements), code,
                            response.get("error", "unknown error"))
                errors += 1
                continue

            lineages = extract_lineage(response, db_vendor=config.sqlflow.db_vendor)
            all_lineages.extend(lineages)

            if lineages:
                for tl in lineages:
                    logger.info("  Lineage: %s --> %s (%d columns)",
                                tl.upstream_table, tl.downstream_table,
                                len(tl.column_mappings))
            else:
                logger.info("  No table-level lineage found")

        except RateLimitError as e:
            logger.error("\n%s", e)
            sys.exit(2)

        except SQLFlowError as e:
            logger.error("[%d/%d] SQLFlow error: %s", i, len(statements), e)
            errors += 1

        except Exception as e:
            logger.error("[%d/%d] Unexpected error: %s", i, len(statements), e)
            errors += 1

    # --- Summary ---
    total_column_mappings = sum(len(tl.column_mappings) for tl in all_lineages)
    logger.info("--- Summary ---")
    logger.info("Statements processed: %d", len(statements))
    logger.info("Errors: %d", errors)
    logger.info("Table-level lineages found:      %d", len(all_lineages))
    if config.openmetadata.column_lineage:
        logger.info("Column-level mappings extracted: %d", total_column_mappings)

    if not all_lineages:
        logger.info("No lineage to emit.")
        sys.exit(0 if errors == 0 else 1)

    # --- Emit to OpenMetadata ---
    # Combine all SQL for the query field
    all_sql = "\n;\n".join(stmt.sql for stmt in statements)

    emitted = emit_lineage(
        all_lineages,
        sql_query=all_sql,
        config=config.openmetadata,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        logger.info("[DRY RUN] Would have emitted %d lineage edges. "
                    "Remove --dry-run to push to OpenMetadata at %s",
                    emitted, config.openmetadata.server)
    else:
        logger.info("Done. Emitted %d lineage edges to %s",
                    emitted, config.openmetadata.server)

    sys.exit(0 if errors == 0 else 1)


if __name__ == "__main__":
    main()
