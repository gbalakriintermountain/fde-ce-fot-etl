"""Entry point: migrate data from Oracle tables into SQL Server tables.

Usage:
    python main.py [--config config.yaml] [--env .env] [--table SOURCE_SCHEMA.TABLE ...] [--dry-run]

Configure connections and the table list in config.yaml (see config.yaml
and .env.example for details) before running.
"""

from __future__ import annotations

import argparse
import logging
import sys

from src.config_loader import ConfigError, load_config
from src.db_connections import get_oracle_connection, get_sqlserver_connection
from src.migrate import migrate_table, sync_new_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate data from Oracle to SQL Server.")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--env", default=".env", help="Path to .env file")
    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        default=None,
        help="Limit the run to specific source table(s) (repeatable). "
        "Matches the 'source' value in config.yaml.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate connections/tables and count source rows without inserting any data.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Only append rows present in the Oracle source but not yet in the SQL Server "
        "target (compared by each table's 'pk' column in config.yaml), instead of copying "
        "the whole source table. Safe to re-run repeatedly to pick up newly added rows.",
    )
    return parser.parse_args()


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("migration.log", encoding="utf-8"),
        ],
    )


def main() -> int:
    args = parse_args()
    setup_logging(args.log_level)
    logger = logging.getLogger("main")

    try:
        config = load_config(args.config, args.env)
    except ConfigError as exc:
        logger.error("Configuration error: %s", exc)
        return 1

    table_configs = config["tables"]
    if args.tables:
        wanted = {t.upper() for t in args.tables}
        table_configs = [t for t in table_configs if t["source"].upper() in wanted]
        if not table_configs:
            logger.error("No matching tables found for --table filter: %s", args.tables)
            return 1

    try:
        oracle_conn = get_oracle_connection(config["oracle"])
    except Exception:
        logger.exception("Failed to connect to Oracle")
        return 1

    try:
        mssql_conn = get_sqlserver_connection(config["sqlserver"])
    except Exception:
        logger.exception("Failed to connect to SQL Server")
        oracle_conn.close()
        return 1

    driver_name = config["sqlserver"].get("driver", "")
    fast_executemany = "ODBC Driver" in driver_name
    if not fast_executemany:
        logger.warning(
            "SQL Server driver '%s' does not reliably support fast_executemany; "
            "inserts will be slower. Install 'ODBC Driver 17 for SQL Server' (or 18) "
            "for better performance.",
            driver_name,
        )

    results = []
    try:
        for table_cfg in table_configs:
            if args.incremental:
                action = "Syncing new rows for"
            elif args.dry_run:
                action = "Validating (dry run)"
            else:
                action = "Migrating"
            logger.info("=== %s %s -> %s ===", action, table_cfg["source"], table_cfg["target"])
            if args.incremental:
                results.append(sync_new_rows(
                    oracle_conn, mssql_conn, table_cfg,
                    fast_executemany=fast_executemany,
                ))
            else:
                results.append(migrate_table(
                    oracle_conn, mssql_conn, table_cfg,
                    dry_run=args.dry_run, fast_executemany=fast_executemany,
                ))
    finally:
        oracle_conn.close()
        mssql_conn.close()

    if args.incremental:
        summary_label = "Incremental sync"
    elif args.dry_run:
        summary_label = "Dry run"
    else:
        summary_label = "Migration"
    logger.info("=== %s summary ===", summary_label)
    failures = 0
    rows_label = "rows_found" if args.dry_run else "rows_inserted"
    for r in results:
        status = "OK" if r.success else "FAILED"
        if not r.success:
            failures += 1
        rows_value = r.rows_read if args.dry_run else r.rows_inserted
        logger.info(
            "%-8s %-40s %s=%-8d time=%.1fs%s",
            status, f"{r.source} -> {r.target}", rows_label, rows_value, r.seconds,
            f" error={r.error}" if r.error else "",
        )

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
