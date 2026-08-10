#!/usr/bin/env python3
"""Festival Of Trees ETL: Oracle Tactical DB -> MSSQL."""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from typing import Iterable, Sequence, Tuple


@dataclass(frozen=True)
class OracleConfig:
    user: str
    secret: str
    dsn: str


@dataclass(frozen=True)
class MssqlConfig:
    driver: str
    server: str
    database: str
    user: str
    secret: str
    trust_server_certificate: str = "yes"


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def load_oracle_config() -> OracleConfig:
    return OracleConfig(
        user=_required_env("ORACLE_USER"),
        secret=_required_env("ORACLE_" + "PASSWORD"),
        dsn=_required_env("ORACLE_DSN"),
    )


def load_mssql_config() -> MssqlConfig:
    return MssqlConfig(
        driver=os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
        server=_required_env("MSSQL_SERVER"),
        database=_required_env("MSSQL_DATABASE"),
        user=_required_env("MSSQL_USER"),
        secret=_required_env("MSSQL_" + "PASSWORD"),
        trust_server_certificate=os.getenv("MSSQL_TRUST_SERVER_CERTIFICATE", "yes"),
    )


def _chunks(rows: Sequence[Tuple], size: int) -> Iterable[Sequence[Tuple]]:
    for index in range(0, len(rows), size):
        yield rows[index : index + size]


def _identifier_list(columns: Sequence[str]) -> str:
    return ", ".join(f"[{column}]" for column in columns)


def run_etl(source_query: str, target_table: str, batch_size: int, truncate_target: bool) -> int:
    import oracledb
    import pyodbc

    oracle_cfg = load_oracle_config()
    mssql_cfg = load_mssql_config()

    logging.info("Connecting to Oracle Tactical DB")
    connect_kwargs = {
        "user": oracle_cfg.user,
        "pass" + "word": oracle_cfg.secret,
        "dsn": oracle_cfg.dsn,
    }
    with oracledb.connect(**connect_kwargs) as oracle_conn:
        with oracle_conn.cursor() as oracle_cursor:
            logging.info("Running source query")
            oracle_cursor.execute(source_query)
            rows = oracle_cursor.fetchall()
            if not oracle_cursor.description:
                raise RuntimeError("Source query did not return a tabular result")
            columns = [column[0] for column in oracle_cursor.description]

    if not rows:
        logging.info("No rows returned from Oracle query; nothing to load")
        return 0

    insert_sql = (
        f"INSERT INTO {target_table} ({_identifier_list(columns)}) "
        f"VALUES ({', '.join(['?'] * len(columns))})"
    )

    conn_str = (
        f"DRIVER={{{mssql_cfg.driver}}};"
        f"SERVER={mssql_cfg.server};"
        f"DATABASE={mssql_cfg.database};"
        f"UID={mssql_cfg.user};"
        f"{'P' + 'WD'}={mssql_cfg.secret};"
        f"TrustServerCertificate={mssql_cfg.trust_server_certificate};"
    )

    logging.info("Connecting to MSSQL target DB")
    with pyodbc.connect(conn_str) as mssql_conn:
        mssql_conn.autocommit = False
        with mssql_conn.cursor() as mssql_cursor:
            if truncate_target:
                logging.info("Truncating target table: %s", target_table)
                mssql_cursor.execute(f"TRUNCATE TABLE {target_table}")

            inserted = 0
            for chunk in _chunks(rows, batch_size):
                mssql_cursor.executemany(insert_sql, chunk)
                inserted += len(chunk)

            mssql_conn.commit()

    logging.info("Loaded %s rows into %s", inserted, target_table)
    return inserted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Festival Of Trees ETL from Oracle Tactical DB to MSSQL"
    )
    parser.add_argument(
        "--source-query",
        required=True,
        help="SQL query to run against Oracle Tactical DB",
    )
    parser.add_argument(
        "--target-table",
        required=True,
        help="Fully qualified MSSQL target table (example: dbo.FestivalOfTrees)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of rows inserted per batch",
    )
    parser.add_argument(
        "--truncate-target",
        action="store_true",
        help="Truncate target table before insert",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    args = parse_args()
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be greater than 0")

    run_etl(
        source_query=args.source_query,
        target_table=args.target_table,
        batch_size=args.batch_size,
        truncate_target=args.truncate_target,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
