"""Helpers to open connections to the Oracle source database and the
SQL Server target database.
"""

from __future__ import annotations

import logging

import oracledb
import pyodbc

logger = logging.getLogger(__name__)


def get_oracle_connection(oracle_cfg: dict) -> oracledb.Connection:
    """Open an Oracle connection using the python-oracledb thin driver
    (no Oracle Instant Client installation required).
    """
    logger.info("Connecting to Oracle at %s", oracle_cfg["dsn"])
    return oracledb.connect(
        user=oracle_cfg["user"],
        password=oracle_cfg["password"],
        dsn=oracle_cfg["dsn"],
    )


def get_sqlserver_connection(sqlserver_cfg: dict) -> pyodbc.Connection:
    """Open a SQL Server connection via pyodbc using SQL authentication."""
    driver = sqlserver_cfg["driver"]
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={sqlserver_cfg['server']},{sqlserver_cfg.get('port', 1433)};"
        f"DATABASE={sqlserver_cfg['database']};"
        f"UID={sqlserver_cfg['user']};"
        f"PWD={sqlserver_cfg['password']};"
    )
    # Encrypt/TrustServerCertificate are only understood by the modern
    # "ODBC Driver 1x for SQL Server" drivers. The legacy "SQL Server"
    # driver rejects them (or fails the TLS handshake), so only add them
    # when a modern driver is configured.
    if "ODBC Driver" in driver:
        conn_str += "Encrypt=yes;TrustServerCertificate=yes;"
    logger.info(
        "Connecting to SQL Server %s,%s / database %s",
        sqlserver_cfg["server"],
        sqlserver_cfg.get("port", 1433),
        sqlserver_cfg["database"],
    )
    conn = pyodbc.connect(conn_str)
    conn.autocommit = False
    return conn

