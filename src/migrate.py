"""Core migration logic: fetch rows from an Oracle table in batches and
insert them into the corresponding SQL Server table.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import oracledb
import pyodbc

logger = logging.getLogger(__name__)


@dataclass
class TableResult:
    source: str
    target: str
    rows_read: int = 0
    rows_inserted: int = 0
    rows_skipped: int = 0
    success: bool = False
    error: str | None = None
    seconds: float = 0.0


def _output_type_handler(cursor, metadata):
    """Force Oracle CLOB/BLOB columns to be fetched as plain str/bytes
    instead of LOB locator objects, so pyodbc can bind them directly.
    """
    if metadata.type_code is oracledb.DB_TYPE_CLOB:
        return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if metadata.type_code is oracledb.DB_TYPE_BLOB:
        return cursor.var(oracledb.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)


def _target_has_identity_column(mssql_conn: pyodbc.Connection, target: str) -> bool:
    """Check whether the target SQL Server table has an IDENTITY column,
    which requires SET IDENTITY_INSERT ON to accept explicit values.
    """
    cur = mssql_conn.cursor()
    cur.execute("SELECT 1 FROM sys.identity_columns WHERE object_id = OBJECT_ID(?)", target)
    return cur.fetchone() is not None


def _build_column_map(mssql_conn: pyodbc.Connection, target: str, oracle_columns: list) -> dict:
    """Map each Oracle source column name to the actual SQL Server target
    column name. Some target columns were renamed (case, underscores
    removed, or an "ID" suffix added) after the original migration ran, so
    the Oracle column name can no longer be assumed to be a valid SQL Server
    column name as-is.

    Matching strategy: normalize both sides (lowercase, strip underscores)
    and match by that. Any columns that still don't match after
    normalization (e.g. PHONES.phone_type -> PHONETYPEID) are paired up by
    their relative left-over order, since column order is otherwise
    preserved. Raises ValueError if the two sides can't be fully reconciled.
    """
    cur = mssql_conn.cursor()
    cur.execute(
        "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?) ORDER BY column_id",
        target,
    )
    target_columns = [row[0] for row in cur.fetchall()]

    def normalize(name: str) -> str:
        return name.lower().replace("_", "")

    norm_to_target = {normalize(c): c for c in target_columns}
    used_targets = set()
    mapping: dict[str, str] = {}
    unresolved_oracle: list[str] = []

    for oc in oracle_columns:
        tc = norm_to_target.get(normalize(oc))
        if tc is not None and tc not in used_targets:
            mapping[oc] = tc
            used_targets.add(tc)
        else:
            unresolved_oracle.append(oc)

    leftover_targets = [c for c in target_columns if c not in used_targets]
    if len(unresolved_oracle) != len(leftover_targets):
        raise ValueError(
            f"[{target}] Could not reconcile Oracle columns {unresolved_oracle} "
            f"with leftover target columns {leftover_targets}"
        )
    for oc, tc in zip(unresolved_oracle, leftover_targets):
        mapping[oc] = tc

    return mapping


# SQL Server allows at most 2100 parameters per statement (and this appears
# to be enforced across an entire semicolon-separated batch sent in a single
# execute() call, not per individual statement); stay comfortably under that
# limit. T-SQL row-constructor INSERT also allows at most 1000 rows per
# VALUES clause.
_MAX_PARAMS_PER_STATEMENT = 2000
_MAX_ROWS_PER_STATEMENT = 1000


def _insert_multi_row(mssql_cur: pyodbc.Cursor, target: str, columns: list, rows: list) -> int:
    """Insert rows using multi-row 'INSERT ... VALUES (...), (...), ...'
    statements instead of one round trip per row. This gives a large speedup
    over plain executemany() on ODBC drivers that don't support
    fast_executemany (e.g. the legacy "SQL Server" driver).

    NULL values are inlined as literal ``NULL`` in the SQL text rather than
    bound as ``?`` parameters. For wide, sparsely-populated tables (many NULL
    columns), the legacy ODBC driver appears to issue an extra round trip
    (SQLDescribeParam) to resolve the SQL type of every parameter bound to
    Python None, since None carries no type information on its own. Inlining
    NULL literals avoids that entirely and is dramatically faster (~20x
    observed on a table with >50% NULL values). Multiple multi-row INSERT
    statements are also batched together (separated by ';') into a single
    execute() call, up to the same overall parameter budget, to further cut
    the number of round trips for wide tables where few rows fit per
    statement.

    If a batched execute() fails (e.g. one bad value in a huge batch, such as
    a datetime the ODBC driver rejects even though the column type accepts
    it), this falls back to retrying at increasingly fine granularity
    (statement, then individual row) so a single bad row doesn't discard an
    entire otherwise-good batch. Rows that still fail in isolation are logged
    (with their values) and skipped. Returns the number of rows actually
    inserted (<= len(rows)).
    """
    column_list = ", ".join(columns)

    statements: list[str] = []
    batch_params: list = []
    batch_param_count = 0
    batch_rows: list[list] = []  # rows belonging to each statement in `statements`

    row_clauses: list[str] = []
    stmt_params: list = []
    stmt_rows: list = []

    inserted_count = 0

    def insert_single_row(row: tuple) -> bool:
        """Insert exactly one row; return True on success, False if this
        specific row was rejected by the server (logged and skipped)."""
        cells = []
        params = []
        for value in row:
            if value is None:
                cells.append("NULL")
            else:
                cells.append("?")
                params.append(value)
        sql = f"INSERT INTO {target} ({column_list}) VALUES (" + ", ".join(cells) + ")"
        try:
            mssql_cur.execute(sql, params)
            return True
        except pyodbc.Error as exc:
            logger.warning("[%s] Skipping row that the server rejected: %s -- row=%r", target, exc, row)
            return False

    def run_batch(stmt_texts: list[str], stmt_params_list: list, stmt_rows_list: list) -> int:
        """Execute a list of statements as one batch; on failure, retry each
        statement individually, and on a statement failure, retry its rows
        individually. Returns the number of rows successfully inserted.
        """
        nonlocal inserted_count
        combined_sql = ";\n".join(stmt_texts)
        combined_params = [p for params in stmt_params_list for p in params]
        try:
            mssql_cur.execute(combined_sql, combined_params)
            count = sum(len(rs) for rs in stmt_rows_list)
            inserted_count += count
            return count
        except pyodbc.Error:
            logger.warning(
                "[%s] Batched insert of %d statement(s) failed; retrying individually to isolate bad rows",
                target, len(stmt_texts),
            )
            total_ok = 0
            for sql, params, rows_in_stmt in zip(stmt_texts, stmt_params_list, stmt_rows_list):
                try:
                    mssql_cur.execute(sql, params)
                    total_ok += len(rows_in_stmt)
                    inserted_count += len(rows_in_stmt)
                except pyodbc.Error:
                    for row in rows_in_stmt:
                        if insert_single_row(row):
                            total_ok += 1
                            inserted_count += 1
            return total_ok

    def flush_batch() -> None:
        nonlocal batch_param_count
        if not statements:
            return
        run_batch(statements, batch_params, batch_rows)
        statements.clear()
        batch_params.clear()
        batch_rows.clear()
        batch_param_count = 0

    def flush_statement() -> None:
        nonlocal batch_param_count
        if not row_clauses:
            return
        values_clause = ", ".join(row_clauses)
        statements.append(f"INSERT INTO {target} ({column_list}) VALUES {values_clause}")
        batch_rows.append(list(stmt_rows))
        if batch_param_count + len(stmt_params) > _MAX_PARAMS_PER_STATEMENT:
            # Flush everything accumulated so far (not including the
            # statement just appended) before it pushes the batch over the
            # limit; the just-appended statement starts the next batch.
            last_stmt = statements.pop()
            last_rows = batch_rows.pop()
            flush_batch()
            statements.append(last_stmt)
            batch_rows.append(last_rows)
        batch_params.append(list(stmt_params))
        batch_param_count += len(stmt_params)
        row_clauses.clear()
        stmt_params.clear()
        stmt_rows.clear()

    for row in rows:
        cells = []
        row_params = []
        for value in row:
            if value is None:
                cells.append("NULL")
            else:
                cells.append("?")
                row_params.append(value)

        # Close out the current statement if adding this row would push it
        # over the per-statement row count or parameter limit.
        if row_clauses and (
            len(row_clauses) >= _MAX_ROWS_PER_STATEMENT
            or len(stmt_params) + len(row_params) > _MAX_PARAMS_PER_STATEMENT
        ):
            flush_statement()

        row_clauses.append("(" + ", ".join(cells) + ")")
        stmt_params.extend(row_params)
        stmt_rows.append(row)

    flush_statement()
    flush_batch()

    return inserted_count


def migrate_table(
    oracle_conn: oracledb.Connection,
    mssql_conn: pyodbc.Connection,
    table_cfg: dict,
    dry_run: bool = False,
    fast_executemany: bool = True,
) -> TableResult:
    """Copy all rows from one Oracle table into one SQL Server table.

    When dry_run is True, no data is inserted: the source row count is
    reported and the target table is checked for existence/accessibility.
    """
    source = table_cfg["source"]
    target = table_cfg["target"]
    where = table_cfg.get("where") or ""
    batch_size = int(table_cfg.get("batch_size", 1000))

    result = TableResult(source=source, target=target)
    start = time.monotonic()

    if dry_run:
        return _dry_run_table(oracle_conn, mssql_conn, source, target, where, result, start)

    try:
        oracle_conn.outputtypehandler = _output_type_handler
        has_identity = _target_has_identity_column(mssql_conn, target)
        with oracle_conn.cursor() as ora_cur:
            ora_cur.arraysize = batch_size
            query = f"SELECT * FROM {source}"
            if where.strip():
                query += f" WHERE {where}"
            logger.info("[%s] Running: %s", source, query)
            ora_cur.execute(query)

            columns = [col[0] for col in ora_cur.description]
            column_map = _build_column_map(mssql_conn, target, columns)
            target_columns = [column_map[c] for c in columns]
            placeholders = ", ".join(["?"] * len(target_columns))
            column_list = ", ".join(target_columns)
            insert_sql = f"INSERT INTO {target} ({column_list}) VALUES ({placeholders})"

            mssql_cur = mssql_conn.cursor()
            mssql_cur.fast_executemany = fast_executemany

            if has_identity:
                logger.info("[%s] Identity column detected; enabling IDENTITY_INSERT", target)
                mssql_cur.execute(f"SET IDENTITY_INSERT {target} ON")

            try:
                while True:
                    rows = ora_cur.fetchmany(batch_size)
                    if not rows:
                        break
                    result.rows_read += len(rows)

                    rows = [tuple(row) for row in rows]
                    if fast_executemany:
                        mssql_cur.executemany(insert_sql, rows)
                        inserted = len(rows)
                    else:
                        # Legacy ODBC driver: collapse many rows into a few
                        # multi-row INSERT statements instead of one round
                        # trip per row. Individual bad rows (e.g. rejected
                        # by the driver) are logged and skipped rather than
                        # failing the whole batch.
                        inserted = _insert_multi_row(mssql_cur, target, target_columns, rows)
                    mssql_conn.commit()
                    result.rows_inserted += inserted
                    skipped = len(rows) - inserted
                    if skipped:
                        result.rows_skipped += skipped
                    logger.info(
                        "[%s -> %s] Inserted %d rows (running total: %d, skipped: %d)",
                        source, target, inserted, result.rows_inserted, result.rows_skipped,
                    )
            finally:
                if has_identity:
                    mssql_cur.execute(f"SET IDENTITY_INSERT {target} OFF")
                    mssql_conn.commit()

        result.success = True
    except Exception as exc:  # noqa: BLE001 - report and continue with other tables
        mssql_conn.rollback()
        result.success = False
        result.error = str(exc)
        logger.exception("[%s -> %s] Migration failed", source, target)
    finally:
        result.seconds = time.monotonic() - start

    return result


# Oracle rejects IN-lists with more than 1000 expressions (ORA-01795).
_MAX_IN_LIST_SIZE = 1000


def sync_new_rows(
    oracle_conn: oracledb.Connection,
    mssql_conn: pyodbc.Connection,
    table_cfg: dict,
    fast_executemany: bool = True,
) -> TableResult:
    """Append only the rows that exist in the Oracle source but are not yet
    present in the SQL Server target, identified by comparing primary key
    values (``table_cfg["pk"]`` / ``table_cfg["source_pk"]``) rather than
    assuming new rows always have a higher key than everything already
    migrated. This is safe even if keys have gaps, are non-numeric (e.g.
    dbo.APPEAL's varchar appealid), or are not strictly increasing. Existing
    target rows are never touched - this only INSERTs rows whose PK is
    missing from the target.

    ``source_pk`` is quoted with double quotes when referenced in Oracle SQL
    text because these Oracle tables use quoted-lowercase column identifiers
    (unquoted references get uppercased by Oracle and fail to resolve).
    """
    source = table_cfg["source"]
    target = table_cfg["target"]
    where = table_cfg.get("where") or ""
    batch_size = int(table_cfg.get("batch_size", 1000))
    pk = table_cfg.get("pk")
    source_pk = table_cfg.get("source_pk") or pk

    result = TableResult(source=source, target=target)
    start = time.monotonic()

    if not pk:
        result.success = False
        result.error = "No 'pk' column configured for this table; cannot do incremental sync"
        result.seconds = time.monotonic() - start
        return result

    try:
        oracle_conn.outputtypehandler = _output_type_handler
        has_identity = _target_has_identity_column(mssql_conn, target)

        mssql_cur = mssql_conn.cursor()
        mssql_cur.arraysize = 10000
        mssql_cur.execute(f"SELECT [{pk}] FROM {target}")
        existing_pks = {row[0] for row in mssql_cur.fetchall()}
        logger.info("[%s] %d existing row(s) already in target", target, len(existing_pks))

        with oracle_conn.cursor() as ora_cur:
            ora_cur.arraysize = 10000
            ora_cur.prefetchrows = 10000
            pk_query = f'SELECT "{source_pk}" FROM {source}'
            if where.strip():
                pk_query += f" WHERE {where}"
            ora_cur.execute(pk_query)
            source_pks = [row[0] for row in ora_cur.fetchall()]
        result.rows_read = len(source_pks)
        logger.info("[%s] %d row(s) currently in source", source, len(source_pks))

        missing_pks = [v for v in source_pks if v not in existing_pks]
        logger.info("[%s -> %s] %d new row(s) to append", source, target, len(missing_pks))

        if not missing_pks:
            result.success = True
            result.seconds = time.monotonic() - start
            return result

        mssql_cur.fast_executemany = fast_executemany
        if has_identity:
            logger.info("[%s] Identity column detected; enabling IDENTITY_INSERT", target)
            mssql_cur.execute(f"SET IDENTITY_INSERT {target} ON")

        try:
            with oracle_conn.cursor() as ora_cur:
                ora_cur.arraysize = batch_size

                # Determine the Oracle -> SQL Server column mapping once
                # up front (some target columns were renamed after the
                # original migration), using a zero-row query just to get
                # column metadata.
                ora_cur.execute(f"SELECT * FROM {source} WHERE 1 = 0")
                oracle_columns = [col[0] for col in ora_cur.description]
                column_map = _build_column_map(mssql_conn, target, oracle_columns)
                target_columns = [column_map[c] for c in oracle_columns]
                column_list = ", ".join(target_columns)
                placeholders_sql = ", ".join(["?"] * len(target_columns))
                insert_sql = f"INSERT INTO {target} ({column_list}) VALUES ({placeholders_sql})"

                for i in range(0, len(missing_pks), _MAX_IN_LIST_SIZE):
                    chunk = missing_pks[i:i + _MAX_IN_LIST_SIZE]
                    placeholders = ", ".join(f":{j + 1}" for j in range(len(chunk)))
                    fetch_query = f'SELECT * FROM {source} WHERE "{source_pk}" IN ({placeholders})'
                    ora_cur.execute(fetch_query, chunk)
                    rows = ora_cur.fetchall()
                    if not rows:
                        continue

                    rows = [tuple(row) for row in rows]

                    if fast_executemany:
                        mssql_cur.executemany(insert_sql, rows)
                        inserted = len(rows)
                    else:
                        inserted = _insert_multi_row(mssql_cur, target, target_columns, rows)
                    mssql_conn.commit()

                    result.rows_inserted += inserted
                    skipped = len(rows) - inserted
                    if skipped:
                        result.rows_skipped += skipped
                    logger.info(
                        "[%s -> %s] Appended %d new rows (running total: %d, skipped: %d)",
                        source, target, inserted, result.rows_inserted, result.rows_skipped,
                    )
        finally:
            if has_identity:
                mssql_cur.execute(f"SET IDENTITY_INSERT {target} OFF")
                mssql_conn.commit()

        result.success = True
    except Exception as exc:  # noqa: BLE001 - report and continue with other tables
        mssql_conn.rollback()
        result.success = False
        result.error = str(exc)
        logger.exception("[%s -> %s] Incremental sync failed", source, target)
    finally:
        result.seconds = time.monotonic() - start

    return result


def _dry_run_table(
    oracle_conn: oracledb.Connection,
    mssql_conn: pyodbc.Connection,
    source: str,
    target: str,
    where: str,
    result: TableResult,
    start: float,
) -> TableResult:
    """Validate a table pair without inserting any data: count source rows
    and confirm the target table is reachable.
    """
    try:
        with oracle_conn.cursor() as ora_cur:
            count_query = f"SELECT COUNT(*) FROM {source}"
            if where.strip():
                count_query += f" WHERE {where}"
            ora_cur.execute(count_query)
            (row_count,) = ora_cur.fetchone()
            result.rows_read = row_count

        mssql_cur = mssql_conn.cursor()
        mssql_cur.execute(f"SELECT TOP 0 * FROM {target}")

        logger.info(
            "[DRY RUN] %s -> %s: %d source rows would be inserted (target table OK)",
            source, target, row_count,
        )
        result.success = True
    except Exception as exc:  # noqa: BLE001 - report and continue with other tables
        result.success = False
        result.error = str(exc)
        logger.exception("[DRY RUN] %s -> %s: validation failed", source, target)
    finally:
        result.seconds = time.monotonic() - start

    return result
