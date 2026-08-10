# Oracle to SQL Server Data Migration

Copies data from Oracle tables into existing Microsoft SQL Server tables,
using batched fetch/insert for large tables.

## Setup

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
   You also need the **ODBC Driver 17 (or 18) for SQL Server** installed on
   the machine running this script (Oracle side uses the `oracledb` thin
   driver, so no Oracle Instant Client is required).

2. Copy `.env.example` to `.env` and fill in real credentials:
   ```
   copy .env.example .env
   ```

3. Edit `config.yaml` to list every source (Oracle) / target (SQL Server)
   table pair you want copied. Target tables must already exist in SQL
   Server with matching (or compatible) column names/types.

## Run

```
python main.py
```

Options:
- `--config path.yaml` – use an alternate config file
- `--env path.env` – use an alternate env file
- `--table SOURCE_SCHEMA.TABLE` – limit the run to specific source table(s) (repeatable)
- `--log-level DEBUG|INFO|WARNING|ERROR` – logging verbosity
- `--dry-run` – validate Oracle/SQL Server connectivity and each table pair
  (counts source rows, confirms the target table is reachable) without
  inserting any data. Recommended before the first real run:
  ```
  python main.py --dry-run
  ```

Progress and errors are logged to the console and to `migration.log`. Each
table is migrated independently — a failure on one table is logged and does
not stop the remaining tables from running. A summary is printed at the end.

## Notes

- Data is appended to the target tables (no truncate/delete is performed).
- Rows are fetched and inserted in batches (`batch_size` per table in
  `config.yaml`) to keep memory usage low for large tables.
- Values are inserted using parameterized queries (`pyodbc` with
  `fast_executemany`), so there is no SQL injection risk from data values.
- CLOB/BLOB Oracle columns are converted to plain string/bytes before
  insertion.
