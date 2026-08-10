# fde-ce-fot-etl

Festival Of Trees data-upload ETL from Oracle Tactical DB (Databricks-loaded source data) to MSSQL is implemented in:

- `./fot_oracle_to_mssql_etl.py`

## Required environment variables

- `ORACLE_USER`
- `ORACLE_PASSWORD`
- `ORACLE_DSN`
- `MSSQL_SERVER`
- `MSSQL_DATABASE`
- `MSSQL_USER`
- `MSSQL_PASSWORD`
- Optional: `MSSQL_DRIVER` (default: `ODBC Driver 18 for SQL Server`)
- Optional: `MSSQL_TRUST_SERVER_CERTIFICATE` (default: `no`)

## Run

```bash
python ./fot_oracle_to_mssql_etl.py \
  --source-query "SELECT * FROM TACTICAL.FESTIVAL_OF_TREES_UPLOAD" \
  --target-table "dbo.FestivalOfTreesUpload" \
  --batch-size 1000 \
  --truncate-target
```
