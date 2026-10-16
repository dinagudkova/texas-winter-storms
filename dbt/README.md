# dbt project

Transforms the raw Parquet files from `ingest/` into analysis-ready tables in DuckDB
(`data/warehouse.duckdb`).

## Run

Always run dbt from this folder, so that relative paths to the data resolve:

    cd dbt
    dbt seed     # load storm_events.csv
    dbt build    # build all models and run all tests
    dbt docs generate && dbt docs serve   # documentation and lineage graph

## Layers

- `models/staging/` — one model per source: renaming, types, units, UTC to local time (views)
- `models/prep/` — local flight timestamps and hourly weather parsed from METAR codes (tables)
- `models/marts/` — flights with weather, airport hours and the storm summary (tables)
- `seeds/storm_events.csv` — approximate storm dates; keep in sync with `config/project.yml`
- `tests/` — singular tests that check counts across models