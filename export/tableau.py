"""Export the dbt marts from DuckDB to CSV files for Tableau.

Tableau Public cannot connect to DuckDB, so every mart_* table is written to
data/tableau/<table>.csv. Run it after `dbt build`. The files are not tracked by git:
the data is embedded in the published Tableau workbook.

Usage:
    python -m export.tableau
"""

from __future__ import annotations

import argparse

import duckdb

from ingest.config import DATA_DIR

WAREHOUSE = DATA_DIR / "warehouse.duckdb"
OUT_DIR = DATA_DIR / "tableau"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.parse_args()

    if not WAREHOUSE.exists():
        raise SystemExit(f"{WAREHOUSE} not found. Build the marts first: cd dbt && dbt build")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # read-only, so the export can run while a notebook has the database open
    with duckdb.connect(str(WAREHOUSE), read_only=True) as con:
        tables = [row[0] for row in con.sql("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_type = 'BASE TABLE'
              AND starts_with(table_name, 'mart_')
            ORDER BY table_name
        """).fetchall()]
        if not tables:
            raise SystemExit("No mart_* tables in the warehouse. Run `dbt build` in dbt/ first.")

        for table in tables:
            path = OUT_DIR / f"{table}.csv"
            con.execute(f"COPY (SELECT * FROM {table}) TO '{path.as_posix()}' (HEADER, DELIMITER ',')")
            rows = con.sql(f"SELECT count(*) FROM {table}").fetchone()[0]
            print(f"wrote  {path.name}: {rows:,} rows")


if __name__ == "__main__":
    main()