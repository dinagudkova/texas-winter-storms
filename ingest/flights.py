"""Download BTS On-Time Performance data and save the relevant flights as Parquet.

One Parquet file per month: data/raw/flights/flights_YYYY_MM.parquet
Only flights departing from or arriving at the configured airports are kept.
Column names stay as in the BTS source; renaming happens in dbt staging.

Usage:
    python -m ingest.flights                    # all events in config/project.yml
    python -m ingest.flights --event fern_2026  # one event
    python -m ingest.flights --force            # rebuild existing Parquet files
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
import zipfile
from collections import defaultdict
from pathlib import Path

import duckdb
import requests

from ingest.config import DOWNLOADS_DIR, RAW_DIR, load_config

BTS_URL = "https://transtats.bts.gov/PREZIP/"
# The month has no leading zero in BTS file names: 2021_2, not 2021_02.
ZIP_NAME = "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"

# Columns to keep and the type each one is cast to.
# Compared to the original group project this adds cancellation reasons,
# delay causes and taxi-out time (a proxy for de-icing queues).
COLUMNS = {
    "FlightDate": "DATE",
    "Reporting_Airline": "VARCHAR",
    "Tail_Number": "VARCHAR",
    "Flight_Number_Reporting_Airline": "INTEGER",
    "Origin": "VARCHAR",
    "Dest": "VARCHAR",
    "CRSDepTime": "VARCHAR",  # scheduled local time, hhmm; text keeps leading zeros
    "DepTime": "VARCHAR",  # actual local time, hhmm, can be "2400"
    "DepDelay": "DOUBLE",  # minutes
    "TaxiOut": "DOUBLE",  # minutes
    "WheelsOff": "VARCHAR",
    "CRSArrTime": "VARCHAR",
    "ArrTime": "VARCHAR",
    "ArrDelay": "DOUBLE",
    "Cancelled": "DOUBLE",  # 1.00 / 0.00 in the source
    "CancellationCode": "VARCHAR",  # A carrier, B weather, C NAS, D security
    "Diverted": "DOUBLE",
    "CRSElapsedTime": "DOUBLE",
    "ActualElapsedTime": "DOUBLE",
    "AirTime": "DOUBLE",
    "Distance": "DOUBLE",  # miles
    "CarrierDelay": "DOUBLE",
    "WeatherDelay": "DOUBLE",
    "NASDelay": "DOUBLE",
    "SecurityDelay": "DOUBLE",
    "LateAircraftDelay": "DOUBLE",
}


def download_zip(year: int, month: int, insecure: bool = False) -> Path:
    """Download the monthly ZIP into data/downloads, reusing it if it already exists."""
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    name = ZIP_NAME.format(year=year, month=month)
    path = DOWNLOADS_DIR / name

    if path.exists() and zipfile.is_zipfile(path):
        print(f"  cached   {name}")
        return path

    url = BTS_URL + name
    tmp_path = path.with_suffix(".part")  # avoids leaving a broken .zip if the download stops
    for attempt in range(1, 4):
        try:
            print(f"  download {name} (attempt {attempt}) ...")
            with requests.get(url, stream=True, timeout=120, verify=not insecure) as response:
                response.raise_for_status()
                with open(tmp_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
            tmp_path.rename(path)
            break
        except requests.exceptions.SSLError as error:
            sys.exit(
                "SSL verification failed for transtats.bts.gov. The BTS server has had "
                "certificate chain issues in the past. If you trust the source, re-run "
                f"with --insecure.\nDetails: {error}"
            )
        except requests.RequestException as error:
            if attempt == 3:
                sys.exit(f"Download failed for {url}: {error}")
            time.sleep(5 * attempt)

    # A month that is not published yet can come back as an HTML page instead of a ZIP.
    if not zipfile.is_zipfile(path):
        path.unlink()
        sys.exit(
            f"{name} is not a valid ZIP file. BTS publishes data with a lag of about "
            "3 months, so this month may not be available yet."
        )

    print(f"  saved    {name} ({path.stat().st_size / 1e6:.1f} MB)")
    return path


def zip_to_parquet(zip_path: Path, out_path: Path, airports: list[str]) -> None:
    """Extract the CSV, keep the needed columns and airports, write Parquet."""
    out_path.parent.mkdir(parents=True, exist_ok=True)

    select_list = ",\n                ".join(
        f'CAST("{col}" AS {dtype}) AS "{col}"' for col, dtype in COLUMNS.items()
    )
    airport_list = ", ".join(f"'{code}'" for code in airports)

    with tempfile.TemporaryDirectory() as tmp_dir, zipfile.ZipFile(zip_path) as zf:
        csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
        zf.extract(csv_name, tmp_dir)
        csv_path = Path(tmp_dir) / csv_name

        # all_varchar reads every column as text; types are set explicitly by CAST,
        # so they are the same for every month regardless of what the data looks like.
        query = f"""
            COPY (
                SELECT
                    {select_list},
                    '{zip_path.name}' AS _source_file
                FROM read_csv('{csv_path.as_posix()}', header = true, all_varchar = true)
                WHERE "Origin" IN ({airport_list}) OR "Dest" IN ({airport_list})
                ORDER BY "FlightDate", "CRSDepTime"
            ) TO '{out_path.as_posix()}' (FORMAT parquet)
        """
        with duckdb.connect() as con:
            con.execute(query)


def validate(out_path: Path, year: int, month: int) -> None:
    """Check the written file and print a short summary."""
    rows, min_date, max_date, cancelled, cancelled_with_code = duckdb.sql(f"""
        SELECT
            count(*),
            min("FlightDate"),
            max("FlightDate"),
            coalesce(sum("Cancelled"), 0)::INTEGER,
            count(*) FILTER (WHERE "Cancelled" = 1 AND "CancellationCode" IS NOT NULL)
        FROM '{out_path.as_posix()}'
    """).fetchone()

    if rows == 0:
        sys.exit(f"{out_path.name}: no rows after filtering, check the airport codes")
    for day in (min_date, max_date):
        if (day.year, day.month) != (year, month):
            sys.exit(f"{out_path.name}: dates {min_date}..{max_date} are outside {year}-{month:02d}")
    if cancelled_with_code != cancelled:
        print(f"  warning  {cancelled - cancelled_with_code} cancelled flights without CancellationCode")

    print(f"  wrote    {out_path.name}: {rows:,} flights, {cancelled:,} cancelled, {min_date}..{max_date}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--event", action="append", help="event id from the config (repeatable)")
    parser.add_argument("--force", action="store_true", help="rebuild existing Parquet files")
    parser.add_argument("--insecure", action="store_true", help="skip SSL verification for BTS")
    args = parser.parse_args()

    config = load_config()
    events = config.select_events(args.event)

    # Several events can share a month, so collect the airports needed for each month.
    airports_by_month: dict[tuple[int, int], set[str]] = defaultdict(set)
    for event in events:
        for year_month in event.months():
            airports_by_month[year_month].update(event.airports)

    for (year, month), airports in sorted(airports_by_month.items()):
        out_path = RAW_DIR / "flights" / f"flights_{year}_{month:02d}.parquet"
        print(f"\n{year}-{month:02d}  airports: {', '.join(sorted(airports))}")

        if out_path.exists() and not args.force:
            print(f"  skip     {out_path.name} already exists (use --force to rebuild)")
            continue

        zip_path = download_zip(year, month, insecure=args.insecure)
        zip_to_parquet(zip_path, out_path, sorted(airports))
        validate(out_path, year, month)


if __name__ == "__main__":
    main()