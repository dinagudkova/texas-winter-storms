"""Download METAR weather observations from the Iowa Environmental Mesonet (IEM) archive.

Source: https://mesonet.agron.iastate.edu/request/download.phtml (no API key needed).
One Parquet file per airport and month: data/raw/metar/metar_AIRPORT_YYYY_MM.parquet

The raw layer keeps the data as the source delivers it:
- all values are text, in US units (°F, knots, inches, miles);
- "M" means missing (for wxcodes: no weather phenomenon reported), "T" means trace;
- timestamps are UTC.
Parsing, unit conversion and hourly aggregation happen in dbt.

Each file covers one *local* month (airport time zone), so files never overlap.

Usage:
    python -m ingest.weather                    # all events in config/project.yml
    python -m ingest.weather --event fern_2026  # one event
    python -m ingest.weather --force            # re-download existing files
"""

from __future__ import annotations

import argparse
import calendar
import datetime as dt
import io
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd
import requests

from ingest.config import RAW_DIR, Airport, load_config

IEM_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"

FIELDS = [
    "tmpf",  # air temperature, °F
    "dwpf",  # dew point, °F
    "relh",  # relative humidity, %
    "drct",  # wind direction, degrees
    "sknt",  # wind speed, knots
    "gust",  # wind gust, knots
    "vsby",  # visibility, miles
    "p01i",  # precipitation since the last routine report, inches
    "wxcodes",  # present weather codes, e.g. "-FZRA BR"
    "ice_accretion_1hr",  # ice accretion over the last hour, inches
    "skyc1",  # lowest cloud layer coverage (FEW, SCT, BKN, OVC)
    "skyl1",  # lowest cloud layer height, feet
    "metar",  # the original METAR text, kept for traceability
]

# IEM report types: 3 = routine (hourly, around :53), 4 = special (issued when weather changes).
REPORT_TYPES = {3: "routine", 4: "special"}


def request_csv(station: str, start: dt.date, end: dt.date, report_type: int) -> str:
    """Request one CSV from IEM. `end` is exclusive."""
    params = [
        ("station", station),
        *[("data", field) for field in FIELDS],
        ("year1", start.year), ("month1", start.month), ("day1", start.day),
        ("year2", end.year), ("month2", end.month), ("day2", end.day),
        ("tz", "Etc/UTC"),
        ("format", "onlycomma"),
        ("latlon", "no"),
        ("elev", "no"),
        ("missing", "M"),
        ("trace", "T"),
        ("report_type", report_type),
    ]
    for attempt in range(1, 6):
        try:
            response = requests.get(IEM_URL, params=params, timeout=120)
            # IEM answers 503 under heavy load and 429 when requests come too fast.
            if response.status_code in (429, 503):
                raise requests.HTTPError(f"HTTP {response.status_code}", response=response)
            response.raise_for_status()
            return response.text
        except requests.RequestException as error:
            if attempt == 5:
                sys.exit(f"IEM request failed for {station} {start}..{end}: {error}")
            wait = 5 * attempt
            print(f"  retry  {station}: {error}, waiting {wait} s")
            time.sleep(wait)
    raise AssertionError("unreachable")


def fetch_month(airport: Airport, year: int, month: int) -> pd.DataFrame:
    """Fetch routine and special reports for one local month."""
    month_start = dt.date(year, month, 1)
    month_end = month_start + dt.timedelta(days=calendar.monthrange(year, month)[1])  # exclusive

    # Requests are in UTC, so pad by a day on each side and cut to the local month below.
    request_start = month_start - dt.timedelta(days=1)
    request_end = month_end + dt.timedelta(days=1)

    frames = []
    for report_type, label in REPORT_TYPES.items():
        text = request_csv(airport.iem_station, request_start, request_end, report_type)
        # dtype=str and keep_default_na=False keep "M" and "T" as text instead of NaN.
        df = pd.read_csv(io.StringIO(text), dtype=str, keep_default_na=False)
        df["report_type"] = label
        frames.append(df)
        time.sleep(1)  # IEM allows about one request per second

    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        return df

    df["valid_utc"] = pd.to_datetime(df["valid"], format="%Y-%m-%d %H:%M")
    local = df["valid_utc"].dt.tz_localize("UTC").dt.tz_convert(airport.timezone)
    in_month = (local.dt.year == year) & (local.dt.month == month)
    df = df.loc[in_month].drop(columns="valid")

    df.insert(0, "airport_code", airport.code)
    columns = ["airport_code", "station", "valid_utc", "report_type", *FIELDS]
    return df[columns].sort_values(["valid_utc", "report_type"]).reset_index(drop=True)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # DuckDB can query a pandas DataFrame by its variable name.
    duckdb.sql(f"COPY (SELECT * FROM df) TO '{path.as_posix()}' (FORMAT parquet)")


def summarize(df: pd.DataFrame, year: int, month: int) -> str:
    expected_hours = calendar.monthrange(year, month)[1] * 24
    routine = int((df["report_type"] == "routine").sum())
    special = int((df["report_type"] == "special").sum())
    temps = pd.to_numeric(df["tmpf"], errors="coerce")  # "M" becomes NaN
    # Rough check only: freezing rain/drizzle, ice pellets or snow, excluding blowing/drifting snow.
    winter = df["wxcodes"].str.contains(r"FZRA|FZDZ|PL|(?<!BL)(?<!DR)SN", regex=True)
    return (
        f"{routine} routine of ~{expected_hours} hours, {special} special, "
        f"temp {temps.min():.0f}..{temps.max():.0f} °F, "
        f"winter weather in {int(winter.sum())} reports"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--event", action="append", help="event id from the config (repeatable)")
    parser.add_argument("--force", action="store_true", help="re-download existing files")
    args = parser.parse_args()

    config = load_config()

    # One job per airport and month; a set removes duplicates if events overlap.
    jobs = sorted({
        (code, year, month)
        for event in config.select_events(args.event)
        for code in event.airports
        for year, month in event.months()
    })

    for code, year, month in jobs:
        airport = config.airports[code]
        path = RAW_DIR / "metar" / f"metar_{code}_{year}_{month:02d}.parquet"
        label = f"{code} {year}-{month:02d}"

        if path.exists() and not args.force:
            print(f"skip   {label}: {path.name} already exists (use --force to re-download)")
            continue

        df = fetch_month(airport, year, month)
        if df.empty:
            print(f"WARN   {label}: no reports returned for station {airport.iem_station}")
            continue

        write_parquet(df, path)
        print(f"wrote  {label}: {summarize(df, year, month)}")


if __name__ == "__main__":
    main()