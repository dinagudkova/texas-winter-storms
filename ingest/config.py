"""Read config/project.yml and provide paths and helpers shared by the ingestion scripts."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

import yaml

# Paths are built from this file's location, so scripts work from any working directory.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config" / "project.yml"
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
DOWNLOADS_DIR = DATA_DIR / "downloads"


@dataclass(frozen=True)
class Airport:
    code: str
    name: str
    city: str
    meteostat_station: str
    timezone: str


@dataclass(frozen=True)
class Event:
    event_id: str
    name: str
    data_start: dt.date
    data_end: dt.date
    storm_start: dt.date
    storm_end: dt.date
    airports: tuple[str, ...]

    def months(self) -> list[tuple[int, int]]:
        """Return all (year, month) pairs between data_start and data_end, inclusive."""
        result = []
        year, month = self.data_start.year, self.data_start.month
        while (year, month) <= (self.data_end.year, self.data_end.month):
            result.append((year, month))
            if month == 12:
                year, month = year + 1, 1
            else:
                month += 1
        return result


@dataclass(frozen=True)
class ProjectConfig:
    airports: dict[str, Airport]
    events: dict[str, Event]

    def select_events(self, event_ids: list[str] | None = None) -> list[Event]:
        """Return the requested events, or all events if none are given."""
        if not event_ids:
            return list(self.events.values())
        unknown = set(event_ids) - set(self.events)
        if unknown:
            raise ValueError(
                f"Unknown event(s): {sorted(unknown)}. Available: {sorted(self.events)}"
            )
        return [self.events[event_id] for event_id in event_ids]


def load_config(path: Path = CONFIG_PATH) -> ProjectConfig:
    """Read the YAML config, validate it and return typed objects."""
    raw = yaml.safe_load(path.read_text())

    airports = {}
    for code, attrs in raw["airports"].items():
        airports[code] = Airport(
            code=code,
            name=attrs["name"],
            city=attrs["city"],
            # str() guards against a station id written without quotes in YAML
            meteostat_station=str(attrs["meteostat_station"]),
            timezone=attrs["timezone"],
        )

    events = {}
    for event_id, attrs in raw["events"].items():
        event = Event(
            event_id=event_id,
            name=attrs["name"],
            data_start=attrs["data_start"],
            data_end=attrs["data_end"],
            storm_start=attrs["storm_start"],
            storm_end=attrs["storm_end"],
            airports=tuple(attrs["airports"]),
        )
        _validate_event(event, airports)
        events[event_id] = event

    return ProjectConfig(airports=airports, events=events)


def _validate_event(event: Event, airports: dict[str, Airport]) -> None:
    """Fail early with a clear message if the config has a mistake."""
    for field in ("data_start", "data_end", "storm_start", "storm_end"):
        if not isinstance(getattr(event, field), dt.date):
            raise ValueError(f"Event {event.event_id}: {field} must be a date like 2021-01-31")

    missing = set(event.airports) - set(airports)
    if missing:
        raise ValueError(
            f"Event {event.event_id} uses airports not defined in config: {sorted(missing)}"
        )

    if not (event.data_start <= event.storm_start <= event.storm_end <= event.data_end):
        raise ValueError(
            f"Event {event.event_id}: storm window must lie inside the data window"
        )


if __name__ == "__main__":
    # Quick check: python -m ingest.config
    config = load_config()
    for airport in config.airports.values():
        print(f"{airport.code}: station {airport.meteostat_station}, {airport.name}")
    for event in config.events.values():
        print(f"{event.event_id}: {event.name}, months {event.months()}")