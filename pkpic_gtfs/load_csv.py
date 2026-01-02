# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import csv
import json
from collections.abc import Iterator, Sequence
from itertools import groupby
from operator import itemgetter

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import StopTime, TimePoint, Trip
from impuls.tools.types import StrPath

CSVRow = dict[str, str]
TrainKey = tuple[str, str]

MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR

ROMAN_TO_ARABIC = {
    "I": "1",
    "II": "2",
    "III": "3",
    "IV": "4",
    "V": "5",
    "VI": "6",
    "VII": "7",
    "VIII": "8",
    "IX": "9",
    "X": "10",
    "XI": "11",
    "XII": "12",
}


class LoadCSV(Task):
    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            rows = train_rows(r.resources["kpd_rozklad.csv"].stored_at)
            trips = (parse_train(list(i)) for _, i in rows)
            for trip, stop_times in trips:
                self.save_trip(r.db, trip)
                self.save_stop_times(r.db, stop_times)

    @staticmethod
    def save_trip(db: DBConnection, trip: Trip) -> None:
        db.raw_execute(
            (
                "INSERT OR IGNORE INTO routes "
                "(route_id, agency_id, short_name, long_name, type) "
                "VALUES (?, '0', ?, '', 2)"
            ),
            (trip.route_id, trip.route_id),
        )
        db.raw_execute(
            (
                "INSERT OR IGNORE INTO calendars (calendar_id, start_date, end_date, monday, "
                "tuesday, wednesday, thursday,friday, saturday, sunday) VALUES "
                "(?, ?, ?, 1, 1, 1, 1, 1, 1, 1)"
            ),
            (trip.calendar_id, trip.calendar_id, trip.calendar_id),
        )
        db.create(trip)

    @staticmethod
    def save_stop_times(db: DBConnection, stop_times: Sequence[StopTime]) -> None:
        db.raw_execute_many(
            "INSERT OR IGNORE INTO stops (stop_id, name, lat, lon) VALUES (?, ?, 0.0, 0.0)",
            ((i.stop_id, i.get_extra_field("stop_name") or "") for i in stop_times),
        )
        db.create_many(StopTime, stop_times)


def train_rows(filename: StrPath) -> Iterator[tuple[TrainKey, Iterator[CSVRow]]]:
    # NOTE: This assumes that the input file is sorted on (DataOdjazdu, NrPociagu, Lp).
    #       For the past 5 years that was the case.
    with open(filename, "r", encoding="windows-1250", newline="") as f:
        all_rows = csv.DictReader(f, delimiter=";")
        pax_rows = filter(lambda r: r["StacjaHandlowa"] == "1" or r["NumerStacji"] in NON_PAX_IMPORTANT_STOPS , all_rows)
        yield from groupby(pax_rows, itemgetter("DataOdjazdu", "NrPociagu"))


def parse_train(rows: list[CSVRow]) -> tuple[Trip, list[StopTime]]:
    # Extract basic train data
    category = rows[0]["KategoriaHandlowa"].replace("  ", " ")
    name = rows[0]["NazwaPociagu"]
    number = rows[0]["NrPociaguHandlowy"]
    calendar_id = rows[0]["DataOdjazdu"]
    number_operational = rows[0]["NrPociagu"]
    trip_id = calendar_id + "_" + number_operational.replace("/", "-")

    # Fix for missing NrPociaguHandlowy
    if number == "":
        number = number_operational.partition("/")[0]

    # Generate a sensible train number to show
    if name and number in name:
        display_name = name.title().replace("Zka", "ZKA")
    elif name:
        display_name = f"{number} {name.title()}"
    else:
        display_name = number

    trip = Trip(
        id=trip_id,
        route_id=category,
        calendar_id=calendar_id,
        short_name=display_name,
    )
    trip.set_extra_fields({"plk_train_number": number_operational})

    # Generate StopTimes, avoiding time travel
    stop_times = list[StopTime]()
    previous_dep = 0
    dist_offset = int(rows[0]["DrogaKumulowanaMetry"])
    for idx, row in enumerate(rows):
        # Basic stop info
        stop_id = row["NumerStacji"]
        dist = int(row["DrogaKumulowanaMetry"]) - dist_offset

        # Parse time
        arr = parse_time(row["Przyjazd"])
        dep = parse_time(row["Odjazd"])
        if arr < previous_dep:
            arr += DAY
        if dep < arr:
            dep += DAY

        # Parse platform
        platform = ""
        if row["BUS"] == "1" or row["PeronWyjazd"] == "BUS":
            platform = "BUS"
        else:
            platform = normalize_platform(row["PeronWyjazd"] or row["PeronWjazd"])
        if row["StacjaHandlowa"] != "1":
            if stop_id not in NON_PAX_IMPORTANT_STOPS:
                continue
            platform = "NO_PAX"

        # Parse other metadata
        track = row["TorWyjazd"] or row["TorWjazd"]
        vehicle_kind = row["Pojazd"]

        stop_time = StopTime(
            trip_id=trip_id,
            stop_id=stop_id,
            stop_sequence=idx,
            arrival_time=TimePoint(seconds=arr),
            departure_time=TimePoint(seconds=dep),
            platform=platform,
        )
        stop_time.set_extra_fields(
            {
                "track": track,
                "fare_dist_m": str(dist),
                "vehicle_kind": vehicle_kind,
            },
        )

        stop_times.append(stop_time)
        previous_dep = dep

    # Ensure arrival and departure times match at first and last stops
    if stop_times:
        stop_times[0].arrival_time = stop_times[0].departure_time
        stop_times[-1].departure_time = stop_times[-1].arrival_time

    return trip, ensure_start_and_end_at_pax_station(stop_times)

def ensure_start_and_end_at_pax_station(stops: list[StopTime]) -> list[StopTime]:
    start = 0
    end = len(stops) - 1

    while start <= end and stops[start].platform == "NO_PAX":
        start += 1

    while end >= start and stops[end].platform == "NO_PAX":
        end -= 1

    return stops[start:end + 1]

def parse_time(x: str) -> int:
    h, m, s = map(int, x.split(":"))
    return h * HOUR + m * MINUTE + s


def normalize_platform(x: str) -> str:
    if x[-1:] == "a":
        base = x[:-1]
        suffix = "a"
    else:
        base = x
        suffix = ""

    base = ROMAN_TO_ARABIC.get(base, base)
    return f"{base}{suffix}"

# List of non pax stops that are important for routing
NON_PAX_IMPORTANT_STOPS = [
    # Swarzędz - Poznań: Franowo vs Wschodni
    "28522", # Poznań Starołęka
    # "29801", # Poznań Wschód - excluded, due to problematic location
    # CMK vs other routes
    "64899", # Włoszczowa Północ
    "48959", # Opoczno Południe
    # Szczecin Dąbie - Szczecin Główny: Port Centralny vs Dziewoklicz
    "299", # Dziewoklicz
    "109", # Szczecin Port Centralny
    # Warszawa Wschodnia - Otwock and Wołomin
    "265314", # Warszawa Wschodnia Towarowa R49
    "265315", # Warszawa Wschodnia Towarowa R51
    # Kraków: "Small Bypass" (to/via Płaszów, not via Główny)
    "80200", #Kraków Olsza
]