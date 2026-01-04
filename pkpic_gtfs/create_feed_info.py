# Copyright (c) 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from zoneinfo import ZoneInfo

from impuls import Task, TaskRuntime
from impuls.model import FeedInfo
import impuls

POLAND_TZ = ZoneInfo("Europe/Warsaw")


class CreateFeedInfo(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        source_timestamp = r.resources["kpd_rozklad.csv"].last_modified.astimezone(POLAND_TZ)
        r.db.create(
            FeedInfo(
                publisher_name="kasmar00",
                publisher_url="https://gtfs.kasznia.net",
                lang="pl",
                version=source_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            )
        )

        r.db.create_many(
                impuls.model.Attribution,
                [
                    impuls.model.Attribution(
                        id=1,
                        organization_name="Platform locations and trip shapes © OpenStreetMap contributors under ODbL",
                        is_producer=True,
                        url="https://openstreetmap.org/copyright",
                    ),
                    impuls.model.Attribution(
                        id=2,
                        organization_name="PKP Intercity",
                        is_operator=True,
                        url="https://www.intercity.pl/",
                    ),
                    impuls.model.Attribution(
                        id="3",
                        organization_name="kasmar00",
                        is_producer=True,
                        url="https://gtfs.kasznia.net",
                    ),
                    impuls.model.Attribution(
                        id="4",
                        organization_name="MKuran",
                        is_producer=True,
                        url="https://mkuran.pl/gtfs/",
                    )
                ],
            )
