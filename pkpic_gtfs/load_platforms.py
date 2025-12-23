from typing import Any, Dict, List
import impuls
import json


class LoadPlatformData(impuls.Task):
    def __init__(self) -> None:
        super().__init__()

    @staticmethod
    def load_platforms(
        path: impuls.tools.types.StrPath,
    ) -> Dict[str, List[Dict[str, Any]]]:
        with open(path, "r", encoding="utf-8") as f:
            platforms = json.load(f)
        return platforms

    def execute(self, r: impuls.TaskRuntime) -> None:
        platforms_in_db = r.db.raw_execute(
            """
        SELECT DISTINCT name, stop_id, platform, json_extract(stop_times.extra_fields_json, '$.track') AS track
        FROM stop_times join stops USING (stop_id)
        WHERE track IS NOT NULL and track is not '' AND platform IS NOT NULL AND platform is not '' AND platform is not 'BUS'
        """
        ).all()
        print(f"Found {len(platforms_in_db)} platforms in DB")
        print(platforms_in_db[:10])
        platforms = self.load_platforms(r.resources["platforms.json"].stored_at)
        for name, stop_id, platform_number, track in platforms_in_db:
            stop_with_platform_id = f"{stop_id}_{platform_number}_{track}"
            platforms_for_station = platforms.get(str(name), [])
            if len(platforms_for_station) == 0:
                self.logger.warning(f"Station not found {name}")
                continue
            platforms_for_platform_number = [
                p for p in platforms_for_station if p.get("platform") == platform_number
            ]
            if len(platforms_for_platform_number) == 1:
                platform = platforms_for_platform_number[0]
            elif len(platforms_for_platform_number) > 1:
                platforms_for_track = [
                    p for p in platforms_for_platform_number if p.get("track") == track
                ]
                if len(platforms_for_track) == 1:
                    platform = platforms_for_track[0]
                elif len(platforms_for_track) > 1:
                    self.logger.warning(
                        f"Multiple matching platforms found for {name} platform {platform_number} track {track}"
                    )
                    continue
                else:
                    self.logger.warning(
                        f"Platform not found for {name} platform {platform_number} track {track}"
                    )
                    continue
            else:
                self.logger.warning(
                    f"Platform not found for {name} platform {platform_number}"
                )
                continue

            location = platform.get("location")
            if not location:
                self.logger.error(
                    f"Platform {name} track {track} has no location, falling back to parent stop"
                )
                parent_stop = r.db.retrieve_must(impuls.model.Stop, stop_id)
                location = [parent_stop.lon, parent_stop.lat]

            r.db.create(
                impuls.model.Stop(
                    id=stop_with_platform_id,
                    name=str(name),
                    parent_station=str(stop_id),
                    location_type=impuls.model.Stop.LocationType.STOP,  # TODO: set 1 for parent station?
                    platform_code=f"{platform_number}/{track}",
                    lon=location[0],
                    lat=location[1],
                )
            )
            r.db.raw_execute(
                """
                UPDATE stop_times
                SET stop_id = ?
                WHERE stop_id = ? AND platform = ? AND json_extract(extra_fields_json, '$.track') = ?
                """,
                (stop_with_platform_id, stop_id, platform_number, track),
            )
