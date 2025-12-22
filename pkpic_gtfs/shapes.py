from typing import Dict, List, Tuple
import impuls
import requests
import polyline
import hashlib


class AddShapes(impuls.Task):
    def execute(self, r: impuls.TaskRuntime):
        shape_hash_to_id: Dict[str, str] = {}
        max_id = 0
        # shapes: List[str] = []
        wrong: List[str] = []
        with r.db.transaction():
            trips = list(
                filter(
                    lambda trip: "BUS" not in trip.route_id,
                    list(r.db.retrieve_all(impuls.model.Trip)),
                )
            )
            for i, trip in enumerate(trips):
                if i % 100 == 0:
                    self.logger.info(
                        f"Processing shapes for trip {i}/{len(trips)}. Created {len(shape_hash_to_id.keys())} shapes"
                    )
                stop_points = list(
                    r.db.raw_execute(
                        """
                        SELECT lat, lon
                        FROM stop_times JOIN stops on stops.stop_id == stop_times.stop_id
                        WHERE stop_times.trip_id = ?
                        """,
                        (trip.id,),
                    )
                )
                point_list = [f"{lat},{lon}" for lat, lon in stop_points]
                shape_hash = _hash_stop_points(point_list)

                if shape_hash not in shape_hash_to_id.keys() and shape_hash not in wrong:
                    try:
                        shape = _get_shape_from_osrm(point_list)

                        max_id += 1
                        shape_id = str(max_id)
                        shape_hash_to_id[shape_hash] = shape_id
                        r.db.raw_execute("INSERT INTO shapes (shape_id) VALUES (?)", (shape_id,))
                        for i, (lat, lon) in enumerate(shape):
                            r.db.create(
                                impuls.model.ShapePoint(
                                    shape_id=shape_id,
                                    lat=lat,
                                    lon=lon,
                                    sequence=i,
                                )
                            )

                        trip.shape_id = shape_id
                        r.db.update(trip)
                    except Exception as e:
                        wrong.append(shape_hash)
                        self.logger.warning(
                            f"Error while getting shape for {trip.id}, exception: {e}"
                        )
                elif shape_hash not in wrong:
                    shape_id = shape_hash_to_id[shape_hash]
                    trip.shape_id = shape_id
                    r.db.update(trip)


def _hash_stop_points(points: List[str]) -> str:
    shape = ";".join(points)
    m = hashlib.sha256()
    m.update(shape.encode())
    return m.hexdigest()


def _get_shape_from_openrailwayrouting(point_list: List[str]) -> List[Tuple[float, float]]:
    params = {
        "point": point_list,
        "type": "json",
        "locale": "pl",
        "key": "",
        "elevation": "false",
        "profile": "all_tracks",
    }
    response = requests.get("https://routing.openrailrouting.org/route", params=params)
    response.raise_for_status()

    data = response.json()["paths"][0]["points"]
    return polyline.decode(data)


def _get_shape_from_osrm(point_list: List[str]) -> List[Tuple[float, float]]:
    base_url = "http://localhost:5000/route/v1/train"
    coordinates = ";".join([reverse(x) for x in point_list])

    params = {"overview": "full", "geometries": "polyline"}
    response = requests.get(f"{base_url}/{coordinates}", params=params)
    response.raise_for_status()

    data = response.json()["routes"][0]["geometry"],
    return polyline.decode(data)

def reverse(x:str):
    a, b = x.split(",")
    return f"{b},{a}"
