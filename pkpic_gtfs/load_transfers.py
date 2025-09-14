import csv


from impuls import DBConnection, Task, TaskRuntime
from impuls.model import StopTime, TimePoint, Trip, Transfer, Stop, Calendar
from impuls.tools.types import StrPath





class LoadTransfers(Task):
    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            transfers = r.resources["kpd_rodzklad_przelaczenia.csv"].stored_at
            with open(transfers, "r", encoding="windows-1250") as f:
                reader = csv.DictReader(f, delimiter=";")

                stops = r.db.retrieve_all(Stop).all()
                calendars = [cal.id for cal in r.db.retrieve_all(Calendar).all()]
                for row in reader:
                    self.save_transfer(r.db, row, stops, calendars)

    @staticmethod
    def save_transfer(db: DBConnection, row: dict[str, str], stops, calendars) -> None:
        # print(row)
        from_train_id = row["NrPoc1"].strip().replace("/", "-")
        to_train_id = row["NrPoc2"].strip().replace("/", "-")
        stop_name = row["StacjaPrzełączenia"].strip()

        stop_id = [s.id for s in stops if s.name == stop_name][0]

        for calendar in calendars[:7]:
            valid = is_valid_for_date(row, calendar, row["Timetable_no"])
            if not valid:
                continue
            transfer = Transfer(
                from_trip_id=calendar + "_" + from_train_id,
                to_trip_id=calendar + "_" + to_train_id,
                from_stop_id=stop_id,
                to_stop_id=stop_id,
                type= Transfer.Type.INSEAT,
            )
            # print(transfer)
            try:
                db.create(transfer)
            except Exception as e:
                print(f"IsValid: {valid} for {calendar}")
                print(f"Error saving transfer {transfer}: {e}")


def is_valid_for_date(row: dict[str, str], date: str, timetable: str) -> bool:
    year, month, day = date.split("-")
    timetable_first_year = timetable.split("/")[0]

    if (year == timetable_first_year and month == "12"):
        return row["mth00"][int(day) - 1] == "1"
    else:
        return row[f"mth{int(month):02}"][int(day) - 1] == "1"




example = {
    "Timetable_no": "2024/2025",
    "baseOrderID1": "12491",
    "NrPoc1": "5424/5",
    "Nazwa1": "SKARBEK",
    "Od1": "Olsztyn Główny",
    "Do1": "Racibórz",
    "Dni1": "NULL",
    "baseOrderID2": "12449",
    "NrPoc2": "1322/3",
    "Nazwa2": "KINGA",
    "Od2": "WARSZAWA WSCHODNIA",
    "Do2": "Kraków Główny",
    "Dni2": "NULL",
    "objectID": "33605",
    "StacjaPrzełączenia": "Warszawa Centralna",
    "mth00": "0000000000000000000000000000000",
    "mth01": "0000000000000000000000000000000",
    "mth02": "0000000000000000000000000000000",
    "mth03": "0000000010000011000001100000110",
    "mth04": "0000110000011000011000000110010",
    "mth05": "1111000001100000110000011000001",
    "mth06": "1000001100000110001011000001100",
    "mth07": "0000110000011000001100000110000",
    "mth08": "0110000011000011100000110000011",
    "mth09": "0000011000001100000110000011000",
    "mth10": "0001100000110000011000001000000",
    "mth11": "0000000000000000000000000000000",
    "mth12": "0000000000000000000000000000000",
    "UserName": "",
    "updateDateTime": "2025-08-07 10:56:49.503",
    "noCarriages": "0",
    "carriageNo": "11,12,13,14,15,16,17,18",
    "ID_przelaczenia": "35889",
}
