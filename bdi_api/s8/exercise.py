import sqlite3

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from bdi_api.settings import Settings

settings = Settings()

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)

DB_PATH = "/tmp/s8_aircraft.db"

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

class AircraftReturn(BaseModel):
    icao: str
    registration: str | None
    type: str | None
    owner: str | None
    manufacturer: str | None
    model: str | None


class AircraftCO2Return(BaseModel):
    icao: str
    hours_flown: float
    co2: float | None


@s8.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[AircraftReturn]:
    """List all aircraft with enriched data, ordered by ICAO ascending.

    The data should come from the silver layer (processed by the Airflow DAG).
    Paginated with `num_results` per page and `page` number (0-indexed).
    """
    # TODO: Read enriched aircraft data from your storage (S3 silver, database, or local)
    # TODO: Order by ICAO ascending
    # TODO: Apply pagination using num_results and page
    conn = get_conn()
    try:
        offset = page * num_results
        rows = conn.execute(
            """
            SELECT icao, registration, type, owner, manufacturer, model
            FROM aircraft
            ORDER BY icao ASC
            LIMIT ? OFFSET ?
            """,
            (num_results, offset),
        ).fetchall()
        return [AircraftReturn(**dict(row)) for row in rows]
    finally:
        conn.close()


@s8.get("/aircraft/{icao}/co2")
def get_aircraft_co2(icao: str, day: str) -> AircraftCO2Return:
    """Calculate CO2 emissions for a given aircraft on a specific day.

    Computation:
    - Each row in the tracking data represents a 5-second observation
    - hours_flown = (number_of_observations * 5) / 3600
    - Look up `galph` (gallons per hour) from fuel consumption rates using the aircraft's ICAO type
    - fuel_used_kg = hours_flown * galph * 3.04
    - co2_tons = (fuel_used_kg * 3.15) / 907.185
    - If fuel consumption rate is not available for this aircraft type, return None for co2
    """
    # TODO: Count observations for this ICAO on the given day
    # TODO: Calculate hours_flown
    # TODO: Look up fuel consumption rate by aircraft type
    # TODO: Calculate CO2 emissions

    conn = get_conn()
    try:
        # Count observations for this aircraft on the given day
        row = conn.execute(
            """
            SELECT COUNT(*) as obs_count, MAX(type) as aircraft_type, MAX(galph) as galph
            FROM aircraft_positions
            WHERE icao = ? AND day = ?
            """,
            (icao.lower(), day),
        ).fetchone()

        if not row or row["obs_count"] == 0:
            return AircraftCO2Return(icao=icao.lower(), hours_flown=0.0, co2=None)

        obs_count = row["obs_count"]
        galph = row["galph"]

        # Each observation = 5 seconds
        hours_flown = (obs_count * 5) / 3600

        # Calculate CO2 if fuel rate is available
        co2 = None
        if galph is not None:
            fuel_used_kg = hours_flown * galph * 3.04
            co2 = (fuel_used_kg * 3.15) / 907.185

        return AircraftCO2Return(icao=icao.lower(), hours_flown=hours_flown, co2=co2)
    finally:
        conn.close()