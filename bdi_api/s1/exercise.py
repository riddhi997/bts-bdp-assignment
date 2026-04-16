import os
import shutil
from typing import Annotated

import duckdb
import requests
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)


@s1.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Downloads the `file_limit` files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to add it to `requirements.txt`
    so it can be installed using `pip install -r requirements.txt`.


    TIP: always clean the download folder before writing again to avoid having old files.
    """
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"
    # TODO Implement download

    # Clean the download folder
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)
    os.makedirs(download_dir, exist_ok=True)

    # Download files (increment by 5: 000000Z, 000005Z, 000010Z...)
    for i in range(file_limit):
        file_num = i * 5
        filename = f"{file_num:06d}Z.json.gz"
        url = base_url + filename
        filepath = os.path.join(download_dir, filename)

        try:
            response = requests.get(url)
            if response.status_code == 200:
                with open(filepath, 'wb') as f:
                    f.write(response.content)
            else:
                print(f"Failed: {filename} - Status {response.status_code}")
        except Exception as e:
            print(f"Error downloading {filename}: {e}")

    return "OK"


@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """Prepare the data in the way you think it's better for the analysis.

    * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    * documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to add it to `requirements.txt`
    so it can be installed using `pip install -r requirements.txt`.

    TIP: always clean the prepared folder before writing again to avoid having old files.

    Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
    """
    # TODO

    # Set up paths
    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    db_path = os.path.join(settings.prepared_dir, "aircraft.db")

    # Clean prepared folder
    if os.path.exists(settings.prepared_dir):
        shutil.rmtree(settings.prepared_dir)
    os.makedirs(settings.prepared_dir, exist_ok=True)

    # Create DuckDB database
    conn = duckdb.connect(str(db_path))

    # STEP 1: Rename all .json.gz files to .json
    print(f"Renaming files in {raw_dir}")
    for filename in os.listdir(raw_dir):
        if filename.endswith('.json.gz'):
            old_path = os.path.join(raw_dir, filename)
            new_filename = filename.replace('.json.gz', '.json')
            new_path = os.path.join(raw_dir, new_filename)
            os.rename(old_path, new_path)
            print(f"  Renamed: {filename} -> {new_filename}")

    # STEP 2: Create a table with raw data and unnested aircraft
    conn.execute(f"""
        CREATE OR REPLACE TABLE positions AS
        WITH unnested AS (
                 SELECT
                 raw_data.now as timestamp, unnested.value as aircraft_obj
                 FROM read_json_auto('{raw_dir}/*.json') AS raw_data,
                 UNNEST(raw_data.aircraft) AS unnested(value)
                 )

        SELECT
            timestamp,
            aircraft_obj ->> 'hex' as icao,
            aircraft_obj ->> 'r' as registration,
            aircraft_obj ->> 't' as type,
            CASE
                WHEN (aircraft_obj ->> 'lat') ~ '^[0-9\\.-]+$'
                THEN (aircraft_obj ->> 'lat')::DOUBLE
                ELSE NULL
            END as lat,
            CASE
                WHEN (aircraft_obj ->> 'lon') ~ '^[0-9\\.-]+$'
                THEN (aircraft_obj ->> 'lon')::DOUBLE
                ELSE NULL
            END as lon,
            CASE
                WHEN (aircraft_obj ->> 'alt_baro') ~ '^[0-9\\.-]+$'
                THEN (aircraft_obj ->> 'alt_baro')::DOUBLE
                ELSE NULL
            END as altitude,
            CASE
                WHEN (aircraft_obj ->> 'gs') ~ '^[0-9\\.-]+$'
                THEN (aircraft_obj ->> 'gs')::DOUBLE
                ELSE NULL
            END as ground_speed,
            CASE
                WHEN (aircraft_obj ->> 'track') ~ '^[0-9\\.-]+$'
                THEN (aircraft_obj ->> 'track')::DOUBLE
                ELSE NULL
            END as track,
            CASE
                WHEN (aircraft_obj ->> 'baro_rate') ~ '^[0-9\\.-]+$'
                THEN (aircraft_obj ->> 'baro_rate')::DOUBLE
                ELSE NULL
            END as baro_rate,
            CASE
                WHEN (aircraft_obj ->> 'seen_pos') ~ '^[0-9\\.-]+$'
                THEN (aircraft_obj ->> 'seen_pos')::DOUBLE
                ELSE NULL
            END as seen_pos
        FROM unnested
        WHERE aircraft_obj ->> 'hex' IS NOT NULL
    """)


    conn.close()

    return "OK"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    # TODO

    # Connect to the database
    conn = duckdb.connect(str(os.path.join(settings.prepared_dir, "aircraft.db")))

    # Get unique aircraft with pagination
    offset = page * num_results
    result = conn.execute("""
        SELECT DISTINCT icao, registration, type
        FROM positions
        WHERE icao IS NOT NULL
        ORDER BY icao ASC
        LIMIT ? OFFSET ?
    """, [num_results, offset]).fetchall()

    conn.close()

    # Convert to list of dictionaries
    return [{"icao": row[0], "registration": row[1], "type": row[2]} for row in result]


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    # TODO implement and return a list with dictionaries with those values.

    # Connect to the database
    conn = duckdb.connect(str(os.path.join(settings.prepared_dir, "aircraft.db")))

    # Get positions with pagination
    offset = page * num_results
    result = conn.execute("""
        SELECT timestamp, lat, lon, altitude, ground_speed, track
        FROM positions
        WHERE icao = ?
        ORDER BY timestamp ASC
        LIMIT ? OFFSET ?
    """, [icao, num_results, offset]).fetchall()

    conn.close()

    # Convert to list of dictionaries
    return [
        {
            "timestamp": row[0],
            "lat": row[1],
            "lon": row[2],
            "altitude": row[3],
            "ground_speed": row[4],
            "track": row[5]
        }
        for row in result
    ]

@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    # TODO Gather and return the correct statistics for the requested aircraft

    # Connect to the database
    conn = duckdb.connect(str(os.path.join(settings.prepared_dir, "aircraft.db")))

    # Get statistics
    result = conn.execute("""
        SELECT
            MAX(altitude) as max_altitude,
            MAX(ground_speed) as max_speed
        FROM positions
        WHERE icao = ?
    """, [icao]).fetchone()

    conn.close()

    # Handle case where aircraft not found
    if result[0] is None and result[1] is None:
        return {"max_altitude_baro": None, "max_ground_speed": None, "had_emergency": False}

    return {
        "max_altitude_baro": result[0] if result[0] is not None else 0,
        "max_ground_speed": result[1] if result[1] is not None else 0,
        "had_emergency": False  # We don't have emergency data in these files
    }
