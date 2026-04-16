import os
import shutil
from typing import Annotated

import boto3
import requests
from fastapi import APIRouter, Query, status

from bdi_api.settings import Settings

settings = Settings()
s3_client = boto3.client('s3')
bucket_name = settings.s3_bucket

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="Limits the number of files to download. Start from first file.",
        ),
    ] = 100,
) -> str:
    """Downloads files and stores them in S3 bucket under raw/day=20231101/"""

    base_url = settings.source_url + "/2023/11/01/"
    s3_prefix = "raw/day=20231101/"

    # STEP 1: Clean the S3 folder (delete all existing files)
    print(f"Cleaning S3 folder: s3://{bucket_name}/{s3_prefix}")

    # List all objects in the folder
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=s3_prefix
        )

        # Delete each object if any exist
        if "Contents" in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response["Contents"]]
            s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': objects_to_delete}
            )
            print(f"Deleted {len(objects_to_delete)} existing files")
        else:
            print("Folder is empty, nothing to delete")
    except Exception as e:
        print(f"Error cleaning folder: {e}")

    # STEP 2: Download new files
    downloaded = 0
    for i in range(file_limit):
        file_num = i * 5
        filename = f"{file_num:06d}Z.json.gz"
        url = base_url + filename
        s3_key = s3_prefix + filename

        try:
            response = requests.get(url)
            if response.status_code == 200:
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=response.content,
                    ContentType="application/json"
                )
                downloaded += 1
                print(f"Uploaded: {s3_key}")
            else:
                print(f"Failed: {filename} - Status {response.status_code}")
        except Exception as e:
            print(f"Error downloading {filename}: {e}")

    return f"Downloaded {downloaded} files to s3://{bucket_name}/{s3_prefix}"


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Read raw files from S3 and prepare data using DuckDB"""

    import duckdb

    s3_prefix = "raw/day=20231101/"
    s3_path = f"s3://{bucket_name}/{s3_prefix}"
    db_path = os.path.join(settings.prepared_dir, "aircraft_s4.db")

    # Clean local prepared folder
    if os.path.exists(settings.prepared_dir):
        shutil.rmtree(settings.prepared_dir)
    os.makedirs(settings.prepared_dir, exist_ok=True)

    # STEP 1: Copy and rename files in S3 (remove .gz extension)
    print("Renaming files in S3 (removing .gz extension)...")

    # List all .json.gz files in the S3 folder
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=s3_prefix
    )

    if "Contents" not in response:
        return "No files found in S3 to prepare"

    renamed_count = 0
    for obj in response["Contents"]:
        old_key = obj["Key"]
        if old_key.endswith('.json.gz'):
            new_key = old_key.replace('.json.gz', '.json')

            # Copy the object to new key (without .gz)
            s3_client.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': old_key},
                Key=new_key
            )
            # Optionally delete the old one (uncomment if you want to clean up)
            s3_client.delete_object(Bucket=bucket_name, Key=old_key)
            renamed_count += 1
            print(f"  Renamed: {old_key} -> {new_key}")

    print(f"Renamed {renamed_count} files")

    # STEP 2: Create DuckDB database and configure S3 access
    conn = duckdb.connect(str(db_path))

    # Install and load httpfs extension for S3 support
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")

    # Use credential_chain to automatically pick up AWS credentials
    conn.execute("""
        CREATE OR REPLACE SECRET s3_secret (
            TYPE S3,
            PROVIDER credential_chain
        )
    """)
    print("Created DuckDB secret with credential_chain")

    # STEP 3: Read from S3 and process
    print("Reading from S3 and processing...")

    try:
        conn.execute(f"""
            CREATE OR REPLACE TABLE positions AS
            WITH unnested AS (
                SELECT
                    raw_data.now as timestamp,
                    unnested.value as aircraft_obj
                FROM read_json_auto('{s3_path}*.json') AS raw_data,
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
                END as baro_rate
            FROM unnested
            WHERE aircraft_obj ->> 'hex' IS NOT NULL
        """)

        # Get row count
        row_count = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
        print(f"Created positions table with {row_count} rows")

    except Exception as e:
        print(f"Error during processing: {e}")
        conn.close()
        return f"Error: Processing failed - {e}"

    conn.close()

    return f"Prepared data from S3 into {db_path}. Processed {row_count} rows."
