"""
S8 Homework: Aircraft Data Pipeline DAG
Downloads ADS-B aircraft tracking data, enriches with metadata,
stores in bronze/silver layers, and loads into SQLite.
"""
import json
import os
import sqlite3
from datetime import datetime

import boto3
import pandas as pd
import requests
import s3fs
from airflow import DAG
from airflow.decorators import task

BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL")
DB_PATH = "/tmp/s8_aircraft.db"

# Download just a few hours to keep it manageable
ADSB_BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"
HOURS = ["000000Z", "010000Z", "020000Z", "030000Z", "040000Z"]

FUEL_RATES_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"
AIRCRAFT_DB_URL = "https://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz"


def get_s3():
    return boto3.client("s3", endpoint_url=S3_ENDPOINT)


def get_fs():
    return s3fs.S3FileSystem(
        endpoint_url=S3_ENDPOINT,
        key=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        secret=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )


with DAG(
    dag_id="s8_aircraft_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1},
) as dag:

    @task()
    def download_to_bronze(ds=None):
        """Download ADS-B JSON files and store in S3 bronze layer."""
        s3 = get_s3()
        written_keys = []

        for hour in HOURS:
            url = f"{ADSB_BASE_URL}{hour}.json.gz"
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
            except requests.RequestException as e:
                print(f"Warning: could not download {url}: {e}")
                continue

            s3_key = f"aircraft/_created_date={ds}/{hour}.json"
            s3.put_object(
                Bucket=BRONZE_BUCKET,
                Key=s3_key,
                Body=response.content,
            )
            written_keys.append(s3_key)
            print(f"Uploaded {s3_key} ({len(response.content)} bytes)")

        if not written_keys:
            raise RuntimeError("No ADS-B files could be downloaded")

        return written_keys

    @task()
    def bronze_to_silver(bronze_keys: list, ds=None):
        """Parse aircraft records and store as Parquet in silver layer."""
        fs = get_fs()
        all_records = []

        for s3_key in bronze_keys:
            with fs.open(f"{BRONZE_BUCKET}/{s3_key}") as f:
                data = json.load(f)

            now_ts = data.get("now")
            for ac in data.get("aircraft", []):
                # Skip aircraft without position
                if ac.get("lat") is None or ac.get("lon") is None:
                    continue
                all_records.append({
                    "icao": ac.get("hex", "").lower().strip(),
                    "registration": ac.get("r"),
                    "type": ac.get("t"),
                    "lat": ac.get("lat"),
                    "lon": ac.get("lon"),
                    "alt_baro": ac.get("alt_baro"),
                    "ground_speed": ac.get("gs"),
                    "timestamp": now_ts,
                    "day": ds,
                })

        if not all_records:
            raise RuntimeError("No aircraft records with positions found")

        df = pd.DataFrame(all_records)
        df = df.drop_duplicates(subset=["icao", "timestamp"])

        # alt_baro can be "ground" string - convert to numeric, set non-numeric to None
        df["alt_baro"] = pd.to_numeric(df["alt_baro"], errors="coerce")
        df["ground_speed"] = pd.to_numeric(df["ground_speed"], errors="coerce")

        silver_key = f"{SILVER_BUCKET}/aircraft/_created_date={ds}/data.snappy.parquet"
        with fs.open(silver_key, "wb") as f:
            df.to_parquet(f, compression="snappy", index=False)

        print(f"Wrote {len(df)} records to silver")
        return silver_key

    @task()
    def enrich_and_load(silver_key: str, ds=None):
        """Enrich with aircraft metadata + fuel rates, load into SQLite."""
        fs = get_fs()

        # Read silver parquet
        with fs.open(silver_key) as f:
            df = pd.read_parquet(f)

        # Download aircraft database (newline-delimited JSON)
        print("Downloading aircraft database...")
        resp = requests.get(AIRCRAFT_DB_URL, timeout=60)
        resp.raise_for_status()

        import gzip
        lines = gzip.decompress(resp.content).decode("utf-8").strip().split("\n")
        ac_records = [json.loads(line) for line in lines if line.strip()]
        ac_df = pd.DataFrame(ac_records)[["icao", "reg", "icaotype", "manufacturer", "model", "ownop"]]
        ac_df.columns = ["icao", "registration_db", "type_db", "manufacturer", "model", "owner"]
        ac_df["icao"] = ac_df["icao"].str.lower().str.strip()

        # Merge enrichment into positions
        df = df.merge(ac_df, on="icao", how="left")

        # Use DB values where tracking data is missing
        df["registration"] = df["registration"].fillna(df["registration_db"])
        df["type"] = df["type"].fillna(df["type_db"])
        df = df.drop(columns=["registration_db", "type_db"])

        # Download fuel rates
        print("Downloading fuel rates...")
        fuel_resp = requests.get(FUEL_RATES_URL, timeout=15)
        fuel_resp.raise_for_status()
        fuel_rates = fuel_resp.json()
        df["galph"] = df["type"].map(lambda t: fuel_rates.get(t, {}).get("galph") if t else None)

        # Load into SQLite
        conn = sqlite3.connect(DB_PATH)

        # positions table (for CO2 calculations)
        df.to_sql("aircraft_positions", conn, if_exists="replace", index=False)

        # unique aircraft table - pick the row with most data per icao
        aircraft_df = (
            df.sort_values(["owner", "manufacturer", "model"], na_position="last")
            .drop_duplicates(subset=["icao"], keep="first")
            [["icao", "registration", "type", "owner", "manufacturer", "model"]]
            .sort_values("icao")
        )
        aircraft_df.to_sql("aircraft", conn, if_exists="replace", index=False)

        conn.close()
        print(f"Loaded {len(df)} positions, {len(aircraft_df)} unique aircraft into {DB_PATH}")
        return DB_PATH

    # Wire up the pipeline
    bronze_keys = download_to_bronze()
    silver_key = bronze_to_silver(bronze_keys)
    enrich_and_load(silver_key)
