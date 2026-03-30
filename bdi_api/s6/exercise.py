from typing import Annotated

import pymongo
from fastapi import APIRouter, HTTPException, status
from fastapi.params import Query
from pydantic import BaseModel

from bdi_api.settings import Settings

settings = Settings()

s6 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s6",
    tags=["s6"],
)

def get_collection():
    """Connect to MongoDB and return the positions collection."""
    client = pymongo.MongoClient("mongodb://admin:admin123@localhost:27017")
    db = client["bdi_aircraft"]
    return db["positions"]

class AircraftPosition(BaseModel):
    icao: str
    registration: str | None = None
    type: str | None = None
    lat: float
    lon: float
    alt_baro: float | None = None
    ground_speed: float | None = None
    timestamp: str


@s6.post("/aircraft")
def create_aircraft(position: AircraftPosition) -> dict:
    """Store an aircraft position document in MongoDB.

    Use the BDI_MONGO_URL environment variable to configure the connection.
    Start MongoDB with: make mongo
    Database name: bdi_aircraft
    Collection name: positions
    """
    # TODO: Connect to MongoDB using pymongo.MongoClient(settings.mongo_url)
    # TODO: Insert the position document into the 'positions' collection
    # TODO: Return {"status": "ok"}

    collection=get_collection()
    collection.insert_one(position.model_dump())
    return {"status": "ok"}


@s6.get("/aircraft/stats")
def aircraft_stats() -> list[dict]:
    """Return aggregated statistics: count of positions grouped by aircraft type.

    Response example: [{"type": "B738", "count": 42}, {"type": "A320", "count": 38}]

    Use MongoDB's aggregation pipeline with $group.
    """
    # TODO: Connect to MongoDB
    # TODO: Use collection.aggregate() with $group on 'type' field
    # TODO: Return list sorted by count descending
    collection = get_collection()
    pipeline = [
        {"$group": {"_id": "$type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    results = collection.aggregate(pipeline)
    return [{"type": r["_id"], "count": r["count"]} for r in results]


@s6.get("/aircraft/")
def list_aircraft(
    page: Annotated[
        int,
        Query(description="Page number (1-indexed)", ge=1),
    ] = 1,
    page_size: Annotated[
        int,
        Query(description="Number of results per page", ge=1, le=100),
    ] = 20,
) -> list[dict]:
    """List all aircraft with pagination.

    Each result should include: icao, registration, type.
    Use MongoDB's skip() and limit() for pagination.
    """
    # TODO: Connect to MongoDB
    # TODO: Query distinct aircraft, apply skip/limit for pagination
    # TODO: Return list of dicts with icao, registration, type
    collection = get_collection()
    skip = (page - 1) * page_size
    # Get distinct aircraft by icao, returning only icao, registration, type
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {"$group": {
            "_id": "$icao",
            "icao": {"$first": "$icao"},
            "registration": {"$first": "$registration"},
            "type": {"$first": "$type"},
        }},
        {"$skip": skip},
        {"$limit": page_size},
        {"$project": {"_id": 0, "icao": 1, "registration": 1, "type": 1}},
    ]
    results = collection.aggregate(pipeline)
    return list(results)


@s6.get("/aircraft/{icao}")
def get_aircraft(icao: str) -> dict:
    """Get the latest position data for a specific aircraft.

    Return the most recent document matching the given ICAO code.
    If not found, return 404.
    """
    # TODO: Connect to MongoDB
    # TODO: Find the latest document for this icao (sort by timestamp descending)
    # TODO: Return 404 if not found
    collection = get_collection()
    result = collection.find_one(
        {"icao": icao},
        sort=[("timestamp", pymongo.DESCENDING)],
        projection={"_id": 0},
    )
    if result is None:
        raise HTTPException(status_code=404, detail=f"Aircraft '{icao}' not found")
    return result


@s6.delete("/aircraft/{icao}")
def delete_aircraft(icao: str) -> dict:
    """Remove all position records for an aircraft.

    Returns the number of deleted documents.
    """
    # TODO: Connect to MongoDB
    # TODO: Delete all documents matching the icao
    # TODO: Return {"deleted": <count>}
    collection = get_collection()
    result = collection.delete_many({"icao": icao})
    return {"deleted": result.deleted_count}
