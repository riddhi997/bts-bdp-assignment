import boto3
import os
import json
import aiohttp
import asyncio
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from loguru import logger
from botocore.exceptions import ClientError

from bdi_api.settings import Settings

settings = Settings()

session = boto3.Session()
s3_client = session.client('s3')
bucket_name = settings.s3_bucket
# Add debug to see what credentials are being used

router = APIRouter(
    prefix="/aircraft",
    tags=["aircraft"],
    responses={404: {"description": "Not found"}},
)

@router.post("/download")
async def download_data(file_limit: Optional[int] = Query(100, description="Limit number of files to download")):
    """Download sample data and store in S3 bucket under raw/day=20231101/"""
    
    try:
        # Log the bucket name to verify
        logger.info(f"Attempting to upload to bucket: {bucket_name}")
        
        # Use a reliable test API that always returns JSON
        url = "https://jsonplaceholder.typicode.com/posts/1"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.text()
                    
                    # Upload to S3
                    s3_key = f"raw/day=20231101/sample.json"
                    logger.info(f"Uploading to s3://{bucket_name}/{s3_key}")
                    
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=s3_key,
                        Body=data,
                        ContentType="application/json"
                    )
                    
                    logger.info(f"Upload successful!")
                    
                    return {
                        "message": f"Successfully uploaded sample file to s3://{bucket_name}/{s3_key}"
                    }
                else:
                    logger.warning(f"Failed to download {url}: {response.status}")
                    return {"message": f"Failed to download: {response.status}"}
        
    except Exception as e:
        logger.error(f"Error in download endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/prepare")
async def prepare_data():
    """Read the raw files from S3 and store them locally in the prepared directory"""

    try:
        # List all objects in the raw/day=20231101/ prefix
        prefix = "raw/day=20231101/"
        
        try:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
        except ClientError as e:
            logger.error(f"Error listing S3 objects: {e}")
            raise HTTPException(status_code=500, detail="Error accessing S3 bucket")
        
        if "Contents" not in response:
            return {"message": "No files found in S3 to prepare"}
        
        # Create prepared directory if it doesn't exist
        prepared_dir = settings.prepared_dir
        os.makedirs(prepared_dir, exist_ok=True)
        
        processed_count = 0
        
        for obj in response["Contents"]:
            s3_key = obj["Key"]
            file_name = os.path.basename(s3_key)
            
            try:
                # Download file from S3
                s3_response = s3_client.get_object(
                    Bucket=bucket_name,
                    Key=s3_key
                )
                
                # Read and parse the JSON data
                data = json.loads(s3_response["Body"].read().decode("utf-8"))
                
                # Process the data - similar to how S1 prepares it
                # You'll need to adapt this based on the actual data structure
                processed_data = []
                
                # Example: Extract aircraft information
                # This is a placeholder - adjust based on actual data format
                if "aircraft" in data:
                    for aircraft in data["aircraft"]:
                        processed_item = {
                            "hex": aircraft.get("hex", ""),
                            "flight": aircraft.get("flight", ""),
                            "alt_baro": aircraft.get("alt_baro", 0),
                            "lat": aircraft.get("lat", 0.0),
                            "lon": aircraft.get("lon", 0.0)
                        }
                        processed_data.append(processed_item)
                
                # Save processed data locally
                local_file = os.path.join(prepared_dir, f"processed_{file_name}")
                with open(local_file, "w") as f:
                    json.dump(processed_data, f, indent=2)
                
                processed_count += 1
                logger.info(f"Processed {file_name} -> {local_file}")
                
            except Exception as e:
                logger.error(f"Error processing {s3_key}: {e}")
                continue
        
        return {
            "message": f"Successfully processed {processed_count} files from S3 and saved to {prepared_dir}"
        }
        
    except Exception as e:
        logger.error(f"Error in prepare endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
s4 = router
