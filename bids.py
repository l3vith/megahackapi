from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from google import genai
from astrapy import DataAPIClient
from collections import defaultdict
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
from fastapi import HTTPException



app = FastAPI()
load_dotenv()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can specify your allowed origins here
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


from astrapy import DataAPIClient

# Initialize the client
client = DataAPIClient(os.getenv("ASTRA_KEY"))
db = client.get_database_by_api_endpoint(
    "https://382c2832-618d-4ee2-a0f4-808c99d1ad09-us-east-2.apps.astra.datastax.com"
)

print(f"Connected to Astra DB: {db.list_collection_names()}")
collection = db.get_collection("listings")

from collections import defaultdict

@app.get('/api/bids')
async def display_bids():
    try:
        cursor = collection.find({})

        bid_data = defaultdict(lambda: {
            "commodity": "",
            "quantity": 0,
            "price": 0,
            "state": "",
            "district": "",
            "bids": [],
            "highest_bid_user": ""
        })

        for entry in cursor:
            vendor_id = entry["vendor_id"]
            bid_data[vendor_id]["commodity"] = entry["commodity"]
            bid_data[vendor_id]["quantity"] = entry["quantity"]
            bid_data[vendor_id]["price"] = entry["price"]
            bid_data[vendor_id]["state"] = entry["state"]
            bid_data[vendor_id]["district"] = entry["district"]
            bid_data[vendor_id]["bids"] = entry.get("bid_history", [])
            bid_data[vendor_id]["highest_bid_user"] = entry.get("highest_bid_user", "")

        return [{"vendor_id": vendor, **data} for vendor, data in bid_data.items()]

    except Exception as e:
        return {"error": "Failed to fetch bids", "details": str(e)}


from fastapi import HTTPException
from pydantic import BaseModel
from datetime import datetime

# Define request schema
class BidRequest(BaseModel):
    commodity: str
    quantity: int
    price: float
    state: str
    district: str
    start_time: datetime
    end_time: datetime
    bid_history: list[int] = []
    highest_bid_user: str

@app.put("/api/bids/{vendor_id}")
async def add_bid(vendor_id: str, bid: BidRequest):
    try:
        # Check if vendor_id exists
        existing_entry = collection.find_one({"vendor_id": vendor_id})
        
        if existing_entry:
            raise HTTPException(status_code=400, detail="Vendor already exists. Use PATCH to update bids.")

        # Insert new auction entry
        new_bid = {
            "vendor_id": vendor_id,
            "commodity": bid.commodity,
            "quantity": bid.quantity,
            "price": bid.price,
            "state": bid.state,
            "district": bid.district,
            "start_time": bid.start_time.isoformat(),
            "end_time": bid.end_time.isoformat(),
            "bid_history": bid.bid_history,
            "highest_bid_user": bid.highest_bid_user
        }
        collection.insert_one(new_bid)

        return {"message": "Bid added successfully", "data": new_bid}

    except Exception as e:
        return {"error": "Failed to add bid", "details": str(e)}


from typing import Optional

class UpdateBidRequest(BaseModel):
    commodity: Optional[str] = None
    quantity: Optional[int] = None
    price: Optional[float] = None
    state: Optional[str] = None
    district: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    bid_history: Optional[list[int]] = None
    highest_bid_user: Optional[str] = None

@app.patch("/api/bids/{vendor_id}")
async def update_bid(vendor_id: int, bid: UpdateBidRequest):
    try:
        print(f"Searching for vendor_id: {vendor_id}")  # Debugging

        # Check if the vendor exists (force vendor_id to int)
        existing_entry = collection.find_one({"vendor_id": vendor_id})
        if not existing_entry:
            raise HTTPException(status_code=404, detail=f"Bid not found for vendor_id {vendor_id}")

        update_data = {k: v for k, v in bid.dict().items() if v is not None}

        # Ensure datetime fields are stored as ISO format
        if "start_time" in update_data:
            update_data["start_time"] = update_data["start_time"].isoformat()
        if "end_time" in update_data:
            update_data["end_time"] = update_data["end_time"].isoformat()

        # Debugging: Print update payload
        print(f"Updating {vendor_id} with data: {update_data}")

        # Perform update
        result = collection.update_one({"vendor_id": vendor_id}, {"$set": update_data})

        return {"message": "Bid updated successfully", "updated_fields": update_data}

    except Exception as e:
        return {"error": "Failed to update bid", "details": str(e)}


    
@app.delete("/api/bids/{vendor_id}")
async def delete_bid(vendor_id: int):
    try:
        # Check if the bid exists
        existing_entry = collection.find_one({"vendor_id": vendor_id})

        if not existing_entry:
            raise HTTPException(status_code=404, detail="Bid not found")

        # Delete the bid from the database
        collection.delete_one({"vendor_id": vendor_id})

        return {"message": "Bid deleted successfully", "vendor_id": vendor_id}

    except Exception as e:
        return {"error": "Failed to delete bid", "details": str(e)}
