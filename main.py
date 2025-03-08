from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import json
from dotenv import load_dotenv
from google import genai
from astrapy import DataAPIClient
from collections import defaultdict

app = FastAPI()

# Apply CORS middleware to the FastAPI app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can specify your allowed origins here
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

load_dotenv()
clientgemini = genai.Client(api_key=os.getenv("GEMINI_KEY"))

# Initialize Astra DB client
client = DataAPIClient(os.getenv("ASTRA_KEY"))
db = client.get_database_by_api_endpoint(
    "https://382c2832-618d-4ee2-a0f4-808c99d1ad09-us-east-2.apps.astra.datastax.com"
)

print(f"Connected to Astra DB: {db.list_collection_names()}")
collection = db.get_collection("markets")

@app.get("/{state}")
async def getPrices(state: str):
    cursor = collection.find(
        {"State": state},
        projection={"Market": True, "Commodity": True, "Modal_Price": True, "lat": True, "lng": True},
        limit=350,
    )

    market_data = defaultdict(lambda: {"commodities": [], "prices": [], "lat": [], "lng": []})

    for entry in cursor:
        market = entry["Market"]
        market_data[market]["commodities"].append(entry["Commodity"])
        market_data[market]["prices"].append(entry["Modal_Price"])

        # Append latitude and longitude instead of overwriting
        if "lat" in entry:
            if len(market_data[market]["lat"]) == 0:
                market_data[market]["lat"].append(entry["lat"])
        if "lng" in entry:
            if len(market_data[market]["lng"]) == 0:
                market_data[market]["lng"].append(entry["lng"])

    return [{"Market": market, **data} for market, data in market_data.items()]

@app.get("/{state}/{district}")
async def getPrices(state: str, district: str):
    cursor = collection.find(
        {"State": state, "Market": district},
        sort={"$vectorize": district},
        projection={"Market": True, "Commodity": True, "Modal_Price": True, "lat": True, "lng": True},
    )

    market_data = defaultdict(lambda: {"commodities": [], "prices": [], "lat": [], "lng": []})

    for entry in cursor:
        market = entry["Market"]
        market_data[market]["commodities"].append(entry["Commodity"])
        market_data[market]["prices"].append(entry["Modal_Price"])

        # Append lat/lng as lists
        if "lat" in entry:
            if len(market_data[market]["lat"]) == 0:
                market_data[market]["lat"].append(entry["lat"])
        if "lng" in entry:
            if len(market_data[market]["lng"]) == 0:
                market_data[market]["lng"].append(entry["lng"])

    return [{"Market": market, **data} for market, data in market_data.items()]

@app.get('/chat/{state}/{question}')
async def chatbot(state: str, question: str):
    cursor = collection.find(
        {"State": state},
        projection={"Market": True, "Commodity": True, "Modal_Price": True},
        limit=250,
    )

    market_data = defaultdict(lambda: {"commodities": [], "prices": []})

    for entry in cursor:
        market = entry["Market"]
        market_data[market]["commodities"].append(entry["Commodity"])
        market_data[market]["prices"].append(entry["Modal_Price"])

    info = [{"Market": market, **data} for market, data in market_data.items()]

    response = clientgemini.models.generate_content(
        model="gemini-2.0-flash",
        contents=f"Be concise and clear and answer the question {question}. Data: {info} - This is the data about farmer markets in {state}."
    )

    return response
