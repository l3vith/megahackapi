from fastapi import FastAPI
import os
import json
from dotenv import load_dotenv
from google import genai

# import google.generativeai as genai


app = FastAPI()
load_dotenv()

clientgemini = genai.Client(api_key=os.getenv("GEMINI_KEY"))

from astrapy import DataAPIClient

# Initialize the client
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
        projection={"Market": True, "Commodity": True, "Modal_Price": True},
        limit=350,
    )

    market_data = defaultdict(lambda: {"commodities": [], "prices": []})

    for entry in cursor:
        market = entry["Market"]
        market_data[market]["commodities"].append(entry["Commodity"])
        market_data[market]["prices"].append(entry["Modal_Price"])

    return [{"Market": market, **data} for market, data in market_data.items()]

from collections import defaultdict

@app.get("/{state}/{district}")
async def getPrices(state: str, district: str):
    cursor = collection.find(
        {"State": state, "Market": district},
        sort={"$vectorize": district},
        projection={"Market": True, "Commodity": True, "Modal_Price": True}
    )

    market_data = defaultdict(lambda: {"commodities": [], "prices": []})

    for entry in cursor:
        market = entry["Market"]
        market_data[market]["commodities"].append(entry["Commodity"])
        market_data[market]["prices"].append(entry["Modal_Price"])

    return [{"Market": market, **data} for market, data in market_data.items()]

@app.get('/chat/{state}/{question}')
async def chatbot(state:str, question: str):
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

    info =  [{"Market": market, **data} for market, data in market_data.items()]

    response = clientgemini.models.generate_content(
    model="gemini-2.0-flash", contents=f"Be conscise and clear and answer the question{question} asked Data: {info} this is the data you need to refer to about farmer markets in the area {state}"
    )

    return response