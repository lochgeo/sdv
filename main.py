import json
from fastapi import FastAPI, Body
from sdv.single_table import GaussianCopulaSynthesizer, CTGANSynthesizer
from sdv.metadata import SingleTableMetadata
import pandas as pd
import sqlite3
import json
from pydantic import BaseModel
from routers import metadata, metadata_elements

from routers.metadata import get_metadata

tags = [
    {
        "name": "metadata",
        "description": "metadata",
    },
    {
        "name": "metadata elements",
        "description": "Individual metadata elements",
    },
    {
        "name": "main",
        "description": "The main api endpoints",
    },
]

class ModelParams(BaseModel):
    metadata_id: int
    name: str
    type: str

app = FastAPI(openapi_tags=tags, title="SDV", description="SDV API")
app.include_router(metadata.router)
app.include_router(metadata_elements.router)

# Initialize empty dictionaries to store models
models = {}
fitted_models = {}

# Connect to SQLite database
conn = sqlite3.connect('metadata.db')
c = conn.cursor()

# Create a table to store metadata
c.execute('''CREATE TABLE IF NOT EXISTS metadata
             (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, primary_key TEXT, alternate_keys TEXT)''')

# Create a table to store metadata elements
c.execute('''CREATE TABLE IF NOT EXISTS metadata_elements
             (id INTEGER PRIMARY KEY AUTOINCREMENT, metadata_id INTEGER, element_name TEXT, element_value TEXT, FOREIGN KEY (metadata_id) REFERENCES metadata(id))''')

@app.post("/set_metadata", tags=["main"])
async def set_metadata(model_params: ModelParams):
    """ Set the metadata for creating a model. """
    metadata_id = model_params.metadata_id
    model_name = model_params.name
    model_type = model_params.type

    c.execute("SELECT id FROM metadata WHERE id = ?", (metadata_id,))
    result = c.fetchone()
    if not result:
        return {"error": "Metadata not found. Please create the metadata first before setting the model."}

    metadata_dict = await get_metadata(metadata_id)
    metadata = SingleTableMetadata.load_from_dict(metadata_dict)

    if model_type == "gaussian_copula":
        models[model_name] = GaussianCopulaSynthesizer(metadata)
    elif model_type == "ctgan":
        models[model_name] = CTGANSynthesizer(metadata)
    else:
        return {"error": "Unsupported model type. Please choose 'gaussian_copula' or 'ctgan'."}

    return {"message": f"Metadata set successfully for the {model_name} model."}

@app.post("/fit_model/{model_name}", tags=["main"])
async def fit_model(model_name: str, data: list = Body(...)):
    """ Fit the model with the provided data. """
    if model_name not in models:
        return {"error": f"Model '{model_name}' not found."}
    model = models[model_name]
    data_df = pd.DataFrame(data)
    fitted_models[model_name] = model.fit(data_df)
    return {"message": f"Model '{model_name}' fitted successfully."}

@app.post("/generate_data/{model_name}", tags=["main"])
async def generate_data(model_name: str, num_rows: int = 100):
    """ Generate synthetic data using the fitted model. """
    if model_name not in fitted_models:
        return {"error": f"Model '{model_name}' has not been fitted yet"}
    fitted_model = fitted_models[model_name]
    synthetic_data = fitted_model.sample(num_rows)
    return synthetic_data.to_dict(orient='records')