import sqlite3
import json
from typing import Literal
from fastapi import APIRouter, Body
from pydantic import BaseModel

router = APIRouter()

# Connect to SQLite database
conn = sqlite3.connect('metadata.db')
c = conn.cursor()

class MetaDataElementValue(BaseModel):
    sd_type: Literal["boolean", "categorical", "datetime", "numerical", "id", "phone_number", "email", "ssn", "first_name", "last_name" ]
    pii: bool
    regex_format: str
    datetime_format: str

class MetaDataElement(BaseModel):
    metadata_id: int
    element_name: str
    element_value: MetaDataElementValue

@router.post("/metadata/{metadata_id}/metadata-elements", tags=["metadata elements"])
async def create_metadata_element(element: MetaDataElement):
    """ Create a new metadata element. """
    c.execute("INSERT INTO metadata_elements (metadata_id, element_name, element_value) VALUES (?, ?, ?)", (element.metadata_id, element.element_name, json.dumps(element.element_value)))
    conn.commit()
    return {"message": "Metadata element created successfully"}

@router.put("/metadata/{metadata_id}/metadata-elements/{element_name}", tags=["metadata elements"])
async def update_metadata_element(metadata_id: int, element_name: str, element_value: MetaDataElementValue = Body(...)):
    """ Update a specific element of the metadata. """
    c.execute("SELECT id FROM metadata_elements WHERE metadata_id = ? AND element_name = ?", (metadata_id, element_name))
    result = c.fetchone()
    if result:
        element_id = result[0]
        c.execute("UPDATE metadata_elements SET element_value = ? WHERE id = ?", (json.dumps(element_value), element_id))
    else:
        c.execute("INSERT INTO metadata_elements (metadata_id, element_name, element_value) VALUES (?, ?, ?)", (metadata_id, element_name, json.dumps(element_value)))
    conn.commit()
    return {"message": f"Metadata element '{element_name}' updated successfully"}

@router.get("/metadata/{metadata_id}/metadata-elements/{element_name}", tags=["metadata elements"])
async def get_metadata_element(metadata_id: int, element_name: str):
    """ Get a metadata element by metadata id and element name. """
    c.execute("SELECT element_value FROM metadata_elements WHERE metadata_id = ? AND element_name = ?", (metadata_id, element_name))
    result = c.fetchone()
    if result:
        element_value = json.loads(result[0])
        return element_value
    else:
        return {"error": "Metadata element not found"}

@router.delete("/metadata/{metadata_id}/metadata-elements/{element_name}", tags=["metadata elements"])
async def delete_metadata_element(metadata_id: int, element_name: str):
    """ Delete a metadata element by metadata id and element name. """
    c.execute("DELETE FROM metadata_elements WHERE metadata_id = ? AND element_name = ?", (metadata_id, element_name))
    conn.commit()
    return {"message": "Metadata element deleted successfully"}