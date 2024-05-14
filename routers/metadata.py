import sqlite3
import json
from fastapi import APIRouter, Body

router = APIRouter()

# Connect to SQLite database
conn = sqlite3.connect('metadata.db')
c = conn.cursor()

@router.post("/metadata", tags=["metadata"])
async def create_metadata(name: str = Body(...), primary_key: str = Body(...), alternate_keys: list = Body(...)):
    """ Create a new metadata entry. """
    alternate_keys_json = json.dumps(alternate_keys)
    c.execute("INSERT INTO metadata (name, primary_key, alternate_keys) VALUES (?, ?, ?)", (name, primary_key, alternate_keys_json))
    conn.commit()
    return {"message": "Metadata created successfully", "id": c.lastrowid}

@router.get("/metadata/{metadata_id}", tags=["metadata"])
async def get_metadata(metadata_id: int):
    """ Get metadata by id. """
    c.execute("SELECT name, primary_key, alternate_keys FROM metadata WHERE id = ?", (metadata_id,))
    result = c.fetchone()
    if result:
        name, primary_key, alternate_keys_json = result
        alternate_keys = json.loads(alternate_keys_json)
        metadata_dict = {"name": name, "primary_key": primary_key, "alternate_keys": alternate_keys, "elements": []}

    c.execute("SELECT element_name, element_value FROM metadata_elements WHERE metadata_id = ?", (metadata_id,))

    elements = c.fetchall()

    for element_name, element_value in elements:
        metadata_dict["elements"].append({"name": element_name, "value": json.loads(element_value)})
    return metadata_dict

@router.put("/metadata/{metadata_id}", tags=["metadata"])
async def update_metadata(metadata_id: int, name: str = Body(...), primary_key: str = Body(...), alternate_keys: list = Body(...)):
    """ Update metadata by id. """
    alternate_keys_json = json.dumps(alternate_keys)
    c.execute("UPDATE metadata SET name = ?, primary_key = ?, alternate_keys = ? WHERE id = ?", (name, primary_key, alternate_keys_json, metadata_id))
    conn.commit()
    return {"message": "Metadata updated successfully"}

@router.delete("/metadata/{metadata_id}", tags=["metadata"])
async def delete_metadata(metadata_id: int):
    """ Delete metadata by id. """
    c.execute("DELETE FROM metadata WHERE id = ?", (metadata_id,))
    c.execute("DELETE FROM metadata_elements WHERE metadata_id = ?", (metadata_id,))
    conn.commit()
    return {"message": "Metadata deleted successfully"}