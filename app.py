from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from azure.storage.blob import BlobServiceClient
import os
import json
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration Azure Blob Storage
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=storagebros;AccountKey=Ix9SHXlpJjuAGz2QnDWupLtUl1C9kcqH79XYE8daleIrMrS2hKecI/hoHaFr0nSTQGTtHxF6Tbhk+AStGrAT9A==;EndpointSuffix=core.windows.net"
CONTAINER_CSV = "csv-uploads"
CONTAINER_JSON = "json-report"

if not AZURE_STORAGE_CONNECTION_STRING or not CONTAINER_CSV or not CONTAINER_JSON:
    raise RuntimeError("Les variables d'environnement Azure ne sont pas définies !")

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_JSON)
container_client_csv = blob_service_client.get_container_client(CONTAINER_CSV)

# Initialiser l'application FastAPI
app = FastAPI()

# Activer CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# root
@app.get("/")
def read_root():
    return "CSVitesse"

# Endpoint pour uploader un fichier CSV
@app.post("/csv/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Seuls les fichiers CSV sont autorisés")
    
    try:
        blob_client = container_client_csv.get_blob_client(file.filename)
        blob_client.upload_blob(await file.read(), overwrite=True)
        return {"message": f"Fichier {file.filename} téléversé avec succès"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint pour lister les fichiers CSV disponibles
@app.get("/csv/files")
async def list_csv_files():
    blob_csv_list = container_client_csv.list_blobs()
    csv_files = [blob.name for blob in blob_csv_list if blob.name.endswith('.csv')]
    return JSONResponse(content=csv_files)

# Endpoint pour supprimer un fichier CSV
@app.delete("/csv/delete/{filename}")
async def delete_csv(filename: str):
    try:
        blob_client = container_client_csv.get_blob_client(filename)
        if not blob_client.exists():
            raise HTTPException(status_code=404, detail=f"Fichier {filename} introuvable")
        
        blob_client.delete_blob()
        return {"message": f"Fichier {filename} supprimé avec succès"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint pour lister les fichiers JSON disponibles
@app.get("/files")
async def list_json_files():
    blob_list = container_client.list_blobs()
    json_files = [blob.name for blob in blob_list if blob.name.endswith('.json')]
    return JSONResponse(content=json_files)

# Endpoint pour récupérer le contenu d'un fichier JSON
@app.get("/file/{filename}")
async def get_json_file(filename: str):
    try:
        blob_client = container_client.get_blob_client(filename)
        json_data = blob_client.download_blob().readall()
        return JSONResponse(content=json.loads(json_data))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))