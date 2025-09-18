import os

from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from neo4j import GraphDatabase

# Ensure environment variables are loaded at import time
from config import env  # noqa: F401
from presentation.api import (
    cover_router,
    image_router,
    manga_router,
    media_arts_router,
    neo4j_router,
    text_generation_router,
)

app = FastAPI(title="Manga Graph API", version="1.0.0")

allow_origins = [
    os.getenv("VISUALIZER_URL", "http://localhost:3000"),
    os.getenv("GRAPHRAG_URL", "http://localhost:8501"),
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT"],
    allow_headers=["*"],
)

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD),
)

# Include routers
app.include_router(manga_router)
app.include_router(media_arts_router)
app.include_router(neo4j_router)
app.include_router(cover_router)
app.include_router(image_router)
app.include_router(text_generation_router)


@app.get("/")
async def root():
    return {"message": "Manga Graph API"}


@app.get("/health")
async def health_check():
    # Check if the Neo4j driver can connect to the database
    try:
        driver.verify_connectivity()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


API_KEYS = [
    os.getenv("API_KEY_VISUALIZER"),
    os.getenv("API_KEY_GRAPHRAG"),
]

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_api_key(api_key: str = Security(api_key_header)):
    if api_key in API_KEYS:
        return api_key
    raise HTTPException(status_code=401, detail="Invalid or missing API Key")


@app.get("/protected")
def protected_endpoint(api_key: str = Depends(get_api_key)):
    return {"message": "Access granted"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
