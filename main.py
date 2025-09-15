import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        os.getenv("LOCALHOST_URL", "http://localhost:3000"),
        os.getenv("PRODUCTION_URL"),
    ],
    allow_credentials=True,
    allow_methods=["*"],
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
