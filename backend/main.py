from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import os
from dotenv import load_dotenv

# from database import Neo4jConnection
from models import SearchRequest, GraphResponse, NodeData, EdgeData

load_dotenv()

app = FastAPI(title="Manga Graph API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Neo4j connection - temporarily disabled for testing
# neo4j_conn = Neo4jConnection(
#     uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
#     user=os.getenv("NEO4J_USER", "neo4j"),
#     password=os.getenv("NEO4J_PASSWORD", "password")
# )

# @app.on_event("startup")
# async def startup_event():
#     """Initialize database connection on startup"""
#     neo4j_conn.connect()

# @app.on_event("shutdown") 
# async def shutdown_event():
#     """Close database connection on shutdown"""
#     neo4j_conn.close()

@app.get("/")
async def root():
    return {"message": "Manga Graph API"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "database": "mock"}

@app.post("/search", response_model=GraphResponse)
async def search_manga(request: SearchRequest):
    """Search for manga and related data"""
    # Temporary fix: return sample data to test frontend
    sample_response = {
        "nodes": [
            {
                "id": "1",
                "label": "ONE PIECE",
                "type": "work",
                "properties": {"title": "ONE PIECE", "publisher": "集英社", "publication_date": "1997"}
            },
            {
                "id": "2", 
                "label": "尾田栄一郎",
                "type": "author",
                "properties": {"name": "尾田栄一郎", "birth_date": "1975-01-01"}
            },
            {
                "id": "3",
                "label": "NARUTO", 
                "type": "work",
                "properties": {"title": "NARUTO", "publisher": "集英社", "publication_date": "1999"}
            }
        ],
        "edges": [
            {
                "id": "edge1",
                "source": "2",
                "target": "1", 
                "type": "created",
                "properties": {}
            },
            {
                "id": "edge2",
                "source": "1",
                "target": "3",
                "type": "same_publisher", 
                "properties": {}
            }
        ]
    }
    
    return GraphResponse(
        nodes=sample_response["nodes"],
        edges=sample_response["edges"],
        total_nodes=len(sample_response["nodes"]),
        total_edges=len(sample_response["edges"])
    )

@app.get("/authors")
async def get_authors():
    """Get all authors"""
    try:
        authors = neo4j_conn.get_all_authors()
        return {"authors": authors}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/works")
async def get_works():
    """Get all works"""
    try:
        works = neo4j_conn.get_all_works()
        return {"works": works}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/magazines")
async def get_magazines():
    """Get all magazines"""
    try:
        magazines = neo4j_conn.get_all_magazines()
        return {"magazines": magazines}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))