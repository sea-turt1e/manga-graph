from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

class SearchRequest(BaseModel):
    query: str
    depth: int = 2

class GraphResponse(BaseModel):
    nodes: list
    edges: list
    total_nodes: int
    total_edges: int

@app.get("/")
async def root():
    return {"message": "Test API"}

@app.post("/search")
async def search_test(request: SearchRequest, response: Response):
    # Add CORS headers manually
    response.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    
    return {
        "nodes": [
            {"id": "1", "label": "ONE PIECE", "type": "work", "properties": {}},
            {"id": "2", "label": "尾田栄一郎", "type": "author", "properties": {"name": "尾田栄一郎"}},
            {"id": "3", "label": "NARUTO", "type": "work", "properties": {"title": "NARUTO"}}
        ],
        "edges": [
            {"id": "edge1", "source": "2", "target": "1", "type": "created", "properties": {}},
            {"id": "edge2", "source": "1", "target": "3", "type": "same_publisher", "properties": {}}
        ],
        "total_nodes": 3,
        "total_edges": 2
    }