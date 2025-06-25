from pydantic import BaseModel
from typing import List, Optional, Dict, Any


class NodeData(BaseModel):
    id: str
    label: str
    type: str  # 'work', 'author', 'magazine', etc.
    properties: Dict[str, Any]


class EdgeData(BaseModel):
    id: str
    source: str
    target: str
    type: str  # 'created_by', 'published_in', 'mentor_of', 'same_magazine', etc.
    properties: Optional[Dict[str, Any]] = {}


class SearchRequest(BaseModel):
    query: str
    depth: Optional[int] = 2
    node_types: Optional[List[str]] = None
    edge_types: Optional[List[str]] = None


class GraphResponse(BaseModel):
    nodes: List[NodeData]
    edges: List[EdgeData]
    total_nodes: int
    total_edges: int


class AuthorResponse(BaseModel):
    id: str
    name: str
    birth_date: Optional[str] = None
    biography: Optional[str] = None


class WorkResponse(BaseModel):
    id: str
    title: str
    publication_date: Optional[str] = None
    genre: Optional[str] = None
    description: Optional[str] = None


class MagazineResponse(BaseModel):
    id: str
    name: str
    publisher: str
    established_date: Optional[str] = None