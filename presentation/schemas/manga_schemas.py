import base64
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class NodeData(BaseModel):
    id: str
    label: str
    type: str  # 'work', 'author', 'magazine', 'publication', etc.
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


class VectorSearchRequest(BaseModel):
    query: Optional[str] = None
    embedding: Optional[List[float]] = None
    limit: Optional[int] = 20
    use_hybrid: Optional[bool] = True  # Whether to combine text and vector search


class VectorIndexRequest(BaseModel):
    label: str
    property_name: Optional[str] = "embedding"
    dimension: Optional[int] = 1536
    similarity: Optional[str] = "cosine"


class AddEmbeddingRequest(BaseModel):
    work_id: str
    embedding: List[float]


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
    isbn: Optional[str] = None
    cover_image_url: Optional[str] = None


class MagazineResponse(BaseModel):
    id: str
    name: str
    publisher: str
    established_date: Optional[str] = None


class ImageFetchRequest(BaseModel):
    work_id: str
    cover_url: str


class ImageFetchResponse(BaseModel):
    work_id: str
    image_data: str  # Base64 encoded image data
    content_type: str
    file_size: int
    success: bool
    error: Optional[str] = None

    @classmethod
    def from_bytes(cls, work_id: str, image_data: bytes, content_type: str, success: bool, error: Optional[str] = None):
        """Create response from bytes data"""
        return cls(
            work_id=work_id,
            image_data=base64.b64encode(image_data).decode("utf-8") if image_data else "",
            content_type=content_type or "",
            file_size=len(image_data) if image_data else 0,
            success=success,
            error=error,
        )


class BulkImageFetchRequest(BaseModel):
    requests: List[ImageFetchRequest]


class BulkImageFetchResponse(BaseModel):
    results: List[ImageFetchResponse]
    total_processed: int
    success_count: int
    error_count: int


class CoverResponse(BaseModel):
    work_id: str
    cover_url: Optional[str] = None
    source: str  # 'database', 'api', 'placeholder'
    has_real_cover: bool
    error: Optional[str] = None


class BulkCoverRequest(BaseModel):
    work_ids: List[str]


class BulkCoverResponse(BaseModel):
    results: List[CoverResponse]
    total_processed: int
    success_count: int
    error_count: int
