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


class SynopsisVectorSearchRequest(BaseModel):
    work_id: str  # 検索クエリの基準となる Work ノード ID
    limit: Optional[int] = 10


class SynopsisVectorSearchResponseItem(BaseModel):
    work_id: str
    title: Optional[str] = None
    synopsis: Optional[str] = None
    similarity_score: Optional[float] = None


class SynopsisVectorSearchResponse(BaseModel):
    results: List[SynopsisVectorSearchResponseItem]
    total: int


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


class MagazineWorkGraphRequest(BaseModel):
    magazine_element_ids: List[str]
    work_limit: Optional[int] = 50
    include_hentai: Optional[bool] = False
    reference_work_id: Optional[str] = None  # For priority-based sorting


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


class TitleSimilarityItem(BaseModel):
    title: str
    similarity_score: float


class TitleSimilarityResponse(BaseModel):
    results: List[TitleSimilarityItem]
    total: int


class EmbeddingSimilaritySearchRequest(BaseModel):
    """類似度検索APIのリクエストスキーマ"""

    query: str
    embedding_type: str = "title_ja"  # "title_ja", "title_en", "description"
    embedding_dims: int = 256  # Matryoshka dimensions: 128, 256, 512, 1024, 2048
    limit: int = 5  # 返却件数
    threshold: float = 0.5  # 類似度閾値（コサイン類似度）
    include_hentai: bool = False


class EmbeddingSimilaritySearchResultItem(BaseModel):
    """類似度検索結果の1件"""

    work_id: str
    title_en: Optional[str] = None
    title_ja: Optional[str] = None
    description: Optional[str] = None
    similarity_score: float
    media_type: Optional[str] = None
    genres: Optional[List[str]] = None


class EmbeddingSimilaritySearchResponse(BaseModel):
    """類似度検索APIのレスポンススキーマ"""

    results: List[EmbeddingSimilaritySearchResultItem]
    total: int
    query: str
    embedding_type: str
    embedding_dims: int
    threshold: float


class MultiEmbeddingSimilaritySearchRequest(BaseModel):
    """複数埋め込みタイプで並列に類似度検索を行うAPIのリクエストスキーマ"""

    query: str
    embedding_types: List[str] = ["title_en", "title_ja"]  # 検索対象の埋め込みタイプリスト
    embedding_dims: int = 256  # Matryoshka dimensions: 128, 256, 512, 1024, 2048
    limit: int = 10  # 返却件数
    threshold: float = 0.3  # 類似度閾値（コサイン類似度）
    include_hentai: bool = False


class MultiEmbeddingSimilaritySearchResponse(BaseModel):
    """複数埋め込みタイプで並列に類似度検索を行うAPIのレスポンススキーマ"""

    results: List[EmbeddingSimilaritySearchResultItem]
    total: int
    query: str
    embedding_types: List[str]
    embedding_dims: int
    threshold: float


class RelatedGraphBatchRequest(BaseModel):
    """Author, Magazine, Publisherの関連グラフを並列取得するリクエストスキーマ"""

    author_node_id: Optional[str] = None
    magazine_node_id: Optional[str] = None
    publisher_node_id: Optional[str] = None
    author_limit: int = 5
    magazine_limit: int = 5
    publisher_limit: int = 3
    reference_work_id: Optional[str] = None  # magazine検索でのソート基準
    exclude_magazine_id: Optional[str] = None  # publisher検索での除外対象
    include_hentai: bool = False


class RelatedGraphBatchResponse(BaseModel):
    """Author, Magazine, Publisherの関連グラフを並列取得するレスポンススキーマ"""

    author_graph: Optional[GraphResponse] = None
    magazine_graph: Optional[GraphResponse] = None
    publisher_graph: Optional[GraphResponse] = None
