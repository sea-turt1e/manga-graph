from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from domain.services import MediaArtsDataService
from domain.services.neo4j_media_arts_service import Neo4jMediaArtsService
from domain.use_cases import SearchMangaUseCase
from infrastructure.database import Neo4jMangaRepository
from presentation.schemas import (
    AuthorResponse,
    EdgeData,
    GraphResponse,
    MagazineResponse,
    NodeData,
    SearchRequest,
    WorkResponse,
)

router = APIRouter(prefix="/api/v1", tags=["manga"])
media_arts_router = APIRouter(prefix="/api/v1/media-arts", tags=["media-arts"])
neo4j_router = APIRouter(prefix="/api/v1/neo4j", tags=["neo4j-fast"])


def get_manga_repository():
    """Dependency to get manga repository instance"""
    # This should be configured via dependency injection container
    # For now, we'll return None and handle in mock mode
    return None


def get_search_manga_use_case(repo: Neo4jMangaRepository = Depends(get_manga_repository)):
    """Dependency to get search manga use case"""
    if repo is None:
        # Mock mode - return use case with mock repository
        return None
    return SearchMangaUseCase(repo)


def get_media_arts_service():
    """Dependency to get media arts data service"""
    return MediaArtsDataService()


def get_neo4j_media_arts_service():
    """Dependency to get Neo4j media arts data service"""
    return Neo4jMediaArtsService()


@router.post("/search", response_model=GraphResponse)
async def search_manga(
    request: SearchRequest,
    use_case: SearchMangaUseCase = Depends(get_search_manga_use_case),
    media_arts_service: MediaArtsDataService = Depends(get_media_arts_service),
):
    """Search for manga and related data"""
    try:
        # 文化庁メディア芸術データベースから検索
        media_arts_data = media_arts_service.search_manga_data(search_term=request.query, limit=20)

        # Neo4jデータベースから検索（利用可能な場合）
        neo4j_data = {"nodes": [], "edges": []}
        if use_case is not None:
            neo4j_data = use_case.execute(
                query=request.query, depth=request.depth, node_types=request.node_types, edge_types=request.edge_types
            )

        # 両方のデータソースをマージ
        all_nodes = media_arts_data["nodes"] + neo4j_data["nodes"]
        all_edges = media_arts_data["edges"] + neo4j_data["edges"]

        # 重複を削除（IDベース）
        unique_nodes = []
        seen_node_ids = set()
        for node in all_nodes:
            if node["id"] not in seen_node_ids:
                unique_nodes.append(node)
                seen_node_ids.add(node["id"])

        unique_edges = []
        seen_edge_ids = set()
        for edge in all_edges:
            if edge["id"] not in seen_edge_ids:
                unique_edges.append(edge)
                seen_edge_ids.add(edge["id"])

        return GraphResponse(
            nodes=unique_nodes, edges=unique_edges, total_nodes=len(unique_nodes), total_edges=len(unique_edges)
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/authors", response_model=List[AuthorResponse])
async def get_authors(repo: Neo4jMangaRepository = Depends(get_manga_repository)):
    """Get all authors"""
    try:
        if repo is None:
            # Mock response
            return []

        authors = repo.get_all_authors()
        return [
            AuthorResponse(
                id=author.id,
                name=author.name,
                birth_date=author.birth_date.isoformat() if author.birth_date else None,
                biography=author.biography,
            )
            for author in authors
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/works", response_model=List[WorkResponse])
async def get_works(repo: Neo4jMangaRepository = Depends(get_manga_repository)):
    """Get all works"""
    try:
        if repo is None:
            # Mock response
            return []

        works = repo.get_all_works()
        return [
            WorkResponse(
                id=work.id,
                title=work.title,
                publication_date=work.publication_date.isoformat() if work.publication_date else None,
                genre=work.genre,
                description=work.description,
            )
            for work in works
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/magazines", response_model=List[MagazineResponse])
async def get_magazines(repo: Neo4jMangaRepository = Depends(get_manga_repository)):
    """Get all magazines"""
    try:
        if repo is None:
            # Mock response
            return []

        magazines = repo.get_all_magazines()
        return [
            MagazineResponse(
                id=magazine.id,
                name=magazine.name,
                publisher=magazine.publisher,
                established_date=magazine.established_date.isoformat() if magazine.established_date else None,
            )
            for magazine in magazines
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@media_arts_router.get("/search", response_model=GraphResponse)
async def search_media_arts(
    q: str = Query(..., description="検索キーワード"),
    limit: int = Query(20, description="結果の上限"),
    media_arts_service: MediaArtsDataService = Depends(get_media_arts_service),
):
    """文化庁メディア芸術データベースから漫画データを検索"""
    try:
        graph_data = media_arts_service.search_manga_data(search_term=q, limit=limit)

        return GraphResponse(
            nodes=graph_data["nodes"],
            edges=graph_data["edges"],
            total_nodes=len(graph_data["nodes"]),
            total_edges=len(graph_data["edges"]),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@media_arts_router.get("/creator/{creator_name}", response_model=GraphResponse)
async def get_creator_works_media_arts(
    creator_name: str,
    limit: int = Query(50, description="結果の上限"),
    media_arts_service: MediaArtsDataService = Depends(get_media_arts_service),
):
    """文化庁メディア芸術データベースから作者の作品を取得"""
    try:
        graph_data = media_arts_service.get_creator_works(creator_name=creator_name, limit=limit)

        return GraphResponse(
            nodes=graph_data["nodes"],
            edges=graph_data["edges"],
            total_nodes=len(graph_data["nodes"]),
            total_edges=len(graph_data["edges"]),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@media_arts_router.get("/magazines", response_model=GraphResponse)
async def get_manga_magazines_media_arts(
    limit: int = Query(100, description="結果の上限"),
    media_arts_service: MediaArtsDataService = Depends(get_media_arts_service),
):
    """文化庁メディア芸術データベースから漫画雑誌データを取得"""
    try:
        graph_data = media_arts_service.get_manga_magazines_graph(limit=limit)

        return GraphResponse(
            nodes=graph_data["nodes"],
            edges=graph_data["edges"],
            total_nodes=len(graph_data["nodes"]),
            total_edges=len(graph_data["edges"]),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@media_arts_router.get("/fulltext-search", response_model=GraphResponse)
async def fulltext_search_media_arts(
    q: str = Query(..., description="検索キーワード"),
    search_type: str = Query("simple_query_string", description="検索タイプ"),
    limit: int = Query(20, description="結果の上限"),
    media_arts_service: MediaArtsDataService = Depends(get_media_arts_service),
):
    """文化庁メディア芸術データベースで全文検索"""
    try:
        graph_data = media_arts_service.search_with_fulltext(search_term=q, search_type=search_type, limit=limit)

        return GraphResponse(
            nodes=graph_data["nodes"],
            edges=graph_data["edges"],
            total_nodes=len(graph_data["nodes"]),
            total_edges=len(graph_data["edges"]),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@media_arts_router.get("/search-with-related", response_model=GraphResponse)
async def search_media_arts_with_related(
    q: str = Query(..., description="検索キーワード"),
    limit: int = Query(20, description="結果の上限"),
    include_related: bool = Query(True, description="関連作品を含めるかどうか"),
    media_arts_service: MediaArtsDataService = Depends(get_media_arts_service),
):
    """文化庁メディア芸術データベースから漫画データを検索（関連作品含む）"""
    try:
        graph_data = media_arts_service.search_manga_data_with_related(
            search_term=q, limit=limit, include_related=include_related
        )

        return GraphResponse(
            nodes=graph_data["nodes"],
            edges=graph_data["edges"],
            total_nodes=len(graph_data["nodes"]),
            total_edges=len(graph_data["edges"]),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@media_arts_router.get("/magazine-relationships", response_model=GraphResponse)
async def get_magazine_relationships_media_arts(
    magazine_name: Optional[str] = Query(None, description="雑誌名（部分一致）"),
    year: Optional[str] = Query(None, description="出版年"),
    limit: int = Query(50, description="結果の上限"),
    media_arts_service: MediaArtsDataService = Depends(get_media_arts_service)
):
    """文化庁メディア芸術データベースから同じ掲載誌・同じ時期の漫画関係を取得"""
    try:
        graph_data = media_arts_service.get_magazine_relationships(
            magazine_name=magazine_name,
            year=year,
            limit=limit
        )
        
        return GraphResponse(
            nodes=graph_data["nodes"],
            edges=graph_data["edges"],
            total_nodes=len(graph_data["nodes"]),
            total_edges=len(graph_data["edges"])
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Neo4j Fast Search Endpoints
@neo4j_router.get("/search", response_model=GraphResponse)
async def search_neo4j_fast(
    q: str = Query(..., description="検索キーワード"),
    limit: int = Query(20, description="結果の上限"),
    include_related: bool = Query(True, description="関連作品を含めるかどうか"),
    neo4j_service: Neo4jMediaArtsService = Depends(get_neo4j_media_arts_service),
):
    """Neo4jを使用した高速検索"""
    try:
        graph_data = neo4j_service.search_manga_data_with_related(
            search_term=q, limit=limit, include_related=include_related
        )

        return GraphResponse(
            nodes=graph_data["nodes"],
            edges=graph_data["edges"],
            total_nodes=len(graph_data["nodes"]),
            total_edges=len(graph_data["edges"]),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@neo4j_router.get("/creator/{creator_name}", response_model=GraphResponse)
async def get_creator_works_neo4j(
    creator_name: str,
    limit: int = Query(50, description="結果の上限"),
    neo4j_service: Neo4jMediaArtsService = Depends(get_neo4j_media_arts_service),
):
    """Neo4jを使用した作者の作品検索"""
    try:
        graph_data = neo4j_service.get_creator_works(creator_name=creator_name, limit=limit)

        return GraphResponse(
            nodes=graph_data["nodes"],
            edges=graph_data["edges"],
            total_nodes=len(graph_data["nodes"]),
            total_edges=len(graph_data["edges"]),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@neo4j_router.get("/stats")
async def get_neo4j_stats(
    neo4j_service: Neo4jMediaArtsService = Depends(get_neo4j_media_arts_service),
):
    """Neo4jデータベースの統計情報を取得"""
    try:
        stats = neo4j_service.get_database_statistics()
        return {"status": "success", "data": stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
