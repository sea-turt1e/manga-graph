from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from domain.services import MediaArtsDataService
from domain.services.cover_cache_service import get_cache_service
from domain.services.cover_image_service import get_cover_service
from domain.services.image_fetch_service import get_image_fetch_service, ImageFetchService
from domain.services.neo4j_media_arts_service import Neo4jMediaArtsService
from domain.use_cases import SearchMangaUseCase
from infrastructure.database import Neo4jMangaRepository
from presentation.schemas import (
    AuthorResponse,
    BulkCoverRequest,
    BulkCoverResponse,
    BulkImageFetchRequest,
    BulkImageFetchResponse,
    CoverResponse,
    GraphResponse,
    ImageFetchRequest,
    ImageFetchResponse,
    MagazineResponse,
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


def get_cover_image_service():
    """Dependency to get cover image service"""
    return get_cover_service()


def get_cover_cache_service():
    """Dependency to get cover cache service"""
    return get_cache_service()


def get_image_fetch_service_dep():
    """Dependency to get image fetch service"""
    return get_image_fetch_service()


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
                isbn=work.isbn,
                cover_image_url=work.cover_image_url,
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
    media_arts_service: MediaArtsDataService = Depends(get_media_arts_service),
):
    """文化庁メディア芸術データベースから同じ掲載誌・同じ時期の漫画関係を取得"""
    try:
        graph_data = media_arts_service.get_magazine_relationships(magazine_name=magazine_name, year=year, limit=limit)

        return GraphResponse(
            nodes=graph_data["nodes"],
            edges=graph_data["edges"],
            total_nodes=len(graph_data["nodes"]),
            total_edges=len(graph_data["edges"]),
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


# Cover Image Endpoints
cover_router = APIRouter(prefix="/api/v1/covers", tags=["cover-images"])

# Image Fetch Endpoints
image_router = APIRouter(prefix="/api/v1/images", tags=["image-fetch"])


@cover_router.get("/work/{work_id:path}")
async def get_work_cover(
    work_id: str,
    cover_service=Depends(get_cover_image_service),
    neo4j_service: Neo4jMediaArtsService = Depends(get_neo4j_media_arts_service),
):
    """作品の書影URLを取得"""
    try:
        # Get work details from Neo4j
        work_data = neo4j_service.get_work_by_id(work_id)
        if not work_data:
            raise HTTPException(status_code=404, detail="Work not found")

        # Try to get cover using existing cover_image_url first
        if work_data.get("cover_image_url"):
            if cover_service.validate_cover_url(work_data["cover_image_url"]):
                return {
                    "work_id": work_id,
                    "cover_url": work_data["cover_image_url"],
                    "source": "database",
                    "has_real_cover": True,
                }

        # Try to get cover using ISBN
        isbn = work_data.get("isbn")
        title = work_data.get("title")
        genre = work_data.get("genre")
        publisher = work_data.get("publisher")

        cover_result = cover_service.get_cover_with_fallback(isbn=isbn, title=title, genre=genre, publisher=publisher)

        return {"work_id": work_id, **cover_result}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@cover_router.post("/bulk", response_model=BulkCoverResponse)
async def get_bulk_covers(
    request: BulkCoverRequest,
    cover_service=Depends(get_cover_image_service),
    neo4j_service: Neo4jMediaArtsService = Depends(get_neo4j_media_arts_service),
):
    """複数作品の書影URLを一括取得"""
    try:
        results = []
        success_count = 0
        error_count = 0

        for work_id in request.work_ids:
            try:
                # Get work details from Neo4j
                work_data = neo4j_service.get_work_by_id(work_id)
                if not work_data:
                    results.append(CoverResponse(
                        work_id=work_id,
                        cover_url=None,
                        source="error",
                        has_real_cover=False,
                        error="Work not found"
                    ))
                    error_count += 1
                    continue

                # Try to get cover using existing cover_image_url first
                if work_data.get("cover_image_url"):
                    if cover_service.validate_cover_url(work_data["cover_image_url"]):
                        results.append(CoverResponse(
                            work_id=work_id,
                            cover_url=work_data["cover_image_url"],
                            source="database",
                            has_real_cover=True
                        ))
                        success_count += 1
                        continue

                # Try to get cover using ISBN
                isbn = work_data.get("isbn")
                title = work_data.get("title")
                genre = work_data.get("genre")
                publisher = work_data.get("publisher")

                cover_result = cover_service.get_cover_with_fallback(
                    isbn=isbn, title=title, genre=genre, publisher=publisher
                )

                results.append(CoverResponse(
                    work_id=work_id,
                    cover_url=cover_result.get("cover_url"),
                    source=cover_result.get("source", "unknown"),
                    has_real_cover=cover_result.get("has_real_cover", False)
                ))
                success_count += 1

            except Exception as e:
                results.append(CoverResponse(
                    work_id=work_id,
                    cover_url=None,
                    source="error",
                    has_real_cover=False,
                    error=str(e)
                ))
                error_count += 1

        return BulkCoverResponse(
            results=results,
            total_processed=len(request.work_ids),
            success_count=success_count,
            error_count=error_count
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@cover_router.post("/work/{work_id:path}/update")
async def update_work_cover(
    work_id: str,
    cover_service=Depends(get_cover_image_service),
    neo4j_service: Neo4jMediaArtsService = Depends(get_neo4j_media_arts_service),
):
    """作品の書影URLを更新"""
    try:
        # Get work details from Neo4j
        work_data = neo4j_service.get_work_by_id(work_id)
        if not work_data:
            raise HTTPException(status_code=404, detail="Work not found")

        isbn = work_data.get("isbn")
        title = work_data.get("title")

        if not isbn:
            raise HTTPException(status_code=400, detail="Work has no ISBN for cover lookup")

        # Get cover image URL
        cover_url = cover_service.get_cover_image_url(isbn, title)

        if cover_url:
            # Update in database
            success = neo4j_service.update_work_cover_image(work_id, cover_url)
            if success:
                return {"work_id": work_id, "cover_url": cover_url, "status": "updated", "source": "api"}
            else:
                raise HTTPException(status_code=500, detail="Failed to update cover in database")
        else:
            raise HTTPException(status_code=404, detail="No cover image found for this work")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@cover_router.post("/bulk-update")
async def bulk_update_covers(
    limit: int = Query(100, description="更新する作品数の上限"),
    cover_service=Depends(get_cover_image_service),
    neo4j_service: Neo4jMediaArtsService = Depends(get_neo4j_media_arts_service),
):
    """ISBNを持つ作品の書影を一括更新"""
    try:
        # Get works with ISBN but no cover image
        works = neo4j_service.get_works_needing_covers(limit)

        updated_count = 0
        failed_count = 0
        results = []

        for work in works:
            try:
                work_id = work["id"]
                isbn = work["isbn"]
                title = work.get("title", "")

                cover_url = cover_service.get_cover_image_url(isbn, title)

                if cover_url:
                    success = neo4j_service.update_work_cover_image(work_id, cover_url)
                    if success:
                        updated_count += 1
                        results.append(
                            {"work_id": work_id, "title": title, "cover_url": cover_url, "status": "updated"}
                        )
                    else:
                        failed_count += 1
                        results.append({"work_id": work_id, "title": title, "status": "database_error"})
                else:
                    failed_count += 1
                    results.append({"work_id": work_id, "title": title, "status": "no_cover_found"})

                # Rate limiting
                import time

                time.sleep(0.1)

            except Exception as e:
                failed_count += 1
                results.append(
                    {
                        "work_id": work.get("id", "unknown"),
                        "title": work.get("title", "unknown"),
                        "status": "error",
                        "error": str(e),
                    }
                )

        return {
            "total_processed": len(works),
            "updated_count": updated_count,
            "failed_count": failed_count,
            "results": results,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@cover_router.get("/cache/stats")
async def get_cache_stats(
    cache_service=Depends(get_cover_cache_service),
):
    """キャッシュの統計情報を取得"""
    try:
        stats = cache_service.get_cache_stats()
        return {"status": "success", "cache_stats": stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@cover_router.post("/cache/cleanup")
async def cleanup_cache(
    cache_service=Depends(get_cover_cache_service),
):
    """期限切れのキャッシュエントリをクリーンアップ"""
    try:
        cache_service.cleanup_expired()
        stats = cache_service.get_cache_stats()
        return {"status": "cleanup_completed", "cache_stats": stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@cover_router.delete("/cache/clear")
async def clear_cache(
    cache_service=Depends(get_cover_cache_service),
):
    """全てのキャッシュをクリア"""
    try:
        cache_service.clear_cache()
        return {"status": "cache_cleared", "message": "All cache entries have been cleared"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@cover_router.delete("/cache/invalidate/{isbn}")
async def invalidate_cache_entry(
    isbn: str,
    title: Optional[str] = Query(None, description="作品タイトル（オプション）"),
    cache_service=Depends(get_cover_cache_service),
):
    """特定のISBNのキャッシュエントリを無効化"""
    try:
        cache_service.invalidate_cache(isbn, title)
        return {"status": "cache_invalidated", "isbn": isbn, "title": title}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Image Fetch API Endpoints
@image_router.post("/fetch", response_model=ImageFetchResponse)
async def fetch_single_image(
    request: ImageFetchRequest,
    image_service: ImageFetchService = Depends(get_image_fetch_service_dep),
):
    """Fetch a single image from URL"""
    try:
        async with image_service:
            result = await image_service.fetch_single_image(request.work_id, request.cover_url)

            return ImageFetchResponse.from_bytes(
                work_id=result['work_id'],
                image_data=result['image_data'],
                content_type=result['content_type'],
                success=result['success'],
                error=result['error']
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@image_router.post("/fetch-bulk", response_model=BulkImageFetchResponse)
async def fetch_bulk_images(
    request: BulkImageFetchRequest,
    image_service: ImageFetchService = Depends(get_image_fetch_service_dep),
):
    """Fetch multiple images concurrently from URLs"""
    try:
        # Convert requests to format expected by service
        fetch_requests = [
            {"work_id": req.work_id, "cover_url": req.cover_url}
            for req in request.requests
        ]

        async with image_service:
            results = await image_service.fetch_images(fetch_requests)

            # Convert results to response format
            response_results = []
            success_count = 0

            for result in results:
                image_response = ImageFetchResponse.from_bytes(
                    work_id=result['work_id'],
                    image_data=result['image_data'],
                    content_type=result['content_type'],
                    success=result['success'],
                    error=result['error']
                )
                response_results.append(image_response)

                if result['success']:
                    success_count += 1

            return BulkImageFetchResponse(
                results=response_results,
                total_processed=len(results),
                success_count=success_count,
                error_count=len(results) - success_count
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
