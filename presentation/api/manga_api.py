from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from domain.services import MediaArtsDataService
from domain.services.batch_embedding_processor import BatchEmbeddingProcessor
from domain.services.cover_cache_service import get_cache_service
from domain.services.cover_image_service import get_cover_service
from domain.services.image_fetch_service import ImageFetchService, get_image_fetch_service
from domain.services.neo4j_media_arts_service import Neo4jMediaArtsService
from domain.use_cases import SearchMangaUseCase
from infrastructure.database import Neo4jMangaRepository
from presentation.schemas import (
    AddEmbeddingRequest,
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
    SynopsisVectorSearchRequest,
    SynopsisVectorSearchResponse,
    SynopsisVectorSearchResponseItem,
    VectorIndexRequest,
    VectorSearchRequest,
    WorkResponse,
)

router = APIRouter(prefix="/api/v1", tags=["manga"])
media_arts_router = APIRouter(prefix="/api/v1/media-arts", tags=["media-arts"])
neo4j_router = APIRouter(prefix="/api/v1/neo4j", tags=["neo4j-fast"])
# BatchEmbeddingProcessorを使って埋め込みを生成


ruri_processor = BatchEmbeddingProcessor(
    embedding_method="huggingface",
    sentence_transformer_model="cl-nagoya/ruri-v3-310m",
)


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


@neo4j_router.post("/vector/search/synopsis", response_model=SynopsisVectorSearchResponse)
async def synopsis_vector_search(
    request: SynopsisVectorSearchRequest,
    neo4j_service: Neo4jMediaArtsService = Depends(get_neo4j_media_arts_service),
):
    """Work.synopsis_embedding を利用した synopsis 類似検索 (title と synopsis を上位 n 件返却)"""
    try:
        if neo4j_service is None or neo4j_service.neo4j_repository is None:
            raise HTTPException(status_code=503, detail="Neo4j service not available")
        if not request.work_id:
            raise HTTPException(status_code=400, detail="work_id is required")

        # 基準となる Work ノードの synopsis_embedding を取得
        with neo4j_service.neo4j_repository.driver.session() as session:
            fetch_query = """
            MATCH (w:Work {id: $work_id})
            RETURN w.synopsis_embedding AS embedding
            """
            record = session.run(fetch_query, work_id=request.work_id).single()
            if record is None or record.get("embedding") is None:
                raise HTTPException(status_code=404, detail="Synopsis embedding not found for given work_id")
            query_embedding = record["embedding"]

        raw_results = neo4j_service.neo4j_repository.search_work_synopsis_by_vector(
            embedding=query_embedding, limit=request.limit
        )
        # 自分自身のノードが結果に含まれる場合、先頭に来ることが多いのでそのまま残すか除外するか判断
        # 現状: 残す（利用側で必要なら除外可能）
        items = [SynopsisVectorSearchResponseItem(**r) for r in raw_results]
        return SynopsisVectorSearchResponse(results=items, total=len(items))
    except HTTPException:
        raise
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
    sort_total_volumes: Optional[str] = Query(
        None,
        description="total_volumesでソート: 'asc' または 'desc'。未指定ならタイトル既定順",
        regex="^(asc|desc)$",
    ),
    min_total_volumes: Optional[int] = Query(
        None,
        description="total_volumes がこの値以上の作品のみ表示（メイン + related）。メインで0件ならフィルタ無効化して再表示",
        ge=1,
    ),
    neo4j_service: Neo4jMediaArtsService = Depends(get_neo4j_media_arts_service),
):
    """Neo4jを使用した高速検索"""
    try:
        graph_data = neo4j_service.search_manga_data_with_related(
            search_term=q,
            limit=limit,
            include_related=include_related,
            sort_total_volumes=sort_total_volumes,
            min_total_volumes=min_total_volumes,
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
                    results.append(
                        CoverResponse(
                            work_id=work_id,
                            cover_url=None,
                            source="error",
                            has_real_cover=False,
                            error="Work not found",
                        )
                    )
                    error_count += 1
                    continue

                # Try to get cover using existing cover_image_url first
                if work_data.get("cover_image_url"):
                    if cover_service.validate_cover_url(work_data["cover_image_url"]):
                        results.append(
                            CoverResponse(
                                work_id=work_id,
                                cover_url=work_data["cover_image_url"],
                                source="database",
                                has_real_cover=True,
                            )
                        )
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

                results.append(
                    CoverResponse(
                        work_id=work_id,
                        cover_url=cover_result.get("cover_url"),
                        source=cover_result.get("source", "unknown"),
                        has_real_cover=cover_result.get("has_real_cover", False),
                    )
                )
                success_count += 1

            except Exception as e:
                results.append(
                    CoverResponse(work_id=work_id, cover_url=None, source="error", has_real_cover=False, error=str(e))
                )
                error_count += 1

        return BulkCoverResponse(
            results=results, total_processed=len(request.work_ids), success_count=success_count, error_count=error_count
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
                work_id=result["work_id"],
                image_data=result["image_data"],
                content_type=result["content_type"],
                success=result["success"],
                error=result["error"],
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
        fetch_requests = [{"work_id": req.work_id, "cover_url": req.cover_url} for req in request.requests]

        async with image_service:
            results = await image_service.fetch_images(fetch_requests)

            # Convert results to response format
            response_results = []
            success_count = 0

            for result in results:
                image_response = ImageFetchResponse.from_bytes(
                    work_id=result["work_id"],
                    image_data=result["image_data"],
                    content_type=result["content_type"],
                    success=result["success"],
                    error=result["error"],
                )
                response_results.append(image_response)

                if result["success"]:
                    success_count += 1

            return BulkImageFetchResponse(
                results=response_results,
                total_processed=len(results),
                success_count=success_count,
                error_count=len(results) - success_count,
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Vector search endpoints
@neo4j_router.post("/vector/create-index")
async def create_vector_index(
    request: VectorIndexRequest,
    neo4j_service: Neo4jMediaArtsService = Depends(get_neo4j_media_arts_service),
):
    """Create a vector index for semantic search"""
    try:
        if neo4j_service is None or neo4j_service.neo4j_repository is None:
            raise HTTPException(status_code=503, detail="Neo4j service not available")

        neo4j_service.neo4j_repository.create_vector_index(
            label=request.label,
            property_name=request.property_name,
            dimension=request.dimension,
            similarity=request.similarity,
        )
        return {"message": f"Vector index created successfully for {request.label}.{request.property_name}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@neo4j_router.post("/vector/search", response_model=GraphResponse)
async def vector_search_manga(
    request: VectorSearchRequest,
    neo4j_service: Neo4jMediaArtsService = Depends(get_neo4j_media_arts_service),
):
    """Search manga using vector similarity"""
    try:
        if neo4j_service is None or neo4j_service.neo4j_repository is None:
            raise HTTPException(status_code=503, detail="Neo4j service not available")

        # Validate input
        if not request.query and not request.embedding:
            raise HTTPException(status_code=400, detail="Either query or embedding must be provided")

        # Get search results
        if request.use_hybrid and request.query and request.embedding:
            # Hybrid search
            results = neo4j_service.neo4j_repository.search_manga_works_with_vector(
                search_term=request.query, embedding=request.embedding, limit=request.limit
            )
        elif request.embedding:
            # Vector only search
            results = neo4j_service.neo4j_repository.search_by_vector(embedding=request.embedding, limit=request.limit)
        elif request.query:
            # Text only search
            results = neo4j_service.neo4j_repository.search_manga_works(search_term=request.query, limit=request.limit)
        else:
            results = []

        # Convert to graph format
        nodes = []
        edges = []

        for work in results:
            node = {
                "id": work["work_id"],
                "label": work["title"],
                "type": "work",
                "properties": {
                    "title": work["title"],
                    "creators": work.get("creators", []),
                    "publishers": work.get("publishers", []),
                    "magazines": work.get("magazines", []),
                    "genre": work.get("genre"),
                    "isbn": work.get("isbn"),
                    "volume": work.get("volume"),
                    "published_date": work.get("published_date"),
                    "similarity_score": work.get("similarity_score"),
                    "search_score": work.get("search_score"),
                    "source": "neo4j",
                },
            }
            nodes.append(node)

        return GraphResponse(nodes=nodes, edges=edges, total_nodes=len(nodes), total_edges=len(edges))

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@neo4j_router.post("/vector/add-embedding")
async def add_embedding_to_work(
    request: AddEmbeddingRequest,
    neo4j_service: Neo4jMediaArtsService = Depends(get_neo4j_media_arts_service),
):
    """Add embedding vector to a work node"""
    try:
        if neo4j_service is None or neo4j_service.neo4j_repository is None:
            raise HTTPException(status_code=503, detail="Neo4j service not available")

        success = neo4j_service.neo4j_repository.add_embedding_to_work(
            work_id=request.work_id, embedding=request.embedding
        )

        if success:
            return {"message": f"Embedding added successfully to work: {request.work_id}"}
        else:
            raise HTTPException(status_code=404, detail=f"Work not found: {request.work_id}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@neo4j_router.get("/vector/progress")
async def get_vector_progress(
    neo4j_service: Neo4jMediaArtsService = Depends(get_neo4j_media_arts_service),
):
    """Get progress of vector embedding addition"""
    try:
        if neo4j_service is None or neo4j_service.neo4j_repository is None:
            raise HTTPException(status_code=503, detail="Neo4j service not available")

        with neo4j_service.neo4j_repository.driver.session() as session:
            # Works with embeddings
            work_stats = session.run(
                """
                MATCH (w:Work)
                RETURN
                    count(*) as total_works,
                    count(w.embedding) as works_with_embeddings
            """
            ).single()

            # Authors with embeddings
            author_stats = session.run(
                """
                MATCH (a:Author)
                RETURN
                    count(*) as total_authors,
                    count(a.embedding) as authors_with_embeddings
            """
            ).single()

            # Magazines with embeddings
            magazine_stats = session.run(
                """
                MATCH (m:Magazine)
                RETURN
                    count(*) as total_magazines,
                    count(m.embedding) as magazines_with_embeddings
            """
            ).single()

            return {
                "status": "success",
                "data": {
                    "works": {
                        "total": work_stats["total_works"],
                        "with_embeddings": work_stats["works_with_embeddings"],
                        "percentage": (
                            (work_stats["works_with_embeddings"] / work_stats["total_works"] * 100)
                            if work_stats["total_works"] > 0
                            else 0
                        ),
                    },
                    "authors": {
                        "total": author_stats["total_authors"],
                        "with_embeddings": author_stats["authors_with_embeddings"],
                        "percentage": (
                            (author_stats["authors_with_embeddings"] / author_stats["total_authors"] * 100)
                            if author_stats["total_authors"] > 0
                            else 0
                        ),
                    },
                    "magazines": {
                        "total": magazine_stats["total_magazines"],
                        "with_embeddings": magazine_stats["magazines_with_embeddings"],
                        "percentage": (
                            (magazine_stats["magazines_with_embeddings"] / magazine_stats["total_magazines"] * 100)
                            if magazine_stats["total_magazines"] > 0
                            else 0
                        ),
                    },
                },
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@neo4j_router.post("/vector/batch-add")
async def batch_add_embeddings(
    node_type: str = Query("Work", description="Node type to process (Work, Author, Magazine)"),
    limit: int = Query(100, description="Number of nodes to process"),
    neo4j_service: Neo4jMediaArtsService = Depends(get_neo4j_media_arts_service),
):
    """Batch add embeddings to nodes"""
    try:
        if neo4j_service is None or neo4j_service.neo4j_repository is None:
            raise HTTPException(status_code=503, detail="Neo4j service not available")

        # Import here to avoid startup issues
        import hashlib

        def generate_simple_embedding(text: str) -> List[float]:
            """Generate a simple hash-based embedding"""
            hash_input = text.encode("utf-8")
            hashes = []
            for i in range(6):
                hash_obj = hashlib.sha256(hash_input + str(i).encode())
                hash_hex = hash_obj.hexdigest()
                hashes.append(hash_hex)

            embedding = []
            combined_hash = "".join(hashes)

            for i in range(1536):
                char_index = i % len(combined_hash)
                if combined_hash[char_index].isdigit():
                    value = int(combined_hash[char_index]) / 15.0 - 0.5
                else:
                    value = (ord(combined_hash[char_index]) - ord("a") + 10) / 15.0 - 0.5
                value += (i % 100) / 10000.0 - 0.005
                embedding.append(value)

            return embedding

        success_count = 0
        failed_count = 0

        with neo4j_service.neo4j_repository.driver.session() as session:
            if node_type == "Work":
                query = """
                MATCH (w:Work)
                WHERE w.title IS NOT NULL AND w.embedding IS NULL
                RETURN w.id as id, w.title as title, w.genre as genre
                LIMIT $limit
                """
                result = session.run(query, limit=limit)

                for record in result:
                    try:
                        text_parts = [record["title"]]
                        if record["genre"]:
                            text_parts.append(record["genre"])
                        text = " ".join(text_parts)

                        embedding = generate_simple_embedding(text)
                        success = neo4j_service.neo4j_repository.add_embedding_to_work(record["id"], embedding)

                        if success:
                            success_count += 1
                        else:
                            failed_count += 1
                    except Exception:
                        failed_count += 1

            elif node_type == "Author":
                query = """
                MATCH (a:Author)
                WHERE a.name IS NOT NULL AND a.embedding IS NULL
                RETURN a.id as id, a.name as name
                LIMIT $limit
                """
                result = session.run(query, limit=limit)

                for record in result:
                    try:
                        embedding = generate_simple_embedding(record["name"])

                        update_query = """
                        MATCH (a:Author {id: $id})
                        SET a.embedding = $embedding
                        RETURN a.id
                        """
                        update_result = session.run(update_query, id=record["id"], embedding=embedding)

                        if update_result.single():
                            success_count += 1
                        else:
                            failed_count += 1
                    except Exception:
                        failed_count += 1

        return {
            "message": f"Batch processing completed for {node_type}",
            "processed": success_count + failed_count,
            "success_count": success_count,
            "failed_count": failed_count,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@neo4j_router.get("/search-fuzzy", response_model=GraphResponse)
async def search_manga_fuzzy(
    q: str = Query(..., description="曖昧検索のクエリテキスト"),
    limit: int = Query(10, description="結果の上限"),
    similarity_threshold: float = Query(0.5, description="類似度の閾値（0.0-1.0）"),
    embedding_method: str = Query("huggingface", description="埋め込み生成方法（hash, huggingface, openai）"),
    neo4j_service: Neo4jMediaArtsService = Depends(get_neo4j_media_arts_service),
):
    """Neo4jのembedding列を使った漫画タイトルの曖昧検索"""
    try:
        if neo4j_service is None or neo4j_service.neo4j_repository is None:
            raise HTTPException(status_code=503, detail="Neo4j service not available")

        # BatchEmbeddingProcessorを使って埋め込みを生成
        from domain.services.batch_embedding_processor import BatchEmbeddingProcessor

        processor = BatchEmbeddingProcessor(
            embedding_method=embedding_method,
            sentence_transformer_model="cl-nagoya/ruri-v3-310m" if embedding_method == "huggingface" else "",
        )

        # クエリテキストの埋め込みを生成
        query_embedding = processor.generate_embedding(q)

        # ベクトル検索を実行
        search_results = neo4j_service.neo4j_repository.search_by_vector(
            embedding=query_embedding,
            label="Work",
            property_name="embedding",
            limit=limit * 2,  # 閾値フィルタリング前に多めに取得
        )

        # 類似度でフィルタリング
        filtered_results = [
            result for result in search_results if result.get("similarity_score", 0) >= similarity_threshold
        ][:limit]

        # GraphResponse形式に変換
        nodes = []
        edges = []

        for work in filtered_results:
            node = {
                "id": work["work_id"],
                "label": work["title"],
                "type": "work",
                "properties": {
                    "title": work["title"],
                    "creators": work.get("creators", []),
                    "publishers": work.get("publishers", []),
                    "magazines": work.get("magazines", []),
                    "genre": work.get("genre"),
                    "isbn": work.get("isbn"),
                    "volume": work.get("volume"),
                    "published_date": work.get("published_date"),
                    "first_published": work.get("first_published"),
                    "last_published": work.get("last_published"),
                    "series_id": work.get("series_id"),
                    "series_name": work.get("series_name"),
                    "similarity_score": work.get("similarity_score"),
                    "source": "neo4j-vector",
                    "search_query": q,
                    "embedding_method": embedding_method,
                },
            }
            nodes.append(node)

            # 作者との関係を表現
            for creator in work.get("creators", []):
                if creator:
                    # 作者ノード
                    author_id = f"author_{creator}"
                    author_node = {
                        "id": author_id,
                        "label": creator,
                        "type": "author",
                        "properties": {
                            "name": creator,
                            "source": "neo4j-vector",
                        },
                    }
                    # 重複チェック（IDベース）
                    if not any(node["id"] == author_id for node in nodes):
                        nodes.append(author_node)

                    # 関係
                    edge = {
                        "id": f"{work['work_id']}_created_by_{creator}",
                        "source": work["work_id"],
                        "target": author_id,
                        "type": "CREATED_BY",
                        "properties": {"relationship": "created_by"},
                    }
                    edges.append(edge)

        return GraphResponse(nodes=nodes, edges=edges, total_nodes=len(nodes), total_edges=len(edges))

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
