from fastapi import APIRouter, HTTPException, Depends
from typing import List
from presentation.schemas import (
    SearchRequest, GraphResponse, AuthorResponse, 
    WorkResponse, MagazineResponse, NodeData, EdgeData
)
from domain.use_cases import SearchMangaUseCase
from infrastructure.database import Neo4jMangaRepository


router = APIRouter(prefix="/api/v1", tags=["manga"])


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


@router.post("/search", response_model=GraphResponse)
async def search_manga(
    request: SearchRequest,
    use_case: SearchMangaUseCase = Depends(get_search_manga_use_case)
):
    """Search for manga and related data"""
    try:
        if use_case is None:
            # Mock response for testing
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
        
        # Execute use case
        graph_data = use_case.execute(
            query=request.query,
            depth=request.depth,
            node_types=request.node_types,
            edge_types=request.edge_types
        )
        
        return GraphResponse(
            nodes=graph_data["nodes"],
            edges=graph_data["edges"],
            total_nodes=len(graph_data["nodes"]),
            total_edges=len(graph_data["edges"])
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
                biography=author.biography
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
                description=work.description
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
                established_date=magazine.established_date.isoformat() if magazine.established_date else None
            )
            for magazine in magazines
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))