from .manga_api import (cover_router, image_router, manga_anime_router,
                        media_arts_router, neo4j_router)
from .manga_api import router as manga_router
from .text_generation_api import text_generation_router

__all__ = [
    "manga_router",
    "media_arts_router",
    "neo4j_router",
    "manga_anime_router",
    "cover_router",
    "image_router",
    "text_generation_router",
]
