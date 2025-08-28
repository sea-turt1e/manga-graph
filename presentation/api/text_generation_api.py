from typing import AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from domain.services.text_generation_service import TextGenerationService, get_text_generation_service
from domain.use_cases.generate_text import GenerateTextUseCase
from presentation.schemas.text_generation_schemas import TextGenerationRequest

router = APIRouter(prefix="/text-generation", tags=["Text Generation"])


async def get_generate_text_use_case(
    text_generation_service: TextGenerationService = Depends(get_text_generation_service),
) -> GenerateTextUseCase:
    """テキスト生成ユースケースの依存性注入"""
    return GenerateTextUseCase(text_generation_service)


@router.post("/generate", summary="テキスト生成", description="OpenAI APIを使用してテキストをストリーミング生成します")
async def generate_text(
    request: TextGenerationRequest, use_case: GenerateTextUseCase = Depends(get_generate_text_use_case)
):
    """テキストを生成してストリーミングで返す"""
    try:

        async def generate_stream() -> AsyncGenerator[str, None]:
            async for chunk in use_case.execute(
                text=request.text, max_tokens=request.max_tokens, temperature=request.temperature, model=request.model
            ):
                yield f"{chunk}\n\n"

        return StreamingResponse(
            generate_stream(),
            media_type="text/plain",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Text generation failed: {str(e)}")


# エクスポート用
text_generation_router = router
