from typing import AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse

from domain.services.text_generation_service import TextGenerationService, get_text_generation_service
from domain.use_cases.generate_text import GenerateTextUseCase
from presentation.schemas.text_generation_schemas import TextGenerationRequest

router = APIRouter(prefix="/text-generation", tags=["Text Generation"])


async def get_generate_text_use_case(
    text_generation_service: TextGenerationService = Depends(get_text_generation_service),
) -> GenerateTextUseCase:
    """テキスト生成ユースケースの依存性注入"""
    return GenerateTextUseCase(text_generation_service)


@router.post(
    "/generate",
    summary="テキスト生成",
    description="OpenAI APIを使用してテキストを生成します。streaming=True でストリーミング、False で一括返却",
)
async def generate_text(
    request: TextGenerationRequest, use_case: GenerateTextUseCase = Depends(get_generate_text_use_case)
):
    """テキストを生成し streaming フラグに応じて返す (デフォルト: 一括返却)"""
    try:
        # ストリーミングレスポンス
        if request.streaming:

            async def generate_stream() -> AsyncGenerator[str, None]:
                async for chunk in use_case.execute(
                    text=request.text,
                    max_tokens=request.max_tokens,
                    temperature=request.temperature,
                    model=request.model,
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

        # 非ストリーミング: すべて受信して JSON で返却
        aggregated = []
        async for chunk in use_case.execute(
            text=request.text,
            max_tokens=request.max_tokens,
            temperature=request.temperature,
            model=request.model,
        ):
            aggregated.append(chunk)
        full_text = "".join(aggregated)
        return JSONResponse({"generated_text": full_text, "model": request.model})

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Text generation failed: {str(e)}")


# エクスポート用
text_generation_router = router
