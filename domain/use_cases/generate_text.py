from typing import AsyncGenerator

from domain.entities.text_generation import TextGenerationRequest
from domain.services.text_generation_service import TextGenerationService


class GenerateTextUseCase:
    """テキスト生成ユースケース"""

    def __init__(self, text_generation_service: TextGenerationService):
        self.text_generation_service = text_generation_service

    async def execute(
        self, text: str, max_tokens: int = 1000, temperature: float = 0.7, model: str = "gpt-4o-mini"
    ) -> AsyncGenerator[str, None]:
        """テキスト生成を実行する"""
        request = TextGenerationRequest(text=text, max_tokens=max_tokens, temperature=temperature, model=model)
        stream = self.text_generation_service.generate_text_stream(request)
        # AsyncMock などで coroutine が返るケースを吸収
        if not hasattr(stream, "__aiter__"):
            stream = await stream  # type: ignore[assignment]
        async for chunk in stream:  # type: ignore[misc]
            yield chunk
