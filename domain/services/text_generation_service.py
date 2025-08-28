import os
from abc import ABC, abstractmethod
from typing import AsyncGenerator, List

import openai
from openai import AsyncOpenAI

from domain.entities.text_generation import TextGenerationRequest


class TextGenerationService(ABC):
    """テキスト生成サービスの抽象クラス"""

    @abstractmethod
    async def generate_text_stream(self, request: TextGenerationRequest) -> AsyncGenerator[str, None]:
        """ストリーミングでテキストを生成する"""
        pass

    async def generate_text(self, request: TextGenerationRequest) -> str:
        """非ストリーミングで全文を取得 (デフォルト実装はストリーム集約)"""
        chunks: List[str] = []
        async for c in self.generate_text_stream(request):
            if c:
                chunks.append(c)
        return "".join(chunks)


class OpenAITextGenerationService(TextGenerationService):
    """OpenAI APIを使用したテキスト生成サービス"""

    def __init__(self):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        self.client = AsyncOpenAI(api_key=api_key)

    async def generate_text_stream(self, request: TextGenerationRequest) -> AsyncGenerator[str, None]:
        """ストリーミングでテキストを生成する"""
        try:
            stream = await self.client.chat.completions.create(
                model=request.model,
                messages=[{"role": "user", "content": request.text}],
                max_tokens=request.max_tokens,
                temperature=request.temperature,
                stream=True,
            )

            async for chunk in stream:
                if chunk.choices[0].delta.content is not None:
                    yield chunk.choices[0].delta.content

        except openai.OpenAIError as e:
            raise Exception(f"OpenAI API error: {str(e)}")
        except Exception as e:
            raise Exception(f"Text generation error: {str(e)}")


def get_text_generation_service() -> TextGenerationService:
    """テキスト生成サービスのファクトリ関数"""
    return OpenAITextGenerationService()
