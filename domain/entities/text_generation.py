from dataclasses import dataclass
from typing import Optional


@dataclass
class TextGenerationRequest:
    """テキスト生成リクエストのドメインエンティティ"""

    text: str
    max_tokens: Optional[int] = 1000
    temperature: Optional[float] = 0.7
    model: Optional[str] = "gpt-4o-mini"


@dataclass
class TextGenerationResponse:
    """テキスト生成レスポンスのドメインエンティティ"""

    generated_text: str
    model: str
    usage_tokens: Optional[int] = None
