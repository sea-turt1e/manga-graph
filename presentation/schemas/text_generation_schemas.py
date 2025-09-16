from typing import Optional

from pydantic import BaseModel, Field


class TextGenerationRequest(BaseModel):
    """テキスト生成リクエストスキーマ"""

    text: str = Field(..., description="生成の元となるテキスト", min_length=1)
    max_tokens: Optional[int] = Field(1000, description="最大トークン数", ge=1, le=4000)
    temperature: Optional[float] = Field(0.7, description="温度パラメータ", ge=0.0, le=2.0)
    model: Optional[str] = Field("gpt-4o-mini", description="使用するモデル")
    streaming: Optional[bool] = Field(
        True, description="ストリーミングで返す場合は True。デフォルト True でストリーミング"
    )


class TextGenerationResponse(BaseModel):
    """テキスト生成レスポンススキーマ"""

    generated_text: str = Field(..., description="生成されたテキスト")
    model: str = Field(..., description="使用されたモデル")
    usage_tokens: Optional[int] = Field(None, description="使用されたトークン数")
