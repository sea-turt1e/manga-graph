from unittest.mock import AsyncMock

import pytest

from domain.entities.text_generation import TextGenerationRequest
from domain.use_cases.generate_text import GenerateTextUseCase


class TestGenerateTextUseCase:
    """テキスト生成ユースケースのテスト"""

    @pytest.fixture
    def mock_text_generation_service(self):
        """モックのテキスト生成サービス"""
        service = AsyncMock()

        async def mock_stream():
            yield "Hello"
            yield " "
            yield "World"
            yield "!"

        service.generate_text_stream.return_value = mock_stream()
        return service

    @pytest.fixture
    def use_case(self, mock_text_generation_service):
        """テキスト生成ユースケースのインスタンス"""
        return GenerateTextUseCase(mock_text_generation_service)

    @pytest.mark.asyncio
    async def test_execute_success(self, use_case, mock_text_generation_service):
        """正常なテキスト生成のテスト"""
        # 実行
        result_chunks = []
        async for chunk in use_case.execute("Test input"):
            result_chunks.append(chunk)

        # 検証
        assert result_chunks == ["Hello", " ", "World", "!"]
        mock_text_generation_service.generate_text_stream.assert_called_once()

        # 呼び出し引数の検証
        call_args = mock_text_generation_service.generate_text_stream.call_args[0][0]
        assert isinstance(call_args, TextGenerationRequest)
        assert call_args.text == "Test input"
        assert call_args.max_tokens == 1000
        assert call_args.temperature == 0.7
        assert call_args.model == "gpt-4o-mini"

    @pytest.mark.asyncio
    async def test_execute_with_custom_parameters(self, use_case, mock_text_generation_service):
        """カスタムパラメータでのテキスト生成のテスト"""
        # 実行
        result_chunks = []
        async for chunk in use_case.execute(text="Custom input", max_tokens=500, temperature=0.5, model="gpt-4o-mini"):
            result_chunks.append(chunk)

        # 検証
        call_args = mock_text_generation_service.generate_text_stream.call_args[0][0]
        assert call_args.text == "Custom input"
        assert call_args.max_tokens == 500
        assert call_args.temperature == 0.5
        assert call_args.model == "gpt-4o-mini"


class TestTextGenerationRequest:
    """テキスト生成リクエストエンティティのテスト"""

    def test_default_values(self):
        """デフォルト値のテスト"""
        request = TextGenerationRequest(text="Test")
        assert request.text == "Test"
        assert request.max_tokens == 1000
        assert request.temperature == 0.7
        assert request.model == "gpt-4o-mini"

    def test_custom_values(self):
        """カスタム値のテスト"""
        request = TextGenerationRequest(text="Custom text", max_tokens=500, temperature=0.5, model="gpt-4o-mini")
        assert request.text == "Custom text"
        assert request.max_tokens == 500
        assert request.temperature == 0.5
        assert request.model == "gpt-4o-mini"
