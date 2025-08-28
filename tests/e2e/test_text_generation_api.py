from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from main import app


class TestTextGenerationAPI:
    """テキスト生成APIのE2Eテスト"""

    @pytest.fixture
    def client(self):
        """テストクライアント"""
        return TestClient(app)

    @patch("domain.services.text_generation_service.OpenAITextGenerationService")
    def test_generate_text_success(self, mock_service_class, client):
        """正常なテキスト生成APIのテスト"""
        # モックの設定
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service

        async def mock_stream():
            yield "Hello"
            yield " "
            yield "World"
            yield "!"

        mock_service.generate_text_stream.return_value = mock_stream()

        # リクエスト
        response = client.post(
            "/text-generation/generate",
            json={"text": "Say hello to the world", "max_tokens": 100, "temperature": 0.7, "model": "gpt-4o-mini"},
        )

        # 検証
        assert response.status_code == 200
        assert response.headers["content-type"] == "text/plain; charset=utf-8"

    def test_generate_text_invalid_request(self, client):
        """不正なリクエストのテスト"""
        # 空のテキストでリクエスト
        response = client.post("/text-generation/generate", json={"text": "", "max_tokens": 100})

        # 検証
        assert response.status_code == 422  # Validation Error

    def test_generate_text_missing_text(self, client):
        """テキストが欠落したリクエストのテスト"""
        response = client.post("/text-generation/generate", json={"max_tokens": 100})

        # 検証
        assert response.status_code == 422  # Validation Error

    def test_generate_text_invalid_parameters(self, client):
        """無効なパラメータのテスト"""
        # 無効なmax_tokens
        response = client.post("/text-generation/generate", json={"text": "Test", "max_tokens": -1})
        assert response.status_code == 422

        # 無効なtemperature
        response = client.post("/text-generation/generate", json={"text": "Test", "temperature": 3.0})
        assert response.status_code == 422
