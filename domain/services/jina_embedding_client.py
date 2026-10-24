"""Helper for loading and reusing the jina-embeddings-v4 embeddings.

Supports two modes:
1. API mode (default): Uses Jina AI's hosted API for fast inference
2. Local mode: Uses SentenceTransformer for local inference

Set JINA_EMBEDDING_MODE=local to use local inference.
Get your Jina AI API key for free: https://jina.ai/?sui=apikey
"""

from __future__ import annotations

import os
import threading
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import List, Optional, Union

import httpx
import numpy as np


class BaseEmbeddingClient(ABC):
    """Abstract base class for embedding clients."""

    @abstractmethod
    def encode(self, text: str) -> Optional[np.ndarray]:
        """Return an embedding vector for text or None if empty."""
        pass

    @abstractmethod
    def encode_batch(self, texts: List[str]) -> np.ndarray:
        """Batch encode texts."""
        pass

    @staticmethod
    def truncate(vector: np.ndarray, dims: int) -> List[float]:
        """Truncate vector to specified dimensions (Matryoshka)."""
        if vector is None:
            return []
        if dims <= 0:
            raise ValueError("dims must be positive")
        if dims > vector.shape[-1]:
            raise ValueError("dims exceeds vector length")
        return vector[:dims].astype(np.float32).tolist()


class JinaEmbeddingAPIClient(BaseEmbeddingClient):
    """Client that uses Jina AI's hosted Embedding API for fast inference.

    This is the recommended mode for production as it:
    - Requires no local GPU/CPU resources for model inference
    - Has no cold start latency from model loading
    - Provides consistent low-latency responses (~100-300ms)
    """

    _instance: Optional["JinaEmbeddingAPIClient"] = None
    _lock = threading.Lock()

    API_URL = "https://api.jina.ai/v1/embeddings"
    DEFAULT_MODEL = "jina-embeddings-v4"
    DEFAULT_TIMEOUT = 30.0

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        self.api_key = api_key or os.getenv("JINA_API_KEY")
        if not self.api_key:
            raise ValueError(
                "JINA_API_KEY environment variable is required for API mode. "
                "Get your free API key at: https://jina.ai/?sui=apikey"
            )
        self.model = model or os.getenv("JINA_API_MODEL", self.DEFAULT_MODEL)
        self.timeout = timeout or float(os.getenv("JINA_API_TIMEOUT", str(self.DEFAULT_TIMEOUT)))
        print(f"Using JinaEmbeddingAPIClient with model '{self.model}' and timeout {self.timeout}s")

        # LRU cache for embeddings
        cache_size = int(os.getenv("JINA_EMBEDDING_CACHE_SIZE", "1024"))
        self._cache: OrderedDict[str, np.ndarray] = OrderedDict()
        self._cache_size = cache_size
        self._cache_lock = threading.Lock()

        print(f"Initialized JinaEmbeddingAPIClient with model '{self.model}'")

    @classmethod
    def get_instance(cls) -> "JinaEmbeddingAPIClient":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def _call_api(self, texts: List[str], task: str = "retrieval.query") -> List[List[float]]:
        """Call Jina Embedding API."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload = {
            "model": self.model,
            "input": texts,
            "task": task,
        }

        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(self.API_URL, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()

        # Extract embeddings from response
        embeddings = [item["embedding"] for item in data["data"]]
        return embeddings

    def encode(self, text: str) -> Optional[np.ndarray]:
        """Return an embedding vector for text or None if empty."""
        normalized = (text or "").strip()
        if not normalized:
            return None

        # Check cache first
        with self._cache_lock:
            if normalized in self._cache:
                vec = self._cache.pop(normalized)
                self._cache[normalized] = vec
                return vec.copy()

        start = time.time()
        try:
            embeddings = self._call_api([normalized])
            arr = np.array(embeddings[0], dtype=np.float32)
        except httpx.HTTPStatusError as e:
            print(f"JinaEmbeddingAPIClient.encode: API error - {e}")
            raise
        except Exception as e:
            print(f"JinaEmbeddingAPIClient.encode: Error - {e}")
            raise

        duration = time.time() - start
        print(f"JinaEmbeddingAPIClient.encode: API call for '{normalized[:40]}' in {duration:.3f}s")

        # Update cache
        with self._cache_lock:
            self._cache[normalized] = arr
            while len(self._cache) > self._cache_size:
                self._cache.popitem(last=False)

        return arr.copy()

    def encode_batch(self, texts: List[str]) -> np.ndarray:
        """Batch encode texts using the API."""
        if not texts:
            return np.array([], dtype=np.float32)

        start = time.time()
        embeddings = self._call_api(texts)
        duration = time.time() - start
        print(f"JinaEmbeddingAPIClient.encode_batch: API call for {len(texts)} texts in {duration:.3f}s")

        return np.array(embeddings, dtype=np.float32)


class JinaEmbeddingLocalClient(BaseEmbeddingClient):
    """Singleton-style client that lazily loads the jina embeddings model locally.

    This mode is useful for:
    - Development/testing without API costs
    - Offline environments
    - High-volume batch processing where API costs are a concern

    Note: Requires significant CPU/GPU resources and has cold start latency.
    """

    _instance: Optional["JinaEmbeddingLocalClient"] = None
    _lock = threading.Lock()

    def __init__(
        self,
        model_name: Optional[str] = None,
        device: Optional[str] = None,
        batch_size: Optional[int] = None,
    ) -> None:
        # Lazy import to avoid loading SentenceTransformer when using API mode
        from sentence_transformers import SentenceTransformer

        self.model_name = model_name or os.getenv("JINA_EMBEDDING_MODEL", "jinaai/jina-embeddings-v4")
        self.device = device or os.getenv("JINA_EMBEDDING_DEVICE") or "cpu"
        self.batch_size = batch_size or int(os.getenv("JINA_EMBEDDING_BATCH", "32"))
        self.model = SentenceTransformer(
            self.model_name,
            device=self.device,
            trust_remote_code=True,
            model_kwargs={"default_task": "retrieval"},
        )
        print(f"Loaded JinaEmbeddingLocalClient model '{self.model_name}' on device '{self.device}'")
        # Simple in-memory LRU cache for embeddings to avoid repeated expensive encodes
        cache_size = int(os.getenv("JINA_EMBEDDING_CACHE_SIZE", "1024"))
        self._cache: OrderedDict[str, np.ndarray] = OrderedDict()
        self._cache_size = cache_size
        self._cache_lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "JinaEmbeddingLocalClient":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def encode(self, text: str) -> Optional[np.ndarray]:
        """Return a 2048-dim embedding for text or None if empty."""
        normalized = (text or "").strip()
        if not normalized:
            return None
        # Check cache first
        key = normalized
        with self._cache_lock:
            if key in self._cache:
                # move to end (most recently used)
                vec = self._cache.pop(key)
                self._cache[key] = vec
                return vec.copy()

        start = time.time()
        vector = self.model.encode(
            [normalized],
            batch_size=1,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )[0]
        duration = time.time() - start
        print(f"JinaEmbeddingLocalClient.encode: computed embedding for '{normalized[:40]}' in {duration:.3f}s")

        arr = vector.astype(np.float32)
        with self._cache_lock:
            # insert into cache
            self._cache[key] = arr
            # enforce size
            while len(self._cache) > self._cache_size:
                self._cache.popitem(last=False)

        return arr.copy()

    def encode_batch(self, texts: List[str]) -> np.ndarray:
        """Batch encode texts, skipping normalization outside of model."""
        return self.model.encode(
            texts,
            batch_size=self.batch_size,
            convert_to_numpy=True,
            normalize_embeddings=True,
        ).astype(np.float32)


# Type alias for backward compatibility
JinaEmbeddingClient = Union[JinaEmbeddingAPIClient, JinaEmbeddingLocalClient]


def get_jina_embedding_client(jina_embedding_mode: Optional[str] = "api") -> BaseEmbeddingClient:
    """Factory function to get the appropriate Jina embedding client.

    Returns API client by default. Set JINA_EMBEDDING_MODE=local to use local inference.

    Environment variables:
    - JINA_EMBEDDING_MODE: 'api' (default) or 'local'
    - JINA_API_KEY: Required for API mode (get free key at https://jina.ai/?sui=apikey)
    - JINA_API_MODEL: Model for API mode (default: jina-embeddings-v4)
    - JINA_API_TIMEOUT: Timeout for API calls in seconds (default: 30)
    - JINA_EMBEDDING_MODEL: Model for local mode (default: jinaai/jina-embeddings-v4)
    - JINA_EMBEDDING_DEVICE: Device for local mode (default: cpu)
    - JINA_EMBEDDING_CACHE_SIZE: LRU cache size (default: 1024)
    """
    mode = (jina_embedding_mode or os.getenv("JINA_EMBEDDING_MODE", "api")).lower()

    if mode == "local":
        return JinaEmbeddingLocalClient.get_instance()
    else:
        return JinaEmbeddingAPIClient.get_instance()
