"""Helper for loading and reusing the jina-embeddings-v4 SentenceTransformer model."""

from __future__ import annotations

import os
import threading
from typing import List, Optional

import numpy as np
from sentence_transformers import SentenceTransformer


class JinaEmbeddingClient:
    """Singleton-style client that lazily loads the jina embeddings model."""

    _instance: Optional["JinaEmbeddingClient"] = None
    _lock = threading.Lock()

    def __init__(
        self,
        model_name: Optional[str] = None,
        device: Optional[str] = None,
        batch_size: Optional[int] = None,
    ) -> None:
        self.model_name = model_name or os.getenv("JINA_EMBEDDING_MODEL", "jinaai/jina-embeddings-v4")
        self.device = device or os.getenv("JINA_EMBEDDING_DEVICE") or "cpu"
        self.batch_size = batch_size or int(os.getenv("JINA_EMBEDDING_BATCH", "32"))
        self.model = SentenceTransformer(
            self.model_name,
            device=self.device,
            trust_remote_code=True,
            model_kwargs={"default_task": "retrieval"},
        )
        print(f"Loaded JinaEmbeddingClient model '{self.model_name}' on device '{self.device}'")

    @classmethod
    def get_instance(cls) -> "JinaEmbeddingClient":
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
        vector = self.model.encode(
            [normalized],
            batch_size=1,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )[0]
        return vector.astype(np.float32)

    def encode_batch(self, texts: List[str]) -> np.ndarray:
        """Batch encode texts, skipping normalization outside of model."""
        return self.model.encode(
            texts,
            batch_size=self.batch_size,
            convert_to_numpy=True,
            normalize_embeddings=True,
        ).astype(np.float32)

    @staticmethod
    def truncate(vector: np.ndarray, dims: int) -> List[float]:
        if vector is None:
            return []
        if dims <= 0:
            raise ValueError("dims must be positive")
        if dims > vector.shape[-1]:
            raise ValueError("dims exceeds vector length")
        return vector[:dims].astype(np.float32).tolist()


def get_jina_embedding_client() -> JinaEmbeddingClient:
    return JinaEmbeddingClient.get_instance()
