import asyncio
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List
import numpy as np
import random
from .constants import (
    TITAN_V1,
    TITAN_V2,
    EMBEDDING,
    NORMALIZE,
    APPLICATION_JSON,
    MAX_RETRIES,
    RETRY_DELAY,
)


# -------------------- Logger --------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# -------------------- Base Class ----------------
class Embedding(ABC):
    """Base class for embedding models.

    Args:
        ABC (type): Abstract base class for all embedding models.
    """

    def __init__(self, session, **kwargs):
        self.__session = session
        self.max_retries = kwargs.get("max_retries", MAX_RETRIES)
        self.retry_delay = kwargs.get("retry_delay", RETRY_DELAY)

    @abstractmethod
    async def generate_embedding(self, payload: Dict) -> List[float]:
        """Generate an embedding vector.
        Must be implemented by concrete adapters.

        Args:
            payload (Dict): Input payload for generating embeddings.

        Returns:
            List[float]: The generated embedding vector.
        """
        pass

    async def invoke_with_retry(self, model_id, payload: Dict) -> List[float]:
        """
        Invoke the model with retries.

        Args:
            model_id (str): The ID of the model to invoke.
            payload (Dict): The input payload for the model.
        Returns:
            List[float]: The embedding vector returned from the model invocation.
        """
        attempt = 0
        while attempt < self.max_retries:
            try:
                response = await self.__session.invoke_model(
                    modelId=model_id,
                    body=json.dumps(payload),
                    contentType=APPLICATION_JSON,
                    accept=APPLICATION_JSON,
                )
                body_content = await response["body"].read()
                result = json.loads(body_content)
                embedding_vector = result[EMBEDDING]
                return embedding_vector
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                attempt += 1
                if attempt < self.max_retries:
                    # Add random jitter to avoid thundering herd problem
                    base_delay = self.retry_delay * (2 ** (attempt - 1))
                    jitter = random.uniform(0, self.retry_delay)
                    await asyncio.sleep(base_delay + jitter)
        raise RuntimeError(
            f"Failed to invoke model {model_id} after {self.max_retries} attempts"
        )


class TitanV1(Embedding):
    """
    Adapter for Titan v1 API (embeddings)
    """

    model_id: str = TITAN_V1

    def __init__(self, session, **kwargs):
        super().__init__(session, **kwargs)

    def _normalise_vector(self, vector) -> List[float]:
        """
        Normalize the embedding vector.
        Args:
            vector (np.array): The embedding vector to normalize.

        """
        norm = np.linalg.norm(vector)
        if norm == 0:
            return vector.tolist()
        return (vector / norm).tolist()

    async def generate_embedding(
        self,
        payload: Dict = None,
    ) -> List[float]:
        """
        Generate an embedding vector.

        Args:
            payload (Dict): Input payload for generating embeddings.
            kwargs: Additional keyword arguments.
        Returns:
            Dict: The generated embedding vector.
        """
        normalize = False
        if payload is None:
            payload = {}
        if NORMALIZE in payload:
            normalize = payload.pop(NORMALIZE)
        embedding_vector = await self.invoke_with_retry(TitanV1.model_id, payload)
        if normalize:
            embedding_vector = self._normalise_vector(np.array(embedding_vector))
        return embedding_vector


class TitanV2(Embedding):
    """
    Adapter for Titan v2 API (embeddings)
    """

    model_id: str = TITAN_V2

    def __init__(self, session, **kwargs):
        super().__init__(session, **kwargs)

    async def generate_embedding(
        self,
        payload: Dict = None,
    ) -> List[float]:
        """
        Generate an embedding vector.

        Args:
            payload (Dict): Input payload for generating embeddings.
        Returns:
            Dict: The generated embedding vector.
        """
        if payload is None:
            payload = {}
        embedding_vector = await self.invoke_with_retry(TitanV2.model_id, payload)
        return embedding_vector
