from unittest.mock import MagicMock, patch
import pytest
from google.genai.errors import APIError

from src.rag.embedders.gemini import GeminiQueryEmbedder, EmbedderQueryConfig


@pytest.fixture
def embedder():
    config = EmbedderQueryConfig(api_key="fake-api-key", model_name="text-embedding-001")
    with patch("src.rag.embedders.gemini.genai.Client"):
        instance = GeminiQueryEmbedder(config)
        return instance


def test_embed_query_retry_success(embedder):
    """Test that embed_query successfully retries on APIError and succeeds on a later attempt."""
    mock_response = MagicMock()
    mock_response.embeddings = [MagicMock(values=[0.1, 0.2, 0.3])]

    # Pass response_json={} to satisfy APIError init
    embedder.client.models.embed_content = MagicMock(
        side_effect=[
            APIError("Transient rate limit error", response_json={}),
            APIError("Transient rate limit error", response_json={}),
            mock_response,
        ]
    )

    vector = embedder.embed_query("Hello world")

    assert vector == [0.1, 0.2, 0.3]
    assert embedder.client.models.embed_content.call_count == 3


def test_embed_query_exhausts_retries(embedder):
    """Test that embed_query raises APIError after exhausting all max retry attempts."""
    embedder.client.models.embed_content = MagicMock(
        side_effect=APIError("Persistent rate limit error", response_json={})
    )

    with pytest.raises(APIError):
        embedder.embed_query("Hello world")

    assert embedder.client.models.embed_content.call_count == 3