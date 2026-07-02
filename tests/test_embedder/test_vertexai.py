# Embedder testing with sample data

# def test():
#     embedder = VertexAIEmbedder(
#         project_id="gen-lang-client-0391985596", 
#         location="us-central1", 
#         model_name="text-multilingual-embedding-002"
#     )
    
#     # Pass them as individual strings
#     print(embedder.embed("Welcome to the data pipeline engineering module."))
#     print(embedder.embed("முயற்சி திருவினை ஆக்கும் முயற்சிற்றின் இன்மை புகுத்தி விடும்."))

# test()

# Embedder testing with mock data

import pytest
from unittest.mock import MagicMock, patch
from embedder.vertexai import VertexAIEmbedder


@pytest.fixture
def mock_vertex_components():
    """Fixture to mock Vertex AI initialization and model loading."""
    with patch("embedder.vertexai.vertexai.init") as mock_init, \
         patch("embedder.vertexai.TextEmbeddingModel.from_pretrained") as mock_from_pretrained:
        
        # Setup a mock model instance
        mock_model_instance = MagicMock()
        mock_from_pretrained.return_value = mock_model_instance
        
        yield mock_init, mock_from_pretrained, mock_model_instance


def test_init_success(mock_vertex_components):
    """Test successful initialization of the VertexAIEmbedder."""
    mock_init, mock_from_pretrained, _ = mock_vertex_components
    
    embedder = VertexAIEmbedder(
        project_id="test-project", 
        location="us-central1", 
        model_name="text-multilingual-embedding-002"
    )
    
    mock_init.assert_called_once_with(project="test-project", location="us-central1")
    mock_from_pretrained.assert_called_once_with("text-multilingual-embedding-002")
    assert embedder.model is not None


def test_init_failure(mock_vertex_components):
    """Test initialization failure when Vertex AI crashes."""
    _, mock_from_pretrained, _ = mock_vertex_components
    mock_from_pretrained.side_effect = Exception("Auth connection error")
    
    with pytest.raises(Exception, match="Auth connection error"):
        VertexAIEmbedder("test-project", "us-central1", "invalid-model")


def test_embed_success(mock_vertex_components):
    """Test successful vector extraction from a text string."""
    _, _, mock_model_instance = mock_vertex_components
    
    # Mock the return structure of model.get_embeddings()
    mock_embedding_obj = MagicMock()
    mock_embedding_obj.values = [0.1, 0.2, 0.3]
    mock_model_instance.get_embeddings.return_value = [mock_embedding_obj]
    
    embedder = VertexAIEmbedder("test-project", "us-central1", "text-multilingual-embedding-002")
    vector = embedder.embed("வணக்கம்")
    
    mock_model_instance.get_embeddings.assert_called_once_with(["வணக்கம்"])
    assert vector == [0.1, 0.2, 0.3]


def test_embed_api_failure(mock_vertex_components):
    """Test handling of an API call crash during the embedding process."""
    _, _, mock_model_instance = mock_vertex_components
    mock_model_instance.get_embeddings.side_effect = Exception("API Timeout")
    
    embedder = VertexAIEmbedder("test-project", "us-central1", "text-multilingual-embedding-002")
    
    with pytest.raises(RuntimeError, match="Vertex Cloud Engine error"):
        embedder.embed("Hello World")