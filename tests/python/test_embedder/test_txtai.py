import unittest
from unittest.mock import MagicMock, patch
from src.python.embedder.txtai import TxtaiEmbeddings
from pydantic import ValidationError

class TestTxtaiEmbeddings(unittest.TestCase):

    def setUp(self):
        self.mock_data = [{"_source": {"id": "1", "text": "Hello world"}}]
        self.config = {
            "path": "test-model",
            "content": True,
            "backend": "annoy"
        }

    @patch("src.python.embedder.txtai.EmbEngine")
    @patch("src.python.embedder.txtai.TXTAI_AVAILABLE", True)
    def test_embed_success(self, MockEngine):
        mock_instance = MockEngine.return_value
        mock_instance.transform.return_value = MagicMock(tolist=lambda: [0.1, 0.2, 0.3])

        processor = TxtaiEmbeddings(iter(self.mock_data), self.config)
        results = list(processor.embed())

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["_source"]["vector"], [0.1, 0.2, 0.3])

    @patch("src.python.embedder.txtai.TXTAI_AVAILABLE", False)
    def test_import_error_raised(self):
        with self.assertRaises(ImportError):
            TxtaiEmbeddings(iter(self.mock_data), self.config)

    # ADDED MOCKS HERE TO PREVENT NETWORK CALLS
    @patch("src.python.embedder.txtai.EmbEngine")
    @patch("src.python.embedder.txtai.TXTAI_AVAILABLE", True)
    def test_strict_config_error(self, MockEngine):
        bad_config = self.config.copy()
        bad_config["extra_field"] = "not_allowed"
        
        with self.assertRaises(ValidationError):
            TxtaiEmbeddings(iter(self.mock_data), bad_config)

if __name__ == "__main__":
    unittest.main()