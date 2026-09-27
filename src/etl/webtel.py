import os
import yaml
from dotenv import load_dotenv

from src.etl.core.config import AppConfig
from src.etl.core.exceptions import BaseETLException
from src.etl.core.logger import setup_logger
from src.etl.domain.entities import Document
from src.etl.infrastructure.transformers import SentenceSplitterTransformer
from src.etl.infrastructure.embedders import GeminiEmbedder
from src.etl.infrastructure.loaders import QdrantVectorLoader

logger = setup_logger(__name__)
load_dotenv()


def run_local_ingestion(input_dir: str = "static/extracted_content") -> None:
    """Reads saved text files, transforms, embeds, and loads them into Qdrant Cloud."""
    try:
        config_path = "etl_config.yml"
        app_config = AppConfig.load_from_yaml(config_path)
        api_key = os.getenv("GEMINI_API_KEY")

        if not api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required.")

        if not os.path.exists(input_dir):
            raise FileNotFoundError(f"Directory '{input_dir}' not found.")

        # 1. Parse saved text files into Document entities
        documents = []
        for file_name in os.listdir(input_dir):
            if not file_name.endswith(".txt"):
                continue

            file_path = os.path.join(input_dir, file_name)
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            source_url = ""
            tenant_id = app_config.aws.tenant_id
            content_start_idx = 0

            for idx, line in enumerate(lines):
                if line.startswith("Source URL:"):
                    source_url = line.replace("Source URL:", "").strip()
                elif line.startswith("Tenant ID:"):
                    tenant_id = line.replace("Tenant ID:", "").strip()
                elif "---" in line:
                    content_start_idx = idx + 1
                    break

            content = "".join(lines[content_start_idx:]).strip()
            if content:
                documents.append(
                    Document(
                        content=content,
                        metadata={
                            "source_key": source_url,
                            "tenant_id": tenant_id,
                            "content_type": "text/plain",
                        },
                    )
                )

        logger.info(f"Loaded {len(documents)} documents from '{input_dir}/'.")
        if not documents:
            return

        # 2. Transform into chunks
        transformer = SentenceSplitterTransformer(app_config.transformer)
        chunk_stream = (chunk for doc in documents for chunk in transformer.transform(doc))

        # 3. Embed & Load into Qdrant Cloud
        embedder = GeminiEmbedder(api_key=api_key, config=app_config.embedder)
        loader = QdrantVectorLoader(app_config.qdrant)

        total_chunks = 0
        for batch_idx, batch in enumerate(embedder.embed_chunks(chunk_stream)):
            loaded_count = loader.load_batch(batch)
            total_chunks += loaded_count
            logger.info(f"Batch {batch_idx + 1}: Loaded {loaded_count} points to Qdrant.")

        logger.info(f"Pipeline completed! Total points loaded: {total_chunks}")

    except BaseETLException as e:
        logger.critical(f"Ingestion failed: {str(e)}")
        raise
    except Exception as e:
        logger.critical(f"Unexpected error: {str(e)}")
        raise


if __name__ == "__main__":
    run_local_ingestion()