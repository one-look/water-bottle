import os
from dotenv import load_dotenv

from src.etl.core.config import AppConfig
from src.etl.core.exceptions import BaseETLException
from src.etl.core.logger import setup_logger
from src.etl.infrastructure.embedders import GeminiEmbedder
from src.etl.infrastructure.extractors import S3DocumentExtractor
from src.etl.infrastructure.loaders import QdrantVectorLoader
from src.etl.infrastructure.transformers import SentenceSplitterTransformer

logger = setup_logger(__name__)

load_dotenv()


def run_pipeline(source_key: str = "tnedu.pdf") -> None:
    """Orchestrates streaming ETL from S3 extraction to Qdrant vector loading.

    Args:
        source_key: Target object key name inside tenant's S3 path prefix.

    Returns:
        None

    Raises:
        BaseETLException: Propagates component error boundaries.
    """
    try:
        config = AppConfig.load_from_yaml("etl_config.yml")
        api_key = os.getenv("GEMINI_API_KEY")

        if not api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required.")

        # 1. Extract raw PDF bytes & text from AWS S3
        extractor = S3DocumentExtractor(config.aws)
        document = extractor.extract(
            source_key=source_key, tenant_id=config.aws.tenant_id
        )

        # 2. Transform raw text into chunk stream
        transformer = SentenceSplitterTransformer(config.transformer)
        chunk_stream = transformer.transform(document)

        # 3. Embed & Load streams directly into Qdrant
        embedder = GeminiEmbedder(api_key=api_key, config=config.embedder)
        loader = QdrantVectorLoader(config.qdrant)

        embedded_batch_stream = embedder.embed_chunks(chunk_stream)
        total_chunks = 0

        for batch_idx, batch in enumerate(embedded_batch_stream):
            loaded_count = loader.load_batch(batch)
            total_chunks += loaded_count
            logger.info(
                f"Batch {batch_idx + 1}: Embedded & loaded {loaded_count} points to Qdrant."
            )

        logger.info(
            f"Pipeline completed successfully. Total points loaded into Qdrant: {total_chunks}"
        )

    except BaseETLException as e:
        logger.critical(f"ETL Pipeline execution failed: {str(e)}")
        raise
    except Exception as e:
        logger.critical(f"Unexpected error in ETL execution: {str(e)}")
        raise


if __name__ == "__main__":
    run_pipeline()