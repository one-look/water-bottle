import os
from dotenv import load_dotenv
from src.etl.core.config import AppConfig
from src.etl.core.logger import setup_logger
from src.etl.infrastructure.embedders import GeminiEmbedder
from src.etl.infrastructure.extractors import S3DocumentExtractor
from src.etl.infrastructure.transformers import SentenceSplitterTransformer

logger = setup_logger(__name__)

load_dotenv()

def run_pipeline(source_key: str):
    config = AppConfig.load_from_yaml("etl_config.yml")
    api_key = os.getenv("GEMINI_API_KEY")

    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable is required.")

    # 1. Extract
    extractor = S3DocumentExtractor(config.aws)
    document = extractor.extract(source_key=source_key, tenant_id=config.aws.tenant_id)

    # 2. Transform (Generator/Stream)
    transformer = SentenceSplitterTransformer(config.transformer)
    chunk_stream = transformer.transform(document)

    # 3. Embed (Batched & Rate-limited Stream)
    embedder = GeminiEmbedder(api_key=api_key, config=config.embedder)
    embedded_batch_stream = embedder.embed_chunks(chunk_stream)

    total_chunks = 0
    for batch_idx, batch in enumerate(embedded_batch_stream):
        total_chunks += len(batch)
        logger.info(
            f"Processed batch {batch_idx + 1}: {len(batch)} chunks embedded. Sample vector length: {len(batch[0].embedding or [])}"
        )

    logger.info(f"Pipeline completed successfully. Total chunks embedded: {total_chunks}")


if __name__ == "__main__":
    run_pipeline("tnedu.pdf")