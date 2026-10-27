import json
import os
from typing import List
from dotenv import load_dotenv

from src.etl.core.config import AppConfig
from src.etl.core.exceptions import BaseETLException
from src.etl.core.logger import setup_logger
from src.etl.domain.entities import Document
from src.etl.infrastructure.embedders import GeminiEmbedder
from src.etl.infrastructure.loaders import QdrantVectorLoader
from src.etl.infrastructure.transformers import SentenceSplitterTransformer

logger = setup_logger(__name__)
load_dotenv()


class JsonDocumentParser:
    """Infrastructure component responsible for parsing raw JSON files 
    into core Domain Document entities.
    """

    @staticmethod
    def load_documents_from_directory(input_dir: str, default_tenant_id: str) -> List[Document]:
        documents: List[Document] = []

        if not os.path.exists(input_dir):
            raise FileNotFoundError(f"Input directory '{input_dir}' does not exist.")

        for file_name in os.listdir(input_dir):
            if not file_name.endswith(".json"):
                continue

            file_path = os.path.join(input_dir, file_name)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                department = data.get("department", "General")
                source_url = data.get("metadata", {}).get("source_url", "")
                
                # Extract and map each section to a distinct Document entity
                for section in data.get("sections", []):
                    category = section.get("category", "")
                    title = section.get("title", "")
                    content = section.get("content", "")
                    keywords = section.get("keywords", [])

                    if not content.strip():
                        continue

                    # Prepend metadata context directly to text for embedding alignment
                    enriched_content = (
                        f"Department: {department}\n"
                        f"Category: {category}\n"
                        f"Title: {title}\n\n"
                        f"{content}"
                    )

                    documents.append(
                        Document(
                            content=enriched_content,
                            metadata={
                                "source_key": source_url,
                                "tenant_id": default_tenant_id,
                                "department": department,
                                "category": category,
                                "title": title,
                                "keywords": keywords,
                                "content_type": "application/json",
                            },
                        )
                    )
            except json.JSONDecodeError as err:
                logger.error(f"Failed to decode JSON from file '{file_name}': {str(err)}")
            except Exception as err:
                logger.error(f"Error processing file '{file_name}': {str(err)}")

        return documents


def run_local_ingestion(input_dir: str, config_path: str) -> None:
    """Executor function that orchestrates reading, chunking, embedding, 
    and loading structured department data into Qdrant Cloud.
    """
    try:
        # 1. Environment and Configuration Setup
        app_config = AppConfig.load_from_yaml(config_path)
        api_key = os.getenv("GEMINI_API_KEY")

        if not api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required.")

        # 2. Extract & Parse JSON files to Domain Entities
        logger.info(f"Scanning directory '{input_dir}' for JSON documents...")
        documents = JsonDocumentParser.load_documents_from_directory(
            input_dir=input_dir,
            default_tenant_id=app_config.aws.tenant_id
        )

        logger.info(f"Loaded {len(documents)} document sections into domain entities.")
        if not documents:
            logger.warning("No valid documents found to ingest. Terminating pipeline execution.")
            return

        # 3. Domain Transformation (Chunking)
        transformer = SentenceSplitterTransformer(app_config.transformer)
        chunk_stream = (chunk for doc in documents for chunk in transformer.transform(doc))

        # 4. Infrastructure Adapters: Embedding & Loading
        embedder = GeminiEmbedder(api_key=api_key, config=app_config.embedder)
        loader = QdrantVectorLoader(app_config.qdrant)

        logger.info(f"Starting vector ingestion targeting collection: '{app_config.qdrant.collection_name}'")
        total_chunks_loaded = 0

        for batch_idx, batch in enumerate(embedder.embed_chunks(chunk_stream)):
            loaded_count = loader.load_batch(batch)
            total_chunks_loaded += loaded_count
            logger.info(f"Batch {batch_idx + 1}: Successfully indexed {loaded_count} points to Qdrant.")

        logger.info(f"Pipeline execution finished successfully! Total vectors indexed: {total_chunks_loaded}")

    except BaseETLException as e:
        logger.critical(f"Domain/ETL Exception encountered: {str(e)}")
        raise
    except Exception as e:
        logger.critical(f"Unhandled system failure during execution: {str(e)}")
        raise


if __name__ == "__main__":
    run_local_ingestion("static/data", "etl_config.yml")