import hashlib
import os
import yaml
from dotenv import load_dotenv

from src.etl.core.config import AppConfig
from src.etl.core.exceptions import BaseETLException
from src.etl.core.logger import setup_logger
from src.etl.infrastructure.extractors.factory import ExtractorFactory

logger = setup_logger(__name__)

load_dotenv()


def run_pipeline() -> None:
    """Orchestrates website extraction using ExtractorFactory and saves raw documents locally."""
    try:
        config_path = "etl_config.yml"
        
        with open(config_path, "r", encoding="utf-8") as f:
            raw_config = yaml.safe_load(f)

        app_config = AppConfig.load_from_yaml(config_path)

        # Retrieve source_key directly from config file's extraction section
        extraction_config = raw_config.get("extraction", {})
        source_key = extraction_config.get("source_key")

        if not source_key:
            raise ValueError("Extraction 'source_key' is missing in etl_config.yml")

        # 1. Instantiate extractor dynamically via Factory
        extractor = ExtractorFactory.create(raw_config)
        
        documents = extractor.extract(
            source_key=source_key, tenant_id=app_config.aws.tenant_id
        )

        if not documents:
            logger.warning("No documents were extracted from the website.")
            return

        # Save extracted content to a local static directory for inspection
        output_dir = "extracted_content"
        os.makedirs(output_dir, exist_ok=True)

        logger.info(
            f"Saving {len(documents)} extracted documents to '{output_dir}/'..."
        )

        for idx, doc in enumerate(documents):
            doc_source_url = doc.metadata.get("source_key", f"page_{idx}")
            safe_name = hashlib.md5(doc_source_url.encode("utf-8")).hexdigest()
            file_path = os.path.join(output_dir, f"{idx}_{safe_name}.txt")

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(f"Source URL: {doc_source_url}\n")
                f.write(f"Tenant ID: {doc.metadata.get('tenant_id')}\n")
                f.write("-" * 50 + "\n\n")
                f.write(doc.content)

        logger.info(
            f"Successfully saved all pages to '{output_dir}/'. Inspect them before proceeding."
        )

    except BaseETLException as e:
        logger.critical(f"ETL Pipeline execution failed: {str(e)}")
        raise
    except Exception as e:
        logger.critical(f"Unexpected error in ETL execution: {str(e)}")
        raise


if __name__ == "__main__":
    run_pipeline()