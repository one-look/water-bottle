from dotenv import load_dotenv
from src.etl.core.config import AppConfig
from src.etl.infrastructure.extractors import S3DocumentExtractor

# 1. Load AWS credentials into environment variables
load_dotenv()


def pipeline():
    config = AppConfig.load_from_yaml("etl_config.yml")
    extractor = S3DocumentExtractor(config=config.aws)

    # 2. Extract (Path is automatically scoped to tenants/nmc/tnedu.pdf)
    tenant_id = "nmc"
    source_key = "tnedu.pdf"

    doc = extractor.extract(source_key=source_key, tenant_id=tenant_id)
    print("Extracted content successfully:", doc.content[:100])


if __name__ == "__main__":
    pipeline()