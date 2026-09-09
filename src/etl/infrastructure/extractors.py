import io
import boto3
from pypdf import PdfReader
from botocore.exceptions import BotoCoreError, ClientError

from src.etl.core.config import AWSConfig
from src.etl.core.exceptions import ExtractionError
from src.etl.core.logger import setup_logger
from src.etl.domain.entities import Document
from src.etl.domain.interfaces import BaseExtractor

logger = setup_logger(__name__)


class S3DocumentExtractor(BaseExtractor):
    def __init__(self, config: AWSConfig):
        self.config = config
        self.s3_client = boto3.client("s3", region_name=config.region_name)

    def extract(self, source_key: str, tenant_id: str) -> Document:
        expected_prefix = f"tenants/{tenant_id}/"
        scoped_key = (
            source_key
            if source_key.startswith(expected_prefix)
            else f"{expected_prefix}{source_key.lstrip('/')}"
        )

        logger.info(
            f"Extracting object '{scoped_key}' from bucket '{self.config.s3_bucket}' for tenant '{tenant_id}'"
        )

        try:
            response = self.s3_client.get_object(
                Bucket=self.config.s3_bucket, Key=scoped_key
            )
            raw_bytes = response["Body"].read()

            if scoped_key.lower().endswith(".pdf"):
                pdf_file = io.BytesIO(raw_bytes)
                reader = PdfReader(pdf_file)
                content = "\n".join(
                    [page.extract_text() or "" for page in reader.pages]
                )
            else:
                content = raw_bytes.decode("utf-8", errors="ignore")

            return Document(
                content=content,
                metadata={
                    "source_key": scoped_key,
                    "tenant_id": tenant_id,
                    "content_type": response.get(
                        "ContentType", "application/octet-stream"
                    ),
                    "content_length": response.get("ContentLength", 0),
                },
            )

        except (BotoCoreError, ClientError) as e:
            logger.error(
                f"S3 extraction failed for key '{scoped_key}': {str(e)}"
            )
            raise ExtractionError(
                f"Failed to fetch document '{scoped_key}' from S3: {str(e)}"
            ) from e
        except Exception as e:
            logger.error(
                f"Unexpected extraction error for key '{scoped_key}': {str(e)}"
            )
            raise ExtractionError(
                f"Unexpected error during extraction: {str(e)}"
            ) from e