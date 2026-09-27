import time
from urllib.parse import urljoin, urlparse
import trafilatura
from bs4 import BeautifulSoup

from src.etl.domain.interfaces import BaseExtractor
from src.etl.domain.entities import Document

from src.etl.core.logger import setup_logger

logger = setup_logger(__name__)


class WebExtractor(BaseExtractor):
    """Crawls a website recursively and extracts clean text into Document entities."""

    def __init__(self, max_pages: int = 150):
        self.max_pages = max_pages

    def extract(self, source_key: str, tenant_id: str) -> List[Document]:
        root_url = source_key  # source_key acts as the website root URL
        domain = urlparse(root_url).netloc
        visited = set()
        queue = [root_url]
        documents = []

        logger.info(f"Starting web crawl from root: {root_url} for tenant '{tenant_id}'")

        while queue and len(visited) < self.max_pages:
            url = queue.pop(0)
            if url in visited:
                continue
            visited.add(url)

            try:
                downloaded = trafilatura.fetch_url(url)
                if not downloaded:
                    continue

                page_text = trafilatura.extract(downloaded, include_comments=False, include_tables=True)
                if page_text and len(page_text.strip()) > 100:
                    documents.append(
                        Document(
                            content=page_text,
                            metadata={
                                "source_key": url,
                                "tenant_id": tenant_id,
                                "content_type": "text/html",
                                "content_length": len(page_text),
                            },
                        )
                    )

                soup = BeautifulSoup(downloaded, "html.parser")
                for link in soup.find_all("a", href=True):
                    absolute_url = urljoin(url, link["href"]).split("#")[0]
                    parsed = urlparse(absolute_url)
                    if (
                        parsed.netloc == domain
                        and parsed.scheme in ["http", "https"]
                        and absolute_url not in visited
                        and absolute_url not in queue
                        and not any(absolute_url.endswith(ext) for ext in [".pdf", ".jpg", ".png", ".zip"])
                    ):
                        queue.append(absolute_url)

                time.sleep(0.3)
            except Exception as e:
                logger.error(f"Error crawling {url}: {e}")

        logger.info(f"Crawling complete. Extracted {len(documents)} pages.")
        return documents