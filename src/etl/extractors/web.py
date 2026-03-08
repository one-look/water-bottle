import time
import logging
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from .base import BaseExtractor

logger = logging.getLogger(__name__)

class WebExtractor(BaseExtractor):
    def __init__(self, connection, config):
        self.connection = connection
        self.config = config
        self.base_url = self.config.get("url")
        self.max_pages = self.config.get("max_pages", 1)
        self.delay = self.config.get("delay", 1.0)
        
    def __call__(self):
        """Entry point for the Factory/nmc.py"""
        return self.extract()

    def _is_internal(self, url: str) -> bool:
        """Helper: Ensures the crawler stays on the college domain."""
        parsed = urlparse(url)
        # Check if the domain contains nmc.ac.in and isn't a file (PDF/JPG)
        is_nmc = "nmc.ac.in" in parsed.netloc or parsed.netloc == ""
        is_not_file = not any(url.lower().endswith(ext) for ext in ['.pdf', '.jpg', '.png', '.zip'])
        return is_nmc and is_not_file

    def extract(self):
        """
        Recursively crawls internal links up to max_pages.
        Returns: List of dicts containing URL and raw HTML content.
        """
        to_visit = [self.base_url]
        visited = set()
        knowledge_base = []

        logger.info(f"Starting crawl of {self.base_url} (Limit: {self.max_pages} pages)")

        while to_visit and len(visited) < self.max_pages:
            url = to_visit.pop(0)
            
            # Skip if already visited or invalid
            if url in visited or not url.startswith("http"):
                continue

            try:
                logger.info(f"Crawling: {url} [{len(visited)+1}/{self.max_pages}]")
                
                # The 'connection' here is your requests session from ConnectorFactory
                response = self.connection.get(url, timeout=10)
                visited.add(url)

                if response.status_code == 200:
                    html_content = response.text
                    
                    # Store raw data for the Transformer
                    knowledge_base.append({
                        "url": url,
                        "content": html_content
                    })

                    # DISCOVERY: Find new links to add to queue
                    soup = BeautifulSoup(html_content, 'html.parser')
                    for a in soup.find_all('a', href=True):
                        # Clean the link (remove fragments like #contact)
                        link = urljoin(self.base_url, a['href']).split('#')[0].rstrip('/')
                        
                        if self._is_internal(link) and link not in visited:
                            to_visit.append(link)

                # Respectful crawling delay
                time.sleep(self.delay)

            except Exception as e:
                logger.error(f"Failed to extract {url}: {e}")

        logger.info(f"Crawl complete. Collected {len(knowledge_base)} pages.")
        return knowledge_base