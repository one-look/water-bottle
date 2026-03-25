import httpx
import yaml
import json
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

class ModularTextCrawler:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.base_url = self.config['base_url']
        self.seed_urls = list(set(self.config['seed_urls']))
        self.keywords = self.config['faculty_keywords']
        self.visited = set()
        self.results = []

    def parse_structured_text(self, html, url):
        soup = BeautifulSoup(html, 'html.parser')
        
        # Strip UI junk
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()

        title = soup.title.get_text(strip=True) if soup.title else url
        sections = []
        current_heading = "General"
        current_text = ""

        # Capture content tags in order
        for element in soup.find_all(['h1', 'h2', 'h3', 'h4', 'p', 'li', 'tr']):
            text = element.get_text(separator=' ', strip=True)
            if not text: continue

            if element.name.startswith('h'):
                if current_text:
                    sections.append({"heading": current_heading, "content": current_text.strip()})
                current_heading = text
                current_text = ""
            else:
                current_text += text + " "

        if current_text:
            sections.append({"heading": current_heading, "content": current_text.strip()})

        # Identify Faculty Sub-links
        sub_links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            link_text = a.get_text().lower()
            if any(k in href.lower() or k in link_text for k in self.keywords):
                full_url = urljoin(self.base_url, href)
                if urlparse(full_url).netloc == urlparse(self.base_url).netloc:
                    sub_links.append(full_url)

        return {"url": url, "title": title, "sections": sections}, list(set(sub_links))

    async def run(self):
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            queue = self.seed_urls.copy()
            while queue:
                url = queue.pop(0)
                if url in self.visited: continue
                self.visited.add(url)

                try:
                    resp = await client.get(url)
                    if resp.status_code == 200:
                        print(f"[*] Crawling: {url}")
                        data, faculty_links = self.parse_structured_text(resp.text, url)
                        self.results.append(data)
                        # Add faculty links to priority queue
                        queue = faculty_links + queue 
                except Exception as e:
                    print(f"[!] Error {url}: {e}")

        with open(self.config['output_file'], 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"\n[DONE] Saved {len(self.results)} pages to {self.config['output_file']}")

if __name__ == "__main__":
    crawler = ModularTextCrawler('examples/nmc/crawlconfig.yml')
    asyncio.run(crawler.run())