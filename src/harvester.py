import requests
import hashlib
import time
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor

class DistributedHarvester:
    def __init__(self, worker_count: int = 8, deduplication_threshold: int = 3):
        self.worker_count = worker_count
        self.deduplication_threshold = deduplication_threshold
        self.crawl_queue = []
        self.crawled_urls = set()
        self.deduplication_cache = {}

    def add_url(self, url: str):
        self.crawl_queue.append(url)

    def _deduplicate(self, url: str) -> bool:
        url_hash = hashlib.md5(url.encode()).hexdigest()
        if url_hash in self.deduplication_cache:
            self.deduplication_cache[url_hash] += 1
            return self.deduplication_cache[url_hash] >= self.deduplication_threshold
        else:
            self.deduplication_cache[url_hash] = 1
            return False

    def _crawl(self, url: str) -> Tuple[str, str]:
        response = requests.get(url)
        content = response.text
        return url, content

    def run(self):
        with ThreadPoolExecutor(max_workers=self.worker_count) as executor:
            while self.crawl_queue:
                url = self.crawl_queue.pop(0)
                if url not in self.crawled_urls and not self._deduplicate(url):
                    self.crawled_urls.add(url)
                    future = executor.submit(self._crawl, url)
                    url, content = future.result()
                    yield url, content
                time.sleep(0.1)
