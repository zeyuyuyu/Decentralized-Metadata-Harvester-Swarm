import asyncio
import aiohttp
from typing import List, Dict, Optional
from dataclasses import dataclass
import time
import logging
from aiohttp import ClientSession
from ratelimit import limits, sleep_and_retry

@dataclass
class CrawlResult:
    url: str
    metadata: Dict
    timestamp: float
    status: int
    error: Optional[str] = None

class DistributedHarvester:
    def __init__(self, concurrency: int = 5, rate_limit: int = 30):
        self.concurrency = concurrency
        self.rate_limit = rate_limit
        self.session: Optional[ClientSession] = None
        self.results: List[CrawlResult] = []
        self.logger = logging.getLogger(__name__)

    @sleep_and_retry
    @limits(calls=30, period=60)
    async def _fetch_url(self, url: str) -> CrawlResult:
        if not self.session:
            self.session = aiohttp.ClientSession()

        try:
            start_time = time.time()
            async with self.session.get(url) as response:
                metadata = {
                    'headers': dict(response.headers),
                    'content_type': response.headers.get('content-type'),
                    'size': len(await response.read()),
                    'status': response.status
                }
                return CrawlResult(
                    url=url,
                    metadata=metadata,
                    timestamp=start_time,
                    status=response.status
                )

        except Exception as e:
            self.logger.error(f'Error crawling {url}: {str(e)}')
            return CrawlResult(
                url=url,
                metadata={},
                timestamp=time.time(),
                status=0,
                error=str(e)
            )

    async def process_urls(self, urls: List[str]) -> List[CrawlResult]:
        tasks = []
        for url in urls:
            task = asyncio.ensure_future(self._fetch_url(url))
            tasks.append(task)

        results = []
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
            if result.error:
                self.logger.warning(f'Failed to process {result.url}: {result.error}')
            else:
                self.logger.info(f'Successfully processed {result.url}')

        return results

    async def harvest(self, urls: List[str], chunk_size: Optional[int] = None) -> List[CrawlResult]:
        if chunk_size is None:
            chunk_size = self.concurrency * 2

        all_results = []
        for i in range(0, len(urls), chunk_size):
            chunk = urls[i:i + chunk_size]
            results = await self.process_urls(chunk)
            all_results.extend(results)

        if self.session:
            await self.session.close()

        return all_results

    @classmethod
    async def create_and_harvest(cls, urls: List[str], **kwargs) -> List[CrawlResult]:
        harvester = cls(**kwargs)
        return await harvester.harvest(urls)
