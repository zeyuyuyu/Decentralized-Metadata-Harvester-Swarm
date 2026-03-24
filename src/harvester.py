import asyncio
import aiohttp
import json
from typing import List, Dict

class DistributedHarvester:
    def __init__(self, swarm_peers: List[str], timeout: int = 10):
        self.swarm_peers = swarm_peers
        self.timeout = timeout
        self.metadata_cache: Dict[str, Dict] = {}

    async def harvest_metadata(self, url: str) -> Dict:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for peer in self.swarm_peers:
                task = asyncio.create_task(self.harvest_from_peer(session, peer, url))
                tasks.append(task)
            results = await asyncio.gather(*tasks)
            merged_metadata = self.merge_metadata(results)
            self.metadata_cache[url] = merged_metadata
            return merged_metadata

    async def harvest_from_peer(self, session: aiohttp.ClientSession, peer: str, url: str) -> Dict:
        try:
            async with session.get(f'{peer}/metadata?url={url}', timeout=self.timeout) as response:
                data = await response.json()
                return data
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return {}

    def merge_metadata(self, metadata_lists: List[Dict]) -> Dict:
        merged = {}
        for metadata in metadata_lists:
            for key, value in metadata.items():
                if key not in merged:
                    merged[key] = value
                else:
                    if isinstance(merged[key], list):
                        merged[key].extend(value)
                    else:
                        merged[key] = [merged[key], value]
        return merged
