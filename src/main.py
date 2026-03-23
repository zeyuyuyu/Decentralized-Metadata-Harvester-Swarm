import asyncio
import aiohttp
import json
import hashlib
import time

class DistributedMetadataHarvester:
    def __init__(self, swarm_peers):
        self.swarm_peers = swarm_peers
        self.metadata_index = {}

    async def crawl_metadata(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                metadata = await response.json()
                metadata_hash = hashlib.sha256(json.dumps(metadata).encode()).hexdigest()
                self.metadata_index[metadata_hash] = metadata
                await self.distribute_metadata(metadata_hash, metadata)

    async def distribute_metadata(self, metadata_hash, metadata):
        tasks = []
        for peer in self.swarm_peers:
            tasks.append(self.send_to_peer(peer, metadata_hash, metadata))
        await asyncio.gather(*tasks)

    async def send_to_peer(self, peer, metadata_hash, metadata):
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{peer}/metadata", json={"hash": metadata_hash, "data": metadata}) as response:
                print(f"Sent metadata to {peer}: {response.status}")

    async def search_metadata(self, query):
        results = []
        for metadata in self.metadata_index.values():
            if query.lower() in json.dumps(metadata).lower():
                results.append(metadata)
        return results

async def main():
    swarm_peers = ["http://peer1.example.com", "http://peer2.example.com", "http://peer3.example.com"]
    harvester = DistributedMetadataHarvester(swarm_peers)
    await harvester.crawl_metadata("https://api.example.com/metadata")
    search_results = await harvester.search_metadata("important")
    print(search_results)

if __name__ == "__main__":
    asyncio.run(main())