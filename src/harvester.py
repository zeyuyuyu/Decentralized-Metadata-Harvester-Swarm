import requests
import json
from typing import List, Dict

class MetadataHarvester:
    def __init__(self, chains: List[str]):
        self.chains = chains
        self.metadata_cache: Dict[str, Dict] = {}

    def harvest_metadata(self, contract_address: str) -> Dict:
        metadata = {}
        for chain in self.chains:
            if chain not in self.metadata_cache:
                self.metadata_cache[chain] = self._fetch_metadata(contract_address, chain)
            metadata[chain] = self.metadata_cache[chain]
        return metadata

    def _fetch_metadata(self, contract_address: str, chain: str) -> Dict:
        url = f'https://api.{chain}.com/metadata/{contract_address}'
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
