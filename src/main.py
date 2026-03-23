import os
import requests
import time
from typing import List
from dataclasses import dataclass

@dataclass
class MetadataRecord:
    source: str
    key: str
    value: str

class DecentralizedMetadataHarvester:
    def __init__(self, swarm_peers: List[str]):
        self.swarm_peers = swarm_peers

    def harvest_metadata(self) -> List[MetadataRecord]:
        metadata_records = []
        for peer in self.swarm_peers:
            try:
                response = requests.get(f'{peer}/metadata')
                if response.status_code == 200:
                    metadata = response.json()
                    for key, value in metadata.items():
                        metadata_records.append(MetadataRecord(source=peer, key=key, value=value))
            except requests.exceptions.RequestException:
                print(f'Failed to fetch metadata from {peer}')
        return metadata_records

    def save_metadata(self, metadata_records: List[MetadataRecord], output_dir: str):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        for record in metadata_records:
            filename = f'{record.key}.txt'
            filepath = os.path.join(output_dir, filename)
            with open(filepath, 'w') as f:
                f.write(record.value)

def main():
    swarm_peers = ['http://peer1.example.com', 'http://peer2.example.com', 'http://peer3.example.com']
    harvester = DecentralizedMetadataHarvester(swarm_peers)
    metadata_records = harvester.harvest_metadata()
    harvester.save_metadata(metadata_records, 'output')

if __name__ == '__main__':
    main()