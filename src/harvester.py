import redis
import json
import time
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class HarvestJob:
    target_url: str
    metadata_types: List[str]
    job_id: str
    status: str = 'pending'
    result: Optional[Dict] = None

class DistributedHarvester:
    def __init__(self, redis_url: str = 'redis://localhost:6379'):
        self.redis_client = redis.from_StrictRedis.from_url(redis_url)
        self.job_queue = 'metadata_harvest_queue'
        self.results_key = 'harvest_results'
    
    def submit_job(self, target_url: str, metadata_types: List[str]) -> str:
        """Submit a new harvesting job to the distributed queue"""
        job = HarvestJob(
            target_url=target_url,
            metadata_types=metadata_types,
            job_id=f'job_{int(time.time())}_{hash(target_url)}'
        )
        self.redis_client.lpush(
            self.job_queue,
            json.dumps(job.__dict__)
        )
        return job.job_id

    def process_jobs(self, batch_size: int = 10):
        """Process jobs from the queue as a worker node"""
        while True:
            # Get batch of jobs
            raw_jobs = self.redis_client.lrange(
                self.job_queue, 
                0, 
                batch_size - 1
            )
            
            if not raw_jobs:
                time.sleep(1)
                continue
                
            for raw_job in raw_jobs:
                job = HarvestJob(**json.loads(raw_job))
                try:
                    # Process the actual metadata harvesting
                    result = self._harvest_metadata(
                        job.target_url,
                        job.metadata_types
                    )
                    
                    job.status = 'completed'
                    job.result = result
                    
                except Exception as e:
                    job.status = 'failed'
                    job.result = {'error': str(e)}
                
                # Store results
                self.redis_client.hset(
                    self.results_key,
                    job.job_id,
                    json.dumps(job.__dict__)
                )
                
                # Remove processed job
                self.redis_client.lrem(self.job_queue, 1, raw_job)
    
    def get_job_result(self, job_id: str) -> Optional[Dict]:
        """Retrieve the results for a specific job"""
        result = self.redis_client.hget(self.results_key, job_id)
        if result:
            return json.loads(result)
        return None

    def _harvest_metadata(self, url: str, metadata_types: List[str]) -> Dict:
        """Internal method to perform actual metadata harvesting"""
        # Implementation of actual harvesting logic goes here
        # This is a placeholder that should be replaced with real harvesting code
        return {
            'url': url,
            'timestamp': time.time(),
            'metadata': {t: f'Sample {t} metadata' for t in metadata_types}
        }

    def clear_queue(self):
        """Clear all jobs and results (useful for testing)"""
        self.redis_client.delete(self.job_queue)
        self.redis_client.delete(self.results_key)
