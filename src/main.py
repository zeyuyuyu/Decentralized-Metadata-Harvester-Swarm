import asyncio
import logging
from pathlib import Path
from typing import List, Dict
from swarm_aggregator import SwarmAggregator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GitLensAI:
    def __init__(self, repos_path: str):
        self.repos_path = Path(repos_path)
        self.aggregator = SwarmAggregator()
        self.batch_size = 5  # Number of repos to process in parallel

    async def analyze_repository(self, repo_path: Path) -> Dict:
        """Analyze a single repository asynchronously"""
        try:
            logger.info(f'Analyzing repository: {repo_path}')
            stats = await self.aggregator.process_repository(repo_path)
            return {'path': repo_path, 'stats': stats, 'status': 'success'}
        except Exception as e:
            logger.error(f'Error analyzing {repo_path}: {str(e)}')
            return {'path': repo_path, 'error': str(e), 'status': 'failed'}

    async def process_batch(self, repos: List[Path]):
        """Process a batch of repositories in parallel"""
        tasks = [self.analyze_repository(repo) for repo in repos]
        return await asyncio.gather(*tasks)

    async def analyze_all(self) -> List[Dict]:
        """Analyze all repositories in the specified directory"""
        all_repos = list(self.repos_path.glob('*/.git/..'))
        results = []

        for i in range(0, len(all_repos), self.batch_size):
            batch = all_repos[i:i + self.batch_size]
            batch_results = await self.process_batch(batch)
            results.extend(batch_results)

        return results

    def generate_report(self, results: List[Dict]):
        """Generate a summary report from analysis results"""
        successful = [r for r in results if r['status'] == 'success']
        failed = [r for r in results if r['status'] == 'failed']

        logger.info(f'Analysis complete:')
        logger.info(f'Processed {len(results)} repositories')
        logger.info(f'Successful: {len(successful)}')
        logger.info(f'Failed: {len(failed)}')

        if failed:
            logger.warning('Failed repositories:')
            for repo in failed:
                logger.warning(f"{repo['path']}: {repo['error']}")

def main():
    repos_dir = './repositories'
    analyzer = GitLensAI(repos_dir)
    
    async def run_analysis():
        results = await analyzer.analyze_all()
        analyzer.generate_report(results)

    asyncio.run(run_analysis())

if __name__ == '__main__':
    main()