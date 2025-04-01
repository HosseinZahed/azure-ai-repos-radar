import os
import requests
import time
import logging
from typing import List, Dict, Any, Optional
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(override=True)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GitHubCrawler:
    """Class for crawling GitHub repositories.
    
    Required GitHub Token Scopes:
    - `public_repo`: For access to public repositories
    - `repo`: For access to private repositories (if needed)
    - `read:org`: For access to organization repository listings
    
    To create a token with these scopes, visit:
    https://github.com/settings/tokens
    """
    
    def __init__(self, github_token: Optional[str] = None, max_retries: int = 3, backoff_factor: float = 0.5):
        """
        Initialize the GitHub crawler.
        
        Args:
            github_token: Optional GitHub API token to increase rate limits.
                          Required scopes: public_repo, read:org
            max_retries: Maximum number of retries for failed requests
            backoff_factor: Backoff factor for exponential delay between retries
        """
        self.base_url = "https://api.github.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json"
        }
        if github_token:
            self.headers["Authorization"] = f"token {github_token}"
        
        # Set up session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)        
    
    def _handle_pagination(self, url: str) -> List[Dict]:
        """
        Handle API pagination and collect all results.
        
        Args:
            url: Base URL for the API request
            
        Returns:
            List of all items from all pages
        """
        all_items = []
        page = 1
        
        while True:
            page_url = f"{url}{'&' if '?' in url else '?'}per_page=100&page={page}"
            logger.info(f"Fetching page {page}")
            
            try:
                response = self.session.get(page_url, headers=self.headers)
                response.raise_for_status()
                
                # Check and handle rate limits
                self._handle_rate_limits(response)
                
                items = response.json()
                if not items:
                    break
                    
                all_items.extend(items)
                
                # Check if we've reached the last page
                if len(items) < 100:
                    break
                    
                page += 1
                
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error occurred: {e}")
                if response.status_code == 403 and "rate limit exceeded" in response.text.lower():
                    self._handle_rate_limits(response, force_wait=True)
                    # Don't increment page, retry the same page
                    continue
                break
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error occurred: {e}")
                break
                
        return all_items
    
    def _handle_rate_limits(self, response: requests.Response, force_wait: bool = False) -> None:
        """
        Check and handle GitHub API rate limits.
        
        Args:
            response: HTTP response from GitHub API
            force_wait: Whether to force waiting for rate limit reset
        """
        if force_wait or "X-RateLimit-Remaining" in response.headers and int(response.headers["X-RateLimit-Remaining"]) < 10:
            reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
            if reset_time:
                current_time = time.time()
                sleep_time = max(reset_time - current_time + 1, 0)  # Add 1 second buffer
                
                if sleep_time > 0:
                    logger.warning(f"Rate limit almost exceeded. Waiting for {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
            else:
                # If no reset time is provided, use the Retry-After header or default to 60 seconds
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limit exceeded. Waiting for {retry_after} seconds...")
                time.sleep(retry_after)
    
    def fetch_organization_repos(self, org_name: str) -> List[Dict[str, Any]]:
        """
        Fetch all repositories from a GitHub organization.
        
        Args:
            org_name: Name of the organization (e.g., 'Azure')
            
        Returns:
            List of dictionaries containing repository information
        """
        url = f"{self.base_url}/orgs/{org_name}/repos"
        all_repos = self._handle_pagination(url)
        
        # Extract relevant information
        simplified_repos = []
        for repo in all_repos:
            try:
                simplified_repos.append(self._extract_repo_info(repo))
            except KeyError as e:
                logger.warning(f"Missing key in repository data: {e}")
                
        logger.info(f"Successfully fetched {len(simplified_repos)} repositories from {org_name}")
        return simplified_repos
    
    def _extract_repo_info(self, repo: Dict) -> Dict[str, Any]:
        """
        Extract and format relevant repository information.
        
        Args:
            repo: Raw repository data from GitHub API
            
        Returns:
            Dictionary with simplified repository information
        """
        # Format the last update time
        last_updated = None
        if 'updated_at' in repo and repo['updated_at']:
            try:
                updated_datetime = datetime.strptime(repo['updated_at'], '%Y-%m-%dT%H:%M:%SZ')
                last_updated = updated_datetime.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                last_updated = repo['updated_at']
        
        return {
            'name': repo['name'],
            'description': repo['description'],
            'url': repo['html_url'],
            'stars': repo['stargazers_count'],
            'forks': repo['forks_count'],
            'language': repo['language'],
            'last_updated': last_updated
        }


if __name__ == "__main__":
    # Example usage - will fetch ALL repositories
    token = os.environ.get("GITHUB_TOKEN")
    crawler = GitHubCrawler()
    azure_repos = crawler.fetch_organization_repos("Azure")
    
    # Print summary
    print(f"Total repositories fetched: {len(azure_repos)}")
    
    # Print the first 5 repositories
    for repo in azure_repos[:5]:
        print(f"Name: {repo['name']}")
        print(f"Description: {repo['description']}")
        print(f"URL: {repo['url']}")
        print(f"Stars: {repo['stars']}")
        print(f"Language: {repo['language']}")
        print(f"Last Updated: {repo['last_updated']}")
        print("-" * 50)
