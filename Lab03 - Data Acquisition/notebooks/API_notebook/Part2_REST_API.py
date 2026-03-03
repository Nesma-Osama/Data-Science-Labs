import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
from datetime import datetime
import requests
import pandas as pd
import matplotlib.pyplot as plt
import time
from dotenv import load_dotenv  
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('api_requests.log'),  # Saves to disk
        logging.StreamHandler(),  # Also print to console
    ],
)


class RateLimiter:
    """
    Smart rate limiter that tracks API usage.
    Uses a sliding time window to count recent requests.
    """

    def __init__(self, max_requests=60, time_window=3600):
        """
        Args:
            max_requests: Maximum requests allowed in the time window
            time_window: Time window in seconds (3600 = 1 hour)
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []  # List of timestamps of past requests

    def wait_if_needed(self):
        """Wait if we've hit the rate limit before making a new request."""
        now = time.time()

        # Remove old timestamps outside the sliding time window
        self.requests = [
            req_time for req_time in self.requests if now - req_time < self.time_window
        ]

        # If we've used up our quota, sleep until the oldest request expires
        if len(self.requests) >= self.max_requests:
            oldest_request = self.requests[0]
            sleep_time = self.time_window - (now - oldest_request)
            if sleep_time > 0:
                print(
                    f"⏰ Rate limit reached. Sleeping for {sleep_time:.1f} seconds..."
                )
                time.sleep(sleep_time)
            self.requests = []  # Clear after sleeping

        # Record the timestamp of this new request
        self.requests.append(now)
def check_rate_limit(response):
    """
    Check rate limit info from response headers.
    GitHub includes rate limit details in every response.
    """
    if 'X-RateLimit-Limit' in response.headers:
        limit = int(response.headers['X-RateLimit-Limit'])
        remaining = int(response.headers['X-RateLimit-Remaining'])
        reset_timestamp = int(response.headers['X-RateLimit-Reset'])
        reset_time = datetime.fromtimestamp(reset_timestamp)

        print(f"Rate Limit: {remaining}/{limit}")
        print(f"Resets at: {reset_time}")

        # Warn when running low on available requests
        if remaining < 10:
            print("⚠️ Warning: Low on API requests!")

        return remaining
    return None
class GitHubAPI:
    """
    Reusable GitHub API client with all best practices:
    - Session management with retry logic
    - Rate limiting
    - Authentication via token
    - Logging
    """

    def __init__(self, token=None):
        self.base_url = 'https://api.github.com'
        self.session = self._create_session()  # Robust session with retries
        self.rate_limiter = RateLimiter(
            max_requests=5000, time_window=3600
        )  # Authenticated limits

        # Add authentication token if provided
        if token:
            self.session.headers.update({'Authorization': f'Bearer {token}'})

        # Always set these headers for proper API communication
        self.session.headers.update(
            {
                'Accept': 'application/vnd.github.v3+json',
                'User-Agent': 'Library-Tutorial/1.0',
            }
        )

        self.logger = logging.getLogger(self.__class__.__name__)

    def _create_session(self):
        """Create session with retry logic (private method)."""
        session = requests.Session()
        retry_strategy = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get(self, endpoint, params=None):
        """Make GET request with rate limiting."""
        self.rate_limiter.wait_if_needed()  # Respect rate limits
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()  # Raises exception for 4xx/5xx
            self.logger.info(f"GET {endpoint} - Status: {response.status_code}")

            # Peek at remaining rate limit with each response
            remaining = check_rate_limit(response)
            return response.json()

        except Exception as e:
            self.logger.error(f"Error fetching {endpoint}: {e}")
            raise

    def get_repo(self, owner, repo):
        """Get repository information."""
        return self.get(f'/repos/{owner}/{repo}')

    def get_user_repos(self, username):
        """Get all repositories for a user."""
        return self.get(f'/users/{username}/repos', params={'per_page': 100})

    def search_repos(self, query, language=None, min_stars=None):
        """
        Search repositories.

        Args:
            query: Search query string
            language: Filter by programming language
            min_stars: Minimum stars required

        Returns:
            list: Repository results
        """
        # Build search query by combining filters with spaces
        q_parts = [query]
        if language:
            q_parts.append(f"language:{language}")
        if min_stars:
            q_parts.append(f"stars:>={min_stars}")

        q = ' '.join(q_parts)
        results = self.get('/search/repositories', params={'q': q})
        return results['items']

    def to_dataframe(self, repos):
        """Convert repository list to DataFrame for analysis."""
        data = []
        for repo in repos:
            data.append(
                {
                    'name': repo['name'],
                    'full_name': repo['full_name'],
                    'description': repo.get('description'),
                    'stars': repo['stargazers_count'],
                    'forks': repo['forks_count'],
                    'language': repo.get('language'),
                    'created_at': repo['created_at'],
                    'updated_at': repo['updated_at'],
                }
            )
        return pd.DataFrame(data)
    
#---- Requirments--------------------------------------------------------------
#--------------------------------Task 1
#---------------------------- 1.1
def task1_fetch_repos():
    """
    Fetch repository information for major ML frameworks.
    Returns a DataFrame with key metrics.
    """
    repos = ['tensorflow/tensorflow', 'pytorch/pytorch', 'scikit-learn/scikit-learn']
    api = GitHubAPI(token=os.getenv('GITHUB_TOKEN'))
    # Your code here
    repos_data=[]
    for i in range(len(repos)):
        owner,repo_name = repos[i].split("/")
        repo=api.get_repo(owner,repo_name)
        repo_data={
            "name":repo["name"],
            "stars":repo["stargazers_count"],
            "forks":repo["forks_count"],
            "language":repo["language"],
            "open_issues":repo["open_issues_count"],
            "created_date":repo["created_at"]
        }
        repos_data.append(repo_data)
    df = pd.DataFrame(repos_data)
    return df

df = task1_fetch_repos()
df.to_csv('task1_github.csv', index=False)
#------------------------------------1.2
df=pd.read_csv('task1_github.csv')
metric_df=df[["name"]]
metric_df["Age in days"]=(pd.to_datetime("today",utc=True)-pd.to_datetime(df["created_date"],utc=True)).dt.days
metric_df["Stars per day"]=df["stars"]/metric_df["Age in days"]
metric_df["Issues per star ratio"]=df["open_issues"]/df["stars"]
metric_df.to_csv('task1_metrics.csv', index=False)
#-----------------------------------1.3
plt.style.use("dark_background")
fig,ax=plt.subplots(3,1,figsize=(10,15))
fig.suptitle("GitHub Repository Metrics Comparison", fontsize=16)
ax[0].bar(metric_df["name"],metric_df["Age in days"])
ax[0].set_ylabel("Age in days")
ax[0].set_xlabel("Repository")
ax[1].bar(metric_df["name"],metric_df["Stars per day"])
ax[1].set_ylabel("Stars per day")
ax[1].set_xlabel("Repository")
ax[2].bar(metric_df["name"],metric_df["Issues per star ratio"])
ax[2].set_ylabel("Issues per star ratio")
ax[2].set_xlabel("Repository")
plt.savefig("task1_comparison.png")
plt.close()
#-----------------------------------------Task2
# ------------------------------------2.1
def fetch_user_repos_paginated(username):
    """
    Fetch all repositories for a user with pagination.

    Args:
        username: GitHub username

    Returns:
        list: All repositories
    """
    
    all_repos = []
    page = 1
    url=f"https://api.github.com/users/{username}/repos"
    token = os.getenv('GITHUB_TOKEN')
    headers={'Authorization': f'Bearer {token}', 
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'Library-Tutorial/1.0'}
    logger = logging.getLogger("fetch_user_repos_paginated")
    while True:
        try:
            params = {'page': page, 'per_page': 100}
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()  
            if response.status_code==200:
                repos=response.json()
                if not repos:
                    logger.info(f"There is no any repo in {page}")
                    break
                all_repos.extend(repos)
                if len(repos)<100:
                    logger.info(f"Page {page} is the last page  with less than 100 repos")
                    break
                logger.info(f"Fetch page {page} and the result is {len(repos)} repos")
            time.sleep(1)
            page+=1
        except Exception as e:
            logger.error(f"Error fetching page {page}: {e}")
            break
    return all_repos

all_repos=fetch_user_repos_paginated("Nesma-Osama")
all_repos_list=[]
for repo in all_repos:
    repo_dict={
            "name":repo["name"],
            "stars":repo["stargazers_count"],
            "forks":repo["forks_count"],
            "language":repo["language"],
            "open_issues":repo["open_issues_count"],
            "created_date":repo["created_at"],
            "updated_date":repo["updated_at"],
            "watchers_count":repo["watchers_count"]
    }
    all_repos_list.append(repo_dict)
all_repos_df=pd.DataFrame(all_repos_list)
all_repos_df.to_csv("task2_all_repos.csv", index=False)
# ------------------------------------2.2
Mmost_use_language=all_repos_df["language"].mode()[0]
average_stars=all_repos_df["stars"].mean()
total_forks=all_repos_df["forks"].sum()
most_recent_update_repo=all_repos_df.sort_values("updated_date",ascending=False).iloc[0]
oldest_repo=all_repos_df.sort_values("created_date").iloc[0]
with open("task2_analysis.txt","w") as f:
    f.write("-------------------------Task 2 Analysis Report----------------------\n")
    f.write(f"Most used programming language across all repos: {Mmost_use_language}\n")
    f.write(f"Average stars per repository: {average_stars}\n")
    f.write(f"Total forks across all repos: {total_forks}\n")
    f.write(f"Most recently updated repo:\n{most_recent_update_repo}\n\n\n")
    f.write(f"Oldest repo:\n{oldest_repo}\n")
    
#------------------Task 3-----------------------
class GitHubAnalyzer:
    """
    Complete GitHub API client with analysis capabilities.
    Build on top of the GitHubAPI class concepts from section 2.7.
    """

    def __init__(self, token=None):
        # Your initialization
        # Hint: Set up session, rate_limiter, logger like in GitHubAPI
        self.logger = logging.getLogger(self.__class__.__name__)
        self.rate_limiter = RateLimiter(
            max_requests=5000, time_window=3600
        )  
        self.session.headers.update(
            {
                'Accept': 'application/vnd.github.v3+json',
                'User-Agent': 'Library-Tutorial/1.0',
            }
        )
        if token:
            self.session.headers.update({'Authorization': f'Bearer {token}'})
        self.session = self._create_session() 
        self.base_url = 'https://api.github.com'

        
    def _create_session(self):
        session = requests.Session()
        retry_strategy = Retry(
                total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
            )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def search_repos(self, query, language=None, min_stars=0):
        """
        Search repositories with filters.

        Returns:
            DataFrame with results
        """
        pass

    def compare_repos(self, repo_list):
        """
        Compare multiple repositories.

        Args:
            repo_list: List of "owner/repo" strings

        Returns:
            DataFrame with comparison
        """
        pass

    def export_to_excel(self, df, filename):
        """
        Export DataFrame to Excel with formatting.
        - Bold headers
        - Auto-adjust column widths
        - Add creation timestamp
        """
        pass


# Test your class by:
# 1. Searching for "data science" repos in Python with >500 stars
# 2. Comparing 5 repos of your choice
# 3. Exporting results to task3_results.xlsx