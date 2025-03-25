import os
import re
import time
import json
import logging
import csv
import hashlib
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Dict, Any, Optional, Set, Tuple
import pandas as pd
import asyncpraw
from dotenv import load_dotenv
from difflib import SequenceMatcher

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Define environment variables first
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'script:fishing_vessel_search:v1.0 (by /u/rgdonohue)')
REDDIT_AUTH_URL = 'https://www.reddit.com/api/v1/access_token'
REDDIT_SEARCH_URL = 'https://oauth.reddit.com/r/{}/search'
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
GOOGLE_CX = os.getenv('GOOGLE_SEARCH_CX')

# Validate environment variables
def validate_env_vars():
    required_vars = {
        'REDDIT_CLIENT_ID': REDDIT_CLIENT_ID,
        'REDDIT_CLIENT_SECRET': REDDIT_CLIENT_SECRET
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Constants
SEARCH_LIMIT = 100  # max posts to retrieve per request
REQUEST_SLEEP = 1  # reduced delay between requests
MAX_TERMS_PER_BATCH = 50  # Maximum number of terms to process in one batch
BATCH_TIMEOUT = 300  # Maximum time (in seconds) to spend on each batch
SEARCH_TIMEOUT = 10  # Timeout for individual search requests
PROGRESS_INTERVAL = 10  # How often to log progress (in seconds)

# Scoring weights configuration
SCORING_CONFIG = {
    'keyword_match_weight': 3,  # Points per keyword match (max 15)
    'industry_term_weight': 3,  # Points per industry term match (max 15)
    'title_match_bonus': 10,    # Bonus points for keyword in title
    'recency_max_score': 10,    # Maximum points for recency
    'recency_half_life': 180,   # Days after which recency score is halved
    'upvote_weight': 0.14,      # Points per 50 upvotes (max 7)
    'comment_weight': 0.6,      # Points per 5 comments (max 3)
}

# Industry terms for relevance scoring
INDUSTRY_TERMS = [
    'fishmeal', 'fish oil', 'fish meal',
    'reduction plant', 'processing facility', 'processing plant',
    'landing site', 'commercial fleet', 'pelagic vessel',
    'fishing vessel', 'fishing industry', 'seafood',
    'fish processing', 'marine harvest', 'fisheries'
]

# Updated relevance keywords for Reddit search with broader terms
RELEVANCE_KEYWORDS: Set[str] = {
    'fishing vessel', 'fishing boat', 'trawler', 'commercial fishing',
    'fishmeal', 'fish meal', 'fish oil', 'processing plant', 'seafood processing',
    'aquaculture', 'marine harvest', 'catch', 'landing', 'industrial fishing',
    'fishing industry', 'seafood', 'fish processing', 'fishing company',
    'marine resources', 'fisheries', 'fishing fleet', 'commercial vessel'
}

# Updated subreddit list with more reliable subreddits
SUBREDDIT_RECOMMENDATIONS: List[str] = [
    'Fishing', 'worldnews', 'news', 'business',
    'environment', 'science', 'technology',
    'investing', 'maritime', 'shipping',
    'ocean', 'sustainability'
]

class SearchProgress:
    def __init__(self, total_terms: int):
        self.total_terms = total_terms
        self.completed_terms = 0
        self.last_update = time.time()
        self.start_time = time.time()
        
    def update(self, completed: int) -> None:
        self.completed_terms += completed
        current_time = time.time()
        if current_time - self.last_update >= PROGRESS_INTERVAL:
            elapsed = current_time - self.start_time
            rate = self.completed_terms / elapsed if elapsed > 0 else 0
            remaining = (self.total_terms - self.completed_terms) / rate if rate > 0 else 0
            logger.info(
                f"Progress: {self.completed_terms}/{self.total_terms} terms "
                f"({(self.completed_terms/self.total_terms)*100:.1f}%) "
                f"Rate: {rate:.1f} terms/sec "
                f"ETA: {timedelta(seconds=int(remaining))}"
            )
            self.last_update = current_time

class DiskCache:
    """Persistent disk-based cache for search results"""
    def __init__(self, cache_dir: str = ".cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
    def get_cache_path(self, key: str) -> str:
        hash_key = hashlib.md5(key.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{hash_key}.json")
        
    def get(self, key: str) -> Optional[Dict]:
        path = self.get_cache_path(key)
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                if time.time() < data['expiry']:
                    return data['results']
            except Exception as e:
                logger.warning(f"Cache read failed for {key}: {e}")
        return None
        
    def set(self, key: str, results: List[Dict], expiry: float):
        path = self.get_cache_path(key)
        try:
            with open(path, 'w') as f:
                json.dump({
                    'results': results,
                    'expiry': expiry
                }, f)
        except Exception as e:
            logger.warning(f"Cache write failed for {key}: {e}")

class AdaptiveRateLimiter:
    """Dynamically adjusts request rate based on success/failure ratio"""
    def __init__(self, initial_rate: int = 60, min_rate: int = 30, max_rate: int = 120):
        self.current_rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.success_count = 0
        self.failure_count = 0
        self.last_adjust = time.time()
        self.last_request = time.time()
        
    async def wait(self):
        """Wait appropriate amount of time before next request"""
        now = time.time()
        wait_time = max(0, (1.0 / self.current_rate) - (now - self.last_request))
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        self.last_request = time.time()
        
    def success(self):
        """Record a successful request"""
        self.success_count += 1
        self._adjust()
        
    def failure(self):
        """Record a failed request"""
        self.failure_count += 1
        self._adjust()
        
    def _adjust(self):
        """Adjust the rate based on success/failure ratio"""
        now = time.time()
        if now - self.last_adjust >= 60:  # Adjust every minute
            total = self.success_count + self.failure_count
            if total >= 10:  # Only adjust if we have enough data
                success_rate = self.success_count / total
                if success_rate > 0.95:
                    # Very successful, try increasing rate
                    self.current_rate = min(self.current_rate * 1.1, self.max_rate)
                    logger.debug(f"Increasing rate to {self.current_rate:.1f} requests/sec")
                elif success_rate < 0.8:
                    # Too many failures, reduce rate
                    self.current_rate = max(self.current_rate * 0.8, self.min_rate)
                    logger.debug(f"Decreasing rate to {self.current_rate:.1f} requests/sec")
            
            # Reset counters
            self.success_count = 0
            self.failure_count = 0
            self.last_adjust = now

class SearchState:
    """Maintains state for resumable searches"""
    def __init__(self, save_path: str = "search_state.json"):
        self.save_path = save_path
        self.completed_terms = set()
        self.results = []
        self.start_time = time.time()
        self.load()
        
    def load(self):
        """Load previous search state if it exists"""
        if os.path.exists(self.save_path):
            try:
                with open(self.save_path, 'r') as f:
                    data = json.load(f)
                    self.completed_terms = set(data['completed_terms'])
                    self.results = data['results']
                    logger.info(f"Loaded {len(self.completed_terms)} completed terms and {len(self.results)} results")
            except Exception as e:
                logger.error(f"Failed to load search state: {e}")
                
    def save(self):
        """Save current search state"""
        try:
            with open(self.save_path, 'w') as f:
                json.dump({
                    'completed_terms': list(self.completed_terms),
                    'results': self.results,
                    'last_updated': time.strftime('%Y-%m-%d %H:%M:%S')
                }, f, indent=2)
            logger.debug("Search state saved")
        except Exception as e:
            logger.error(f"Failed to save search state: {e}")
            
    def add_results(self, term: str, new_results: List[Dict]):
        """Add results for a completed term"""
        self.completed_terms.add(term)
        self.results.extend(new_results)
        
        # Save periodically (every 5 minutes)
        if time.time() - self.start_time >= 300:
            self.save()
            self.start_time = time.time()

def quick_similarity(a: str, b: str) -> float:
    """Fast similarity check using character sets and improved matching"""
    # If strings are exactly the same, return 1.0
    if a == b:
        return 1.0
        
    # If one string contains the other, consider them very similar
    a_lower = a.lower()
    b_lower = b.lower()
    if a_lower in b_lower or b_lower in a_lower:
        return 0.95
    
    # If length difference is too large, they're different
    if abs(len(a) - len(b)) / max(len(a), len(b)) > 0.5:
        return 0.0
    
    # Compare character sets
    set_a = set(a_lower)
    set_b = set(b_lower)
    intersection = len(set_a.intersection(set_b))
    union = len(set_a.union(set_b))
    
    # Also consider word-level similarity for multi-word terms
    words_a = set(a_lower.split())
    words_b = set(b_lower.split())
    word_intersection = len(words_a.intersection(words_b))
    word_union = len(words_a.union(words_b))
    
    char_similarity = intersection / union if union > 0 else 0.0
    word_similarity = word_intersection / word_union if word_union > 0 else 0.0
    
    # Combine both similarities with more weight on word similarity
    return 0.4 * char_similarity + 0.6 * word_similarity

def preprocess_search_terms(terms: List[str]) -> List[str]:
    """Group similar terms with less aggressive deduplication"""
    logger.info(f"Starting preprocessing of {len(terms)} terms...")
    
    # First normalize all terms
    normalized_terms = []
    
    # Process in chunks to show progress
    chunk_size = 100
    for i in range(0, len(terms), chunk_size):
        chunk = terms[i:min(i + chunk_size, len(terms))]
        normalized_chunk = [term.strip() for term in chunk if term and len(term.strip()) >= 3]
        normalized_terms.extend(normalized_chunk)
        if i % 500 == 0:
            logger.info(f"Normalized {i}/{len(terms)} terms...")
    
    # Remove exact duplicates first
    unique_terms = list(dict.fromkeys(normalized_terms))
    logger.info(f"Removed {len(normalized_terms) - len(unique_terms)} exact duplicates")
    
    # Sort by length for more efficient comparison
    sorted_terms = sorted(unique_terms, key=len)
    final_terms = []
    excluded_terms = set()
    
    # Process in smaller batches
    batch_size = 50
    for i in range(0, len(sorted_terms), batch_size):
        batch = sorted_terms[i:i + batch_size]
        for term in batch:
            if term in excluded_terms:
                continue
            
            # Only compare with terms of similar length
            term_len = len(term)
            similar_found = False
            
            for existing in final_terms:
                # More permissive length difference
                if abs(len(existing) - term_len) > 10:
                    continue
                    
                # More permissive similarity threshold
                if quick_similarity(term, existing) > 0.9:  # Increased threshold
                    excluded_terms.add(term)
                    similar_found = True
                    break
            
            if not similar_found:
                final_terms.append(term)
        
        if i % 200 == 0 and i > 0:
            logger.info(f"Deduplication progress: {i}/{len(sorted_terms)} terms...")
    
    logger.info(f"Preprocessed {len(terms)} terms to {len(final_terms)} unique terms")
    return final_terms

def clean_search_terms(df: pd.DataFrame, name_column: str) -> List[str]:
    """Clean and extract unique search terms from a DataFrame column with improved validation"""
    # Minimal set of common words to remove
    common_words = {'the', 'and', 'of'}
    
    # Process DataFrame in chunks with better error handling
    chunk_size = 1000
    cleaned_terms = set()  # Use set for automatic deduplication
    total_rows = len(df)
    
    try:
        for chunk_start in range(0, total_rows, chunk_size):
            chunk = df.iloc[chunk_start:chunk_start + chunk_size]
            terms = chunk[name_column].dropna().astype(str).values
            
            for term in terms:
                # Keep original term if it's long enough
                if len(term.strip()) >= 3:
                    cleaned_terms.add(term.strip())
                
                # Also try cleaning the term
                cleaned = term.lower()
                # Replace specific punctuation with spaces
                cleaned = re.sub(r'[,\.\(\)\[\]{}]', ' ', cleaned)
                # Replace other special chars with empty string
                cleaned = re.sub(r'[^a-z0-9\s\-_]', '', cleaned)
                # Normalize whitespace
                cleaned = ' '.join(cleaned.split())
                
                # Remove common words only if term is multi-word
                if ' ' in cleaned:
                    words = [w for w in cleaned.split() if w not in common_words and len(w) > 1]
                    cleaned = ' '.join(words)
                
                if len(cleaned) >= 3:
                    cleaned_terms.add(cleaned)
            
            if chunk_start % 5000 == 0:
                logger.info(f"Processed {chunk_start}/{total_rows} rows from {name_column}")
    
    except Exception as e:
        logger.error(f"Error cleaning search terms: {e}")
        logger.debug(f"Sample terms that caused error: {terms[:5] if 'terms' in locals() else 'N/A'}")
    
    # Convert to list and preprocess
    return preprocess_search_terms(list(cleaned_terms))

class RedditSearcher:
    def __init__(self):
        self.reddit = asyncpraw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
        self.cache = {}
        self.cache_file = 'data_out/search_cache.json'
        self.load_cache()

    def load_cache(self):
        """Load cached results from file"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    self.cache = json.load(f)
                logger.info(f"Loaded {len(self.cache)} cached results")
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
            self.cache = {}

    def save_cache(self):
        """Save cache to file"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f)
            logger.info(f"Saved {len(self.cache)} results to cache")
        except Exception as e:
            logger.error(f"Error saving cache: {e}")

    async def search_term(self, term: str, max_results: int = 100) -> List[Dict[str, Any]]:
        """Search for a term across subreddits using AsyncPRAW"""
        cache_key = f"term_{term}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        results = []
        for subreddit_name in SUBREDDIT_RECOMMENDATIONS:
            try:
                subreddit = await self.reddit.subreddit(subreddit_name)
                
                # Use different search queries for better coverage
                search_queries = [
                    term,
                    f'"{term}"',
                    f'{term} (fishing OR vessel OR boat)',
                    f'title:{term}'
                ]
                
                for query in search_queries:
                    try:
                        async for submission in subreddit.search(query, limit=max_results, sort='relevance'):
                            if is_relevant_post(submission.title + " " + submission.selftext):
                                # Find matching keywords
                                content = f"{submission.title} {submission.selftext}".lower()
                                matched = [kw for kw in RELEVANCE_KEYWORDS 
                                         if kw.lower() in content]
                                
                                result = {
                                    'subreddit': subreddit_name,
                                    'id': submission.id,
                                    'title': submission.title,
                                    'author': str(submission.author),
                                    'created_utc': datetime.fromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                                    'permalink': f"https://reddit.com{submission.permalink}",
                                    'url': submission.url,
                                    'score': submission.score,
                                    'num_comments': submission.num_comments,
                                    'search_term': term,
                                    'snippet': extract_relevant_context(submission.selftext, term),
                                    'matched_keywords': ', '.join(matched) if matched else 'none'
                                }
                                results.append(result)
                                
                    except Exception as e:
                        logger.error(f"Error searching {subreddit_name} for query '{query}': {e}")
                        continue
                        
            except Exception as e:
                logger.error(f"Error accessing subreddit {subreddit_name}: {e}")
                continue

        # Deduplicate results
        unique_results = deduplicate_and_score_results(results)
        self.cache[cache_key] = unique_results
        self.save_cache()
        
        return unique_results

    async def close(self):
        """Close the Reddit client session"""
        await self.reddit.close()

async def batch_search_terms(terms: List[str]) -> List[Dict[str, Any]]:
    """
    Asynchronously search for terms in batches to manage rate limits and improve performance.
    
    Args:
        terms: List of search terms to process
        
    Returns:
        List of search results
    """
    batch_size = MAX_TERMS_PER_BATCH
    all_results = []
    rate_limiter = AdaptiveRateLimiter()
    searcher = RedditSearcher()
    
    try:
        for i in range(0, len(terms), batch_size):
            batch = terms[i:i + batch_size]
            batch_start = time.time()
            
            try:
                for term in batch:
                    await rate_limiter.wait()
                    try:
                        results = await searcher.search_term(term)
                        all_results.extend(results)
                        rate_limiter.success()
                    except Exception as e:
                        logger.error(f"Error searching term '{term}': {e}")
                        rate_limiter.failure()
                        continue
                    
                    # Check batch timeout
                    if time.time() - batch_start > BATCH_TIMEOUT:
                        logger.warning(f"Batch timeout reached after processing {len(all_results)} results")
                        break
                        
                logger.info(f"Completed batch {i//batch_size + 1}, processed {len(all_results)} results so far")
                
            except Exception as e:
                logger.error(f"Error processing batch starting at index {i}: {e}")
                continue
    finally:
        await searcher.close()
            
    return deduplicate_and_score_results(all_results)

def search_subreddits(terms: List[str]) -> List[Dict[str, Any]]:
    """
    Wrapper function to run async search in sync context with proper error handling.
    
    Args:
        terms: List of search terms
        
    Returns:
        List of search results
    """
    try:
        # Remove duplicates and very short terms
        filtered_terms = list({term for term in terms if len(term) > 3})
        logger.info(f"Starting search with {len(filtered_terms)} unique terms")
        
        return asyncio.run(batch_search_terms(filtered_terms))
    except KeyboardInterrupt:
        logger.info("Search interrupted by user")
        return []
    except Exception as e:
        logger.error(f"Search failed: {e}", exc_info=True)
        return []

def deduplicate_and_score_results(results: List[Dict]) -> List[Dict]:
    """
    Remove duplicates and score results by relevance with configurable scoring weights.
    
    Args:
        results: List of search result dictionaries
        
    Returns:
        List of deduplicated and scored results, sorted by relevance
    """
    seen_contents = {}
    scored_results = []
    
    for result in results:
        # Create a content key that captures the essence of the post
        content_key = f"{result['title']}:{result['snippet']}"
        if content_key in seen_contents:
            continue
            
        # Calculate relevance score
        score = 0
        content = f"{result['title']} {result['snippet']}".lower()
        
        # Score based on keyword matches (0-15 points)
        matched_keywords = result['matched_keywords'].split(', ')
        keyword_score = min(15, len(matched_keywords) * SCORING_CONFIG['keyword_match_weight'])
        score += keyword_score
        
        # Score based on industry term matches (0-15 points)
        industry_matches = sum(1 for term in INDUSTRY_TERMS if term in content)
        score += min(15, industry_matches * SCORING_CONFIG['industry_term_weight'])
        
        # Score based on recency (0-10 points)
        try:
            post_date = datetime.strptime(result['created_utc'], '%Y-%m-%d %H:%M:%S')
            days_old = (datetime.now() - post_date).days
            recency_score = SCORING_CONFIG['recency_max_score'] * (2 ** (-days_old / SCORING_CONFIG['recency_half_life']))
            score += recency_score
        except Exception as e:
            logger.warning(f"Error calculating recency score: {e}")
        
        # Score based on engagement (0-10 points)
        try:
            score += min(7, result['score'] * SCORING_CONFIG['upvote_weight'])  # Up to 7 points for upvotes
            score += min(3, result['num_comments'] * SCORING_CONFIG['comment_weight'])  # Up to 3 points for comments
        except Exception as e:
            logger.warning(f"Error calculating engagement score: {e}")
        
        # Bonus points for title matches (0-10 points)
        title_lower = result['title'].lower()
        if any(kw in title_lower for kw in matched_keywords):
            score += SCORING_CONFIG['title_match_bonus']
        
        # Normalize final score to 0-100 range
        result['relevance_score'] = min(100, score * 2)
        seen_contents[content_key] = True
        scored_results.append(result)
    
    # Sort by relevance score
    return sorted(scored_results, key=lambda x: x['relevance_score'], reverse=True)

def extract_relevant_context(text: str, search_term: str, window: int = 100) -> str:
    """
    Extract a snippet of text surrounding the search term.
    
    Args:
        text: The full text content.
        search_term: The term to search for.
        window: Number of characters to include before and after the term.
        
    Returns:
        A substring of text around the found search term.
    """
    index = text.lower().find(search_term.lower())
    if index == -1:
        return ""
    start = max(0, index - window)
    end = min(len(text), index + len(search_term) + window)
    return text[start:end]

def is_relevant_post(text: str) -> bool:
    """
    Determine if a post's text is relevant based on industry terms with improved matching.
    """
    # Primary terms - if any of these match, the post is relevant
    primary_terms = [
        'fishing vessel', 'fishing boat', 'trawler',
        'fishmeal', 'fish meal', 'fish oil',
        'processing plant', 'seafood processing',
        'commercial fishing', 'industrial fishing'
    ]
    
    # Secondary terms - need at least two of these to be relevant
    secondary_terms = [
        'vessel', 'boat', 'ship', 'fishing',
        'seafood', 'fishery', 'fisheries',
        'processing', 'industry', 'commercial',
        'marine', 'catch', 'fleet'
    ]
    
    # Exclude terms - if any of these match in context, the post is not relevant
    exclude_patterns = [
        r'aquarium\s+fish',
        r'pet\s+fish',
        r'fishing\s+game',
        r'fishing\s+rod',
        r'fishing\s+tackle',
        r'sport\s+fishing',
        r'recreational\s+fishing'
    ]
    
    text_lower = text.lower()
    
    # Check for primary terms first
    if any(term in text_lower for term in primary_terms):
        if not any(re.search(pattern, text_lower) for pattern in exclude_patterns):
            return True
    
    # If no primary terms, check for multiple secondary terms
    secondary_matches = sum(1 for term in secondary_terms if term in text_lower)
    if secondary_matches >= 2:
        if not any(re.search(pattern, text_lower) for pattern in exclude_patterns):
            return True
    
    return False

def is_relevant_timeframe(post_date: datetime) -> bool:
    """
    Check if a post's date is within the last 5 years.
    
    Args:
        post_date: The datetime object of the post.
        
    Returns:
        True if the post is recent enough, otherwise False.
    """
    cutoff_date = datetime.now() - timedelta(days=5*365)
    return post_date >= cutoff_date

def determine_keyword_category(keyword: str) -> str:
    """
    Determine whether a keyword corresponds to a facility or a vessel.
    
    Args:
        keyword: The search term keyword.
        
    Returns:
        'facility' if it matches facility indicators, 'vessel' otherwise.
    """
    facility_indicators = ['plant', 'processing', 'facility', 'factory', 'company']
    vessel_indicators = ['vessel', 'ship', 'trawler', 'boat', 'fishing']
    keyword_lower = keyword.lower()
    
    if any(ind in keyword_lower for ind in facility_indicators):
        return "facility"
    if any(ind in keyword_lower for ind in vessel_indicators):
        return "vessel"
    # Default to vessel if unsure
    return "vessel"

def generate_summary(results_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate summary statistics from the search results DataFrame.
    
    Args:
        results_df: DataFrame containing search result records.
        
    Returns:
        A dictionary summarizing key statistics.
    """
    if results_df.empty:
        return {
            'total_results': 0,
            'unique_posts': 0,
            'results_by_subreddit': {},
            'results_by_keyword': {},
            'top_terms': {},
            'date_range': {'earliest': None, 'latest': None},
            'engagement_stats': {'avg_score': 0, 'avg_comments': 0, 'max_score': 0, 'max_comments': 0},
            'relevance_stats': {'avg_relevance': 0, 'high_relevance_count': 0}
        }

    # Calculate high relevance threshold (top 25%)
    high_relevance_threshold = results_df['relevance_score'].quantile(0.75)

    return {
        'total_results': len(results_df),
        'unique_posts': results_df['id'].nunique(),
        'results_by_subreddit': results_df['subreddit'].value_counts().to_dict(),
        'results_by_keyword': results_df['matched_keywords'].value_counts().to_dict(),
        'top_terms': results_df['search_term'].value_counts().head(10).to_dict(),
        'date_range': {
            'earliest': results_df['created_utc'].min(),
            'latest': results_df['created_utc'].max()
        },
        'engagement_stats': {
            'avg_score': float(results_df['score'].mean()),
            'avg_comments': float(results_df['num_comments'].mean()),
            'max_score': int(results_df['score'].max()),
            'max_comments': int(results_df['num_comments'].max())
        },
        'relevance_stats': {
            'avg_relevance': float(results_df['relevance_score'].mean()),
            'high_relevance_count': int(results_df['relevance_score'].ge(high_relevance_threshold).sum())
        }
    }

def organize_search_results(search_results_csv: str, all_terms: List[str]) -> Dict[str, Any]:
    """
    Organize search results into facility and vessel categories.
    
    Args:
        search_results_csv: Path to the CSV file with search results.
        all_terms: List of all search terms to ensure each has an entry.
        
    Returns:
        A dictionary with organized data by keyword category.
    """
    organized_data: Dict[str, Any] = {
        "facility_keywords": {},
        "vessel_keywords": {}
    }

    # Initialize entries for all terms
    for term in all_terms:
        category = determine_keyword_category(term)
        if category == "facility":
            organized_data["facility_keywords"][term] = {"mentions": []}
        else:
            organized_data["vessel_keywords"][term] = {"mentions": []}

    try:
        with open(search_results_csv, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                keyword = row['search_term']
                category = determine_keyword_category(keyword)
                
                # Create the keyword entry if it doesn't exist
                if category == "facility":
                    if keyword not in organized_data["facility_keywords"]:
                        organized_data["facility_keywords"][keyword] = {"mentions": []}
                    target = organized_data["facility_keywords"][keyword]["mentions"]
                else:
                    if keyword not in organized_data["vessel_keywords"]:
                        organized_data["vessel_keywords"][keyword] = {"mentions": []}
                    target = organized_data["vessel_keywords"][keyword]["mentions"]

                mention = {
                    "subreddit": row['subreddit'],
                    "post_id": row['id'],
                    "title": row['title'],
                    "author": row['author'],
                    "created_utc": row['created_utc'],
                    "permalink": row['permalink'],
                    "snippet": row['snippet'],
                    "score": int(row['score']),
                    "num_comments": int(row['num_comments'])
                }
                target.append(mention)
    except Exception as e:
        logger.error(f"Error organizing search results: {e}", exc_info=True)
    return organized_data

def save_organized_results(organized_data: Dict[str, Any], output_file: str) -> None:
    """
    Save the organized search results to a JSON file.
    
    Args:
        organized_data: The dictionary containing organized search results.
        output_file: The path to the output JSON file.
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(organized_data, f, indent=2)
        logger.info("Organized results saved to %s", output_file)
    except Exception as e:
        logger.error("Failed to save organized results: %s", e, exc_info=True)

def main() -> None:
    """Main execution function"""
    try:
        # Validate environment variables first
        validate_env_vars()
        
        # Ensure output directory exists
        os.makedirs('data_out', exist_ok=True)
        
        # Configure logging to file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f'data_out/search_log_{timestamp}.txt'
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)
        
        logger.info("Starting search process...")
        
        # Load input files with better error handling
        try:
            plants_df = pd.read_csv('./data_in/Plants.csv')
            ships_df = pd.read_csv('./data_in/Ships.csv')
            
            if plants_df.empty:
                raise ValueError("Plants.csv is empty")
            if ships_df.empty:
                raise ValueError("Ships.csv is empty")
                
            logger.info(f"Loaded {len(plants_df)} plant records and {len(ships_df)} ship records")
            
        except FileNotFoundError as e:
            logger.error(f"Input file not found: {e}")
            return
        except pd.errors.EmptyDataError as e:
            logger.error(f"Input file is empty: {e}")
            return
        except Exception as e:
            logger.error(f"Error loading input files: {e}", exc_info=True)
            return

        # Get sample size from environment with validation
        try:
            sample_size = int(os.getenv('SEARCH_SAMPLE_SIZE', '10'))
            if sample_size <= 0:
                logger.warning("Invalid SEARCH_SAMPLE_SIZE, defaulting to 10")
                sample_size = 10
        except ValueError:
            logger.warning("Invalid SEARCH_SAMPLE_SIZE, defaulting to 10")
            sample_size = 10
        
        # Clean and prepare search terms with sample
        plant_terms = clean_search_terms(plants_df, 'Company name')[:sample_size]
        vessel_terms = clean_search_terms(ships_df, 'Vessel Name')[:sample_size]
        owner_terms = clean_search_terms(ships_df, 'Owner Name')[:sample_size]

        if not any([plant_terms, vessel_terms, owner_terms]):
            logger.error("No valid search terms found after cleaning")
            return

        logger.info(f"Prepared search terms (sample size {sample_size}) - Plants: {len(plant_terms)}, "
                   f"Vessels: {len(vessel_terms)}, Owners: {len(owner_terms)}")

        # Perform searches with better error handling
        all_results = []
        search_categories = [
            (plant_terms, 'plants'),
            (vessel_terms, 'vessels'),
            (owner_terms, 'owners')
        ]
        
        for terms, category in search_categories:
            if not terms:
                logger.warning(f"No terms to search for category: {category}")
                continue
                
            logger.info(f"Searching {category} terms...")
            try:
                results = search_subreddits(terms)
                if results:
                    logger.info(f"Found {len(results)} results for {category}")
                    all_results.extend(results)
                else:
                    logger.warning(f"No results found for {category}")
            except Exception as e:
                logger.error(f"Error searching {category}: {e}", exc_info=True)
                continue

        if not all_results:
            logger.warning("No results found in any category")
            return

        # Save results with error handling
        try:
            results_df = pd.DataFrame(all_results)
            results_csv = f'data_out/search_results_{timestamp}.csv'
            results_df.to_csv(results_csv, index=False)
            logger.info(f"Saved {len(results_df)} results to {results_csv}")

            # Generate and save summary
            summary = generate_summary(results_df)
            summary_json = f'data_out/search_summary_{timestamp}.json'
            with open(summary_json, 'w') as f:
                json.dump(summary, f, indent=2)
            logger.info(f"Saved summary to {summary_json}")

            # Organize and save results
            all_terms = plant_terms + vessel_terms + owner_terms
            organized_results = organize_search_results(results_csv, all_terms)
            organized_json = f'data_out/organized_results_{timestamp}.json'
            save_organized_results(organized_results, organized_json)

            logger.info("Search process completed successfully")
            
            # Print summary
            print("\nSearch Results Summary:")
            print(f"Total results found: {summary['total_results']}")
            print(f"Unique posts: {summary['unique_posts']}")
            print(f"High relevance results: {summary['relevance_stats']['high_relevance_count']}")
            print(f"Top subreddits: {dict(sorted(summary['results_by_subreddit'].items(), key=lambda x: x[1], reverse=True)[:5])}")

        except Exception as e:
            logger.error(f"Error saving results: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"Critical error in main process: {e}", exc_info=True)
    finally:
        logger.info("Process finished")
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

if __name__ == '__main__':
    main()
