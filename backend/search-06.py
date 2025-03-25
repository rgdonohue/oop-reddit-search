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
from typing import List, Dict, Any, Optional, Set

import pandas as pd
import praw
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables with better error handling
load_dotenv()

# Environment variable definitions
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'script:fishing_vessel_search:v1.0 (by /u/rgdonohue)')
REDDIT_AUTH_URL = 'https://www.reddit.com/api/v1/access_token'
REDDIT_SEARCH_URL = 'https://oauth.reddit.com/r/{}/search'
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
GOOGLE_CX = os.getenv('GOOGLE_SEARCH_CX')
SEARCH_LIMIT = 100  # max posts to retrieve per request
REQUEST_SLEEP = 1  # reduced delay between requests

# Validate required environment variables
def validate_env_vars() -> None:
    required_vars = {
        'REDDIT_CLIENT_ID': REDDIT_CLIENT_ID,
        'REDDIT_CLIENT_SECRET': REDDIT_CLIENT_SECRET
    }
    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

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

# Configuration for search batching and timeouts
MAX_TERMS_PER_BATCH = 50  # Maximum number of terms to process in one batch
BATCH_TIMEOUT = 300       # Maximum time (in seconds) to spend on each batch
SEARCH_TIMEOUT = 10       # Timeout for individual search requests
PROGRESS_INTERVAL = 10    # How often to log progress (in seconds)

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
        
    async def wait(self) -> None:
        """Wait appropriate amount of time before next request"""
        now = time.time()
        wait_time = max(0, (1.0 / self.current_rate) - (now - self.last_request))
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        self.last_request = time.time()
        
    def success(self) -> None:
        """Record a successful request"""
        self.success_count += 1
        self._adjust()
        
    def failure(self) -> None:
        """Record a failed request"""
        self.failure_count += 1
        self._adjust()
        
    def _adjust(self) -> None:
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
        
    def load(self) -> None:
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
                
    def save(self) -> None:
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
            
    def add_results(self, term: str, new_results: List[Dict]) -> None:
        """Add results for a completed term"""
        self.completed_terms.add(term)
        self.results.extend(new_results)
        # Save periodically (every 5 minutes)
        if time.time() - self.start_time >= 300:
            self.save()
            self.start_time = time.time()

def quick_similarity(a: str, b: str) -> float:
    """Fast similarity check using character and word-level matching"""
    if a == b:
        return 1.0
    a_lower = a.lower()
    b_lower = b.lower()
    if a_lower in b_lower or b_lower in a_lower:
        return 0.95
    if abs(len(a) - len(b)) / max(len(a), len(b)) > 0.5:
        return 0.0
    set_a = set(a_lower)
    set_b = set(b_lower)
    intersection = len(set_a.intersection(set_b))
    union = len(set_a.union(set_b))
    words_a = set(a_lower.split())
    words_b = set(b_lower.split())
    word_intersection = len(words_a.intersection(words_b))
    word_union = len(words_a.union(words_b))
    char_similarity = intersection / union if union > 0 else 0.0
    word_similarity = word_intersection / word_union if word_union > 0 else 0.0
    return 0.4 * char_similarity + 0.6 * word_similarity

def preprocess_search_terms(terms: List[str]) -> List[str]:
    """Group similar terms with less aggressive deduplication"""
    logger.info(f"Starting preprocessing of {len(terms)} terms...")
    normalized_terms = []
    chunk_size = 100
    for i in range(0, len(terms), chunk_size):
        chunk = terms[i:min(i + chunk_size, len(terms))]
        normalized_chunk = [term.strip() for term in chunk if term and len(term.strip()) >= 3]
        normalized_terms.extend(normalized_chunk)
        if i % 500 == 0:
            logger.info(f"Normalized {i}/{len(terms)} terms...")
    unique_terms = list(dict.fromkeys(normalized_terms))
    logger.info(f"Removed {len(normalized_terms) - len(unique_terms)} exact duplicates")
    sorted_terms = sorted(unique_terms, key=len)
    final_terms = []
    excluded_terms = set()
    batch_size = 50
    for i in range(0, len(sorted_terms), batch_size):
        batch = sorted_terms[i:i + batch_size]
        for term in batch:
            if term in excluded_terms:
                continue
            term_len = len(term)
            similar_found = False
            for existing in final_terms:
                if abs(len(existing) - term_len) > 10:
                    continue
                if quick_similarity(term, existing) > 0.9:
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
    common_words = {'the', 'and', 'of'}
    chunk_size = 1000
    cleaned_terms = set()
    total_rows = len(df)
    try:
        for chunk_start in range(0, total_rows, chunk_size):
            chunk = df.iloc[chunk_start:chunk_start + chunk_size]
            terms = chunk[name_column].dropna().astype(str).values
            for term in terms:
                if len(term.strip()) >= 3:
                    cleaned_terms.add(term.strip())
                cleaned = term.lower()
                cleaned = re.sub(r'[,\.\(\)\[\]{}]', ' ', cleaned)
                cleaned = re.sub(r'[^a-z0-9\s\-_]', '', cleaned)
                cleaned = ' '.join(cleaned.split())
                if ' ' in cleaned:
                    words = [w for w in cleaned.split() if w not in common_words and len(w) > 1]
                    cleaned = ' '.join(words)
                if len(cleaned) >= 3:
                    cleaned_terms.add(cleaned)
            if chunk_start % 5000 == 0:
                logger.info(f"Processed {chunk_start}/{total_rows} rows from {name_column}")
    except Exception as e:
        logger.error(f"Error cleaning search terms: {e}")
    return preprocess_search_terms(list(cleaned_terms))

class RedditSearcher:
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
        self.cache = {}
        self.cache_file = 'data_out/search_cache.json'
        self.load_cache()

    def load_cache(self) -> None:
        """Load cached results from file"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    self.cache = json.load(f)
                logger.info(f"Loaded {len(self.cache)} cached results")
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
            self.cache = {}

    def save_cache(self) -> None:
        """Save cache to file"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f)
            logger.info(f"Saved {len(self.cache)} results to cache")
        except Exception as e:
            logger.error(f"Error saving cache: {e}")

    def search_term(self, term: str, max_results: int = 100) -> List[Dict[str, Any]]:
        """Search for a term across subreddits using PRAW"""
        logger.info(f"Searching for term: {term}")
        cache_key = f"term_{term}"
        if cache_key in self.cache:
            logger.debug(f"Cache hit for term: {term}")
            return self.cache[cache_key]
        results = []
        for subreddit_name in SUBREDDIT_RECOMMENDATIONS:
            try:
                logger.debug(f"Searching subreddit: {subreddit_name}")
                subreddit = self.reddit.subreddit(subreddit_name)
                search_queries = [
                    term,
                    f'"{term}"',
                    f'{term} (fishing OR vessel OR boat)',
                    f'title:{term}'
                ]
                for query in search_queries:
                    try:
                        logger.debug(f"Executing search query: {query}")
                        for submission in subreddit.search(query, limit=max_results, sort='relevance'):
                            if is_relevant_post(submission.title + " " + submission.selftext):
                                content = f"{submission.title} {submission.selftext}".lower()
                                matched = [kw for kw in RELEVANCE_KEYWORDS if kw.lower() in content]
                                
                                # Extract snippet with more detailed logging
                                logger.debug(f"Extracting snippet for post: {submission.id}")
                                selftext = submission.selftext or ""
                                title = submission.title or ""
                                
                                # Try to extract from selftext first, if not available or no match, use title
                                snippet = extract_relevant_context(selftext, term)
                                if not snippet and title:
                                    logger.debug("No snippet found in selftext, trying title")
                                    snippet = extract_relevant_context(title, term)
                                    
                                # If still no snippet, extract from beginning of selftext or title
                                if not snippet:
                                    logger.debug("No direct match found, using beginning of content")
                                    if selftext:
                                        snippet = selftext[:150] + "..."
                                    elif title:
                                        snippet = title
                                
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
                                    'snippet': snippet,
                                    'matched_keywords': ', '.join(matched) if matched else 'none'
                                }
                                logger.debug(f"Found relevant post: {result['title']}")
                                logger.debug(f"Snippet length: {len(snippet)} chars")
                                results.append(result)
                    except Exception as e:
                        logger.error(f"Error searching {subreddit_name} for query '{query}': {e}")
                        continue
            except Exception as e:
                logger.error(f"Error accessing subreddit {subreddit_name}: {e}")
                continue
        unique_results = deduplicate_and_score_results(results)
        self.cache[cache_key] = unique_results
        self.save_cache()
        return unique_results

def deduplicate_and_score_results(results: List[Dict]) -> List[Dict]:
    """
    Remove duplicates and score results by relevance with adjusted scoring.
    """
    seen_contents = {}
    scored_results = []
    for result in results:
        content_key = f"{result['title']}:{result['snippet']}"
        if content_key in seen_contents:
            continue
        score = 0
        content = f"{result['title']} {result['snippet']}".lower()
        matched_keywords = result['matched_keywords'].split(', ')
        keyword_score = min(15, len(matched_keywords) * 3)
        score += keyword_score
        industry_terms = [
            'fishmeal', 'fish oil', 'fish meal',
            'reduction plant', 'processing facility', 'processing plant',
            'landing site', 'commercial fleet', 'pelagic vessel',
            'fishing vessel', 'fishing industry', 'seafood',
            'fish processing', 'marine harvest', 'fisheries'
        ]
        industry_matches = sum(1 for term in industry_terms if term in content)
        score += min(15, industry_matches * 3)
        try:
            post_date = datetime.strptime(result['created_utc'], '%Y-%m-%d %H:%M:%S')
            days_old = (datetime.now() - post_date).days
            recency_score = max(0, 10 - (days_old / 180))
            score += recency_score
        except:
            pass
        score += min(7, result['score'] / 50)
        score += min(3, result['num_comments'] / 5)
        title_lower = result['title'].lower()
        if any(kw in title_lower for kw in matched_keywords):
            score += 10
        result['relevance_score'] = min(100, score * 2)
        seen_contents[content_key] = True
        scored_results.append(result)
    return sorted(scored_results, key=lambda x: x['relevance_score'], reverse=True)

def search_terms(terms: List[str]) -> List[Dict[str, Any]]:
    """Search for multiple terms using PRAW"""
    searcher = RedditSearcher()
    all_results = []
    total_terms = len(terms)
    for i, term in enumerate(terms, 1):
        try:
            logger.info(f"Searching term {i}/{total_terms}: {term}")
            results = searcher.search_term(term)
            all_results.extend(results)
            if i % 10 == 0:
                logger.info(f"Completed {i}/{total_terms} terms")
            time.sleep(2)
        except Exception as e:
            logger.error(f"Error searching term '{term}': {e}")
            continue
    return deduplicate_and_score_results(all_results)

def extract_relevant_context(text: str, search_term: str, window: int = 100) -> str:
    """
    Extract a snippet of text surrounding the search term.
    """
    logger.debug(f"Extracting context for term: '{search_term}'")
    if not text or not isinstance(text, str):
        logger.debug(f"Invalid text to extract context from: {type(text)}")
        return ""
    
    # Try exact match first
    text_lower = text.lower()
    search_term_lower = search_term.lower()
    index = text_lower.find(search_term_lower)
    
    # If exact match fails, try word-by-word search
    if index == -1:
        logger.debug(f"Exact match for '{search_term}' not found, trying word search")
        words = search_term_lower.split()
        if len(words) > 1:
            for word in words:
                if len(word) > 3:  # Only consider meaningful words
                    word_index = text_lower.find(word)
                    if word_index != -1:
                        logger.debug(f"Found partial match with word: '{word}'")
                        index = word_index
                        break
    
    if index == -1:
        logger.debug(f"Term '{search_term}' not found in text (length: {len(text)})")
        # Return a small sample of the text if it's not empty
        if len(text) > 50:
            logger.debug(f"Returning sample of text instead")
            return text[:100] + "..."
        return ""
    
    start = max(0, index - window)
    end = min(len(text), index + len(search_term) + window)
    snippet = text[start:end]
    
    # Clean and format the snippet
    snippet = re.sub(r'\s+', ' ', snippet).strip()
    if len(snippet) > 5:
        logger.debug(f"Extracted snippet ({len(snippet)} chars): {snippet[:50]}...")
        return snippet
    else:
        logger.debug(f"Extracted snippet too short: {snippet}")
        return ""

def is_relevant_post(text: str) -> bool:
    """
    Determine if a post's text is relevant based on industry terms.
    """
    logger.debug("Checking post relevance")
    primary_terms = [
        'fishing vessel', 'fishing boat', 'trawler',
        'fishmeal', 'fish meal', 'fish oil',
        'processing plant', 'seafood processing',
        'commercial fishing', 'industrial fishing'
    ]
    secondary_terms = [
        'vessel', 'boat', 'ship', 'fishing',
        'seafood', 'fishery', 'fisheries',
        'processing', 'industry', 'commercial',
        'marine', 'catch', 'fleet'
    ]
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
    if any(term in text_lower for term in primary_terms):
        if not any(re.search(pattern, text_lower) for pattern in exclude_patterns):
            logger.debug("Post is relevant based on primary terms.")
            return True
    secondary_matches = sum(1 for term in secondary_terms if term in text_lower)
    if secondary_matches >= 2:
        if not any(re.search(pattern, text_lower) for pattern in exclude_patterns):
            logger.debug("Post is relevant based on secondary terms.")
            return True
    logger.debug("Post is not relevant.")
    return False

def is_relevant_timeframe(post_date: datetime) -> bool:
    """
    Check if a post's date is within the last 5 years.
    """
    cutoff_date = datetime.now() - timedelta(days=5*365)
    return post_date >= cutoff_date

def determine_keyword_category(keyword: str) -> str:
    """
    Determine whether a keyword corresponds to a facility or a vessel.
    """
    facility_indicators = ['plant', 'processing', 'facility', 'factory', 'company']
    vessel_indicators = ['vessel', 'ship', 'trawler', 'boat', 'fishing']
    keyword_lower = keyword.lower()
    if any(ind in keyword_lower for ind in facility_indicators):
        return "facility"
    if any(ind in keyword_lower for ind in vessel_indicators):
        return "vessel"
    return "vessel"

def generate_summary(results_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate summary statistics from the search results DataFrame.
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
    """
    organized_data: Dict[str, Any] = {
        "facility_keywords": {},
        "vessel_keywords": {}
    }
    for term in all_terms:
        category = determine_keyword_category(term)
        if category == "facility":
            organized_data["facility_keywords"][term] = {"mentions": []}
        else:
            organized_data["vessel_keywords"][term] = {"mentions": []}
    try:
        empty_snippets = 0
        total_rows = 0
        with open(search_results_csv, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_rows += 1
                keyword = row['search_term']
                category = determine_keyword_category(keyword)
                if category == "facility":
                    if keyword not in organized_data["facility_keywords"]:
                        organized_data["facility_keywords"][keyword] = {"mentions": []}
                    target = organized_data["facility_keywords"][keyword]["mentions"]
                else:
                    if keyword not in organized_data["vessel_keywords"]:
                        organized_data["vessel_keywords"][keyword] = {"mentions": []}
                    target = organized_data["vessel_keywords"][keyword]["mentions"]
                
                # Check if snippet is empty
                snippet = row['snippet']
                if not snippet or snippet.isspace():
                    empty_snippets += 1
                    logger.debug(f"Empty snippet for post ID: {row['id']} with search term: {keyword}")
                
                mention = {
                    "subreddit": row['subreddit'],
                    "post_id": row['id'],
                    "title": row['title'],
                    "author": row['author'],
                    "created_utc": row['created_utc'],
                    "permalink": row['permalink'],
                    "snippet": snippet,
                    "score": int(row['score']),
                    "num_comments": int(row['num_comments'])
                }
                target.append(mention)
        
        logger.info(f"Organized results: {empty_snippets}/{total_rows} empty snippets ({empty_snippets/total_rows*100:.1f}% if total_rows > 0)")
    except Exception as e:
        logger.error(f"Error organizing search results: {e}", exc_info=True)
    return organized_data

def save_organized_results(organized_data: Dict[str, Any], output_file: str) -> None:
    """
    Save the organized search results to a JSON file.
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(organized_data, f, indent=2)
        logger.info("Organized results saved to %s", output_file)
    except Exception as e:
        logger.error("Failed to save organized results: %s", e, exc_info=True)

# Define a stub for batch_search_terms as an async function
async def batch_search_terms(terms: List[str]) -> List[Dict[str, Any]]:
    """
    Asynchronously process search terms by running the synchronous search_terms function in a thread.
    """
    return await asyncio.to_thread(search_terms, terms)

def search_subreddits(terms: List[str]) -> List[Dict[str, Any]]:
    """Wrapper function to run async search in sync context"""
    try:
        filtered_terms = list({term for term in terms if len(term) > 3})
        logger.info(f"Starting search with {len(filtered_terms)} unique terms")
        return asyncio.run(batch_search_terms(filtered_terms))
    except KeyboardInterrupt:
        logger.info("Search interrupted by user")
        return []
    except Exception as e:
        logger.error(f"Search failed: {e}")
        return []

def main() -> None:
    """Main execution function"""
    try:
        # Validate environment variables
        validate_env_vars()
        
        # Ensure output directory exists
        os.makedirs('data_out', exist_ok=True)
        
        # Configure logging to file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f'data_out/search_log_{timestamp}.txt'
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)
        
        # Set up more verbose logging during development
        logger.setLevel(logging.DEBUG)
        
        logger.info("Starting search process...")
        
        # Load input files
        try:
            plants_df = pd.read_csv('./data_in/Plants.csv')
            ships_df = pd.read_csv('./data_in/Ships.csv')
            if plants_df.empty or ships_df.empty:
                raise ValueError("Input files are empty")
            logger.info(f"Loaded {len(plants_df)} plant records and {len(ships_df)} ship records")
        except Exception as e:
            logger.error(f"Error loading input files: {e}")
            return

        # Get sample size from environment or default to 10 for testing
        sample_size = int(os.getenv('SEARCH_SAMPLE_SIZE', '10'))
        plant_terms = plants_df['Company name'].dropna().unique().tolist()[:sample_size]
        vessel_terms = ships_df['Vessel Name'].dropna().unique().tolist()[:sample_size]
        owner_terms = ships_df['Owner Name'].dropna().unique().tolist()[:sample_size]
        logger.info(f"Prepared search terms (sample size {sample_size}) - Plants: {len(plant_terms)}, "
                    f"Vessels: {len(vessel_terms)}, Owners: {len(owner_terms)}")

        # Perform searches for different categories
        all_results = []
        for terms, category in [
            (plant_terms, 'plants'),
            (vessel_terms, 'vessels'),
            (owner_terms, 'owners')
        ]:
            logger.info(f"Searching {category} terms...")
            results = search_subreddits(terms)
            logger.info(f"Found {len(results)} results for {category}")
            all_results.extend(results)

        if not all_results:
            logger.warning("No results found")
            return

        # Save results
        results_df = pd.DataFrame(all_results)
        
        # Add snippet statistics
        empty_snippets = results_df['snippet'].isna().sum() + (results_df['snippet'] == '').sum()
        total_snippets = len(results_df)
        avg_snippet_length = results_df['snippet'].str.len().mean() if total_snippets > 0 else 0
        
        logger.info(f"Snippet statistics: {empty_snippets}/{total_snippets} empty snippets")
        logger.info(f"Average snippet length: {avg_snippet_length:.2f} characters")
        
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
        print("\nSearch Results Summary:")
        print(f"Total results found: {summary['total_results']}")
        print(f"Unique posts: {summary['unique_posts']}")
        print(f"High relevance results: {summary['relevance_stats']['high_relevance_count']}")
        print(f"Empty snippets: {empty_snippets}/{total_snippets} ({empty_snippets/total_snippets*100:.1f}%)")
        top_subreddits = dict(sorted(summary['results_by_subreddit'].items(), key=lambda x: x[1], reverse=True)[:5])
        print(f"Top subreddits: {top_subreddits}")

    except Exception as e:
        logger.error(f"Critical error in main process: {e}", exc_info=True)
    finally:
        logger.info("Process finished")
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

if __name__ == '__main__':
    main()
