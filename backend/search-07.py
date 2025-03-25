import os
import re
import time
import json
import logging
import csv
import hashlib
import asyncio
import aiofiles
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Dict, Any, Optional, Set, Tuple

import pandas as pd
import asyncpraw  # using asyncpraw for asynchronous Reddit queries
from dotenv import load_dotenv

# Set up logging with a more efficient format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s|%(levelname)s|%(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables once at startup
load_dotenv()

# Environment variables with defaults
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'script:fishing_vessel_search:v1.0')
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
GOOGLE_CX = os.getenv('GOOGLE_SEARCH_CX')
SEARCH_LIMIT = int(os.getenv('SEARCH_LIMIT', '100'))

def validate_env_vars() -> None:
    """Validate required environment variables exist"""
    required = {'REDDIT_CLIENT_ID': REDDIT_CLIENT_ID, 'REDDIT_CLIENT_SECRET': REDDIT_CLIENT_SECRET}
    missing = [var for var, val in required.items() if not val]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

# Optimized keyword sets using frozenset for immutability and faster lookups
RELEVANCE_KEYWORDS: frozenset = frozenset({
    'fishing vessel', 'fishing boat', 'trawler', 'commercial fishing',
    'fishmeal', 'fish meal', 'fish oil', 'processing plant', 'seafood processing',
    'aquaculture', 'marine harvest', 'catch', 'landing', 'industrial fishing',
    'fishing industry', 'seafood', 'fish processing', 'fishing company',
    'marine resources', 'fisheries', 'fishing fleet', 'commercial vessel'
})

SUBREDDIT_RECOMMENDATIONS: tuple = (
    'Fishing', 'worldnews', 'news', 'business',
    'environment', 'science', 'technology',
    'investing', 'maritime', 'shipping',
    'ocean', 'sustainability'
)

def setup_logging() -> None:
    """Set up logging configuration"""
    os.makedirs('logs', exist_ok=True)
    file_handler = logging.FileHandler(f'logs/search_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    file_handler.setFormatter(logging.Formatter('%(asctime)s|%(levelname)s|%(message)s'))
    logger.addHandler(file_handler)
    logger.info("Logging initialized")

def cleanup_logging() -> None:
    """Clean up logging handlers"""
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)

def load_input_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load input data from CSV files"""
    try:
        # First try with backend prefix (run from project root)
        try:
            plants_df = pd.read_csv('backend/data_in/Plants.csv', encoding='utf-8')
            ships_df = pd.read_csv('backend/data_in/Ships.csv', encoding='utf-8')
            logger.info(f"Loaded {len(plants_df)} plants and {len(ships_df)} ships from project root path")
        except FileNotFoundError:
            # Then try without prefix (run from backend directory)
            plants_df = pd.read_csv('data_in/Plants.csv', encoding='utf-8')
            ships_df = pd.read_csv('data_in/Ships.csv', encoding='utf-8')
            logger.info(f"Loaded {len(plants_df)} plants and {len(ships_df)} ships from backend directory path")
        
        return plants_df, ships_df
    except FileNotFoundError as e:
        logger.error(f"Input data file not found: {e}")
        # Create empty DataFrames as fallback
        return pd.DataFrame(columns=['Company name']), pd.DataFrame(columns=['Vessel Name'])
    except Exception as e:
        logger.error(f"Error loading input data: {e}")
        return pd.DataFrame(columns=['Company name']), pd.DataFrame(columns=['Vessel Name'])

def prepare_search_terms(plants_df: pd.DataFrame, ships_df: pd.DataFrame) -> List[str]:
    """Prepare search terms from input data"""
    # First detect the appropriate column names
    plant_name_col = detect_name_column(plants_df, ['Company name', 'name', 'Name', 'CompanyName', 'Company'])
    ship_name_col = detect_name_column(ships_df, ['Vessel Name', 'name', 'Name', 'VesselName', 'Ship Name'])
    
    if plant_name_col:
        plant_terms = clean_search_terms(plants_df, plant_name_col)
    else:
        logger.warning("No valid name column found in plants data, skipping")
        plant_terms = []
        
    if ship_name_col:
        ship_terms = clean_search_terms(ships_df, ship_name_col)
    else:
        logger.warning("No valid name column found in ships data, skipping")
        ship_terms = []
    
    # Combine terms and remove duplicates
    all_terms = list(set(plant_terms + ship_terms))
    logger.info(f"Prepared {len(all_terms)} search terms")
    return all_terms

def detect_name_column(df: pd.DataFrame, possible_names: List[str]) -> Optional[str]:
    """Try to find a valid name column from a list of possible options"""
    # First try exact matches
    for col_name in possible_names:
        if col_name in df.columns:
            logger.info(f"Found column: {col_name}")
            return col_name
    
    # Then try case-insensitive matches
    df_cols_lower = [col.lower() for col in df.columns]
    for col_name in possible_names:
        if col_name.lower() in df_cols_lower:
            idx = df_cols_lower.index(col_name.lower())
            actual_col = df.columns[idx]
            logger.info(f"Found column with case difference: {actual_col}")
            return actual_col
            
    # If we still haven't found a match, use the first column as a fallback
    if len(df.columns) > 0:
        logger.warning(f"No matching column found. Using first column as fallback: {df.columns[0]}")
        return df.columns[0]
        
    # If the DataFrame is empty, we have a problem
    logger.error("DataFrame has no columns")
    return None

async def process_search_terms(terms: List[str]) -> List[Dict[str, Any]]:
    """Process all search terms and collect results"""
    logger.info(f"Processing {len(terms)} search terms")
    start_time = time.time()
    
    # Start the search process
    results = await async_search_terms(terms)
    
    # Log completion details
    elapsed = time.time() - start_time
    logger.info(f"Found {len(results)} results in {elapsed:.2f} seconds")
    return results

async def save_results(results: List[Dict[str, Any]]) -> None:
    """Save search results to file"""
    try:
        # Try both potential directories
        os.makedirs('data_out', exist_ok=True)
        os.makedirs('backend/data_out', exist_ok=True)
        
        # Save as JSON in both potential locations to ensure it works
        try:
            async with aiofiles.open('data_out/results.json', 'w') as f:
                await f.write(json.dumps(results, indent=2))
                
            # Save as CSV
            with open('data_out/results.csv', 'w', newline='', encoding='utf-8') as csvfile:
                if results:
                    fieldnames = list(results[0].keys())
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(results)
                    
            logger.info(f"Saved {len(results)} results to data_out/")
        except:
            # Fallback path
            async with aiofiles.open('backend/data_out/results.json', 'w') as f:
                await f.write(json.dumps(results, indent=2))
                
            # Save as CSV
            with open('backend/data_out/results.csv', 'w', newline='', encoding='utf-8') as csvfile:
                if results:
                    fieldnames = list(results[0].keys())
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(results)
                    
            logger.info(f"Saved {len(results)} results to backend/data_out/")
    except Exception as e:
        logger.error(f"Error saving results: {e}")

def print_summary(results: List[Dict[str, Any]]) -> None:
    """Print summary of search results"""
    if not results:
        print("No results found.")
        return
        
    print(f"\nFound {len(results)} relevant results.")
    print("\nTop 5 highest scored results:")
    
    for i, result in enumerate(results[:5], 1):
        print(f"\n{i}. {result['title']} (Score: {result['relevance_score']})")
        print(f"   {result['url']}")
        print(f"   {result['snippet'][:100]}...")

def calculate_result_score(result: Dict[str, Any]) -> float:
    """Calculate relevance score for a result"""
    score = 0.0
    
    # Base score from Reddit data
    if 'ups' in result and isinstance(result['ups'], (int, float)):
        score += min(result['ups'] / 100, 10)
        
    # Check for relevant keywords in title and text
    content = f"{result.get('title', '')} {result.get('snippet', '')}"
    content_lower = content.lower()
    
    for keyword in RELEVANCE_KEYWORDS:
        if keyword in content_lower:
            score += 5
            
    # Recent content gets higher score
    if 'created_utc' in result:
        try:
            created_date = datetime.fromtimestamp(result['created_utc'])
            days_old = (datetime.now() - created_date).days
            if days_old < 30:
                score += 10
            elif days_old < 90:
                score += 5
            elif days_old < 365:
                score += 2
        except:
            pass
            
    return max(1, min(50, score))

def extract_relevant_context(text: str, search_term: str, window: int = 100) -> str:
    """Extract relevant context with optimized text processing"""
    if not text or not isinstance(text, str):
        return ""
    
    text_lower = text.lower()
    search_term_lower = search_term.lower()
    
    # Use more efficient string search
    try:
        index = text_lower.index(search_term_lower)
    except ValueError:
        # Fallback to word search
        for word in search_term_lower.split():
            if len(word) > 3:
                try:
                    index = text_lower.index(word)
                    break
                except ValueError:
                    continue
        else:
            return text[:150] + "..." if len(text) > 150 else text
    
    start = max(0, index - window)
    end = min(len(text), index + len(search_term) + window)
    snippet = text[start:end].strip()
    return ' '.join(snippet.split()) or text[:150]

class AdaptiveRateLimiter:
    """Improved rate limiter with exponential backoff"""
    def __init__(self, initial_rate: float = 10.0, min_rate: float = 5.0, max_rate: float = 30.0):
        self.current_rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.success_count = 0
        self.failure_count = 0
        self.last_adjust = time.monotonic()
        self.last_request = time.monotonic()
        self.backoff_time = 1.0
        self.consecutive_429 = 0
        
    async def wait(self) -> None:
        now = time.monotonic()
        wait_time = max(0, (1.0 / self.current_rate) - (now - self.last_request))
        
        # Additional wait if we've had 429 errors
        if self.consecutive_429 > 0:
            wait_time += self.backoff_time
            
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        self.last_request = time.monotonic()
        
    def success(self) -> None:
        self.success_count += 1
        self.consecutive_429 = 0  # Reset consecutive 429 counter
        self.backoff_time = 1.0   # Reset backoff time
        self._adjust()
        
    def failure(self, status_code: int = None) -> None:
        self.failure_count += 1
        
        # If we got a 429 error, increase backoff time exponentially
        if status_code == 429:
            self.consecutive_429 += 1
            # Exponential backoff: double the wait time for each consecutive 429
            self.backoff_time = min(60, self.backoff_time * 2)
            # Also reduce the rate immediately
            self.current_rate = max(self.min_rate, self.current_rate * 0.5)
            logger.warning(f"Rate limit hit. Backoff for {self.backoff_time}s, new rate: {self.current_rate:.1f}/min")
        
        self._adjust()
        
    def _adjust(self) -> None:
        now = time.monotonic()
        if now - self.last_adjust >= 60:
            total = self.success_count + self.failure_count
            if total >= 5:  # Reduced threshold to react faster
                success_rate = self.success_count / total
                if success_rate > 0.95 and self.consecutive_429 == 0:
                    self.current_rate = min(self.current_rate * 1.1, self.max_rate)
                elif success_rate < 0.8:
                    self.current_rate = max(self.current_rate * 0.5, self.min_rate)
            self.success_count = self.failure_count = 0
            self.last_adjust = now

def quick_similarity(a: str, b: str) -> float:
    """Optimized string similarity check"""
    if a == b:
        return 1.0
    
    a_lower = a.lower()
    b_lower = b.lower()
    
    if a_lower in b_lower or b_lower in a_lower:
        return 0.95
        
    len_diff = abs(len(a) - len(b)) / max(len(a), len(b))
    if len_diff > 0.5:
        return 0.0
        
    # Use sets for faster operations
    chars_a = set(a_lower)
    chars_b = set(b_lower)
    words_a = frozenset(a_lower.split())
    words_b = frozenset(b_lower.split())
    
    char_sim = len(chars_a & chars_b) / len(chars_a | chars_b)
    word_sim = len(words_a & words_b) / len(words_a | words_b)
    
    return 0.4 * char_sim + 0.6 * word_sim

def preprocess_search_terms(terms: List[str]) -> List[str]:
    """More efficient search term preprocessing"""
    # Use sets for faster duplicate removal
    unique_terms = {term.strip() for term in terms if term and len(term.strip()) >= 3}
    sorted_terms = sorted(unique_terms, key=len)
    
    final_terms = []
    excluded = set()
    
    # Process in larger batches
    batch_size = 100
    for i in range(0, len(sorted_terms), batch_size):
        batch = sorted_terms[i:i + batch_size]
        for term in batch:
            if term in excluded:
                continue
            if not any(quick_similarity(term, existing) > 0.9 for existing in final_terms):
                final_terms.append(term)
            else:
                excluded.add(term)
                
    return final_terms

def clean_search_terms(df: pd.DataFrame, name_column: str) -> List[str]:
    """More efficient term cleaning with vectorized operations"""
    common_words = frozenset({'the', 'and', 'of'})
    
    # Use pandas vectorized operations
    terms = df[name_column].dropna().astype(str)
    cleaned_terms = set(terms.str.strip())
    
    # Clean terms in batches
    cleaned = terms.str.lower()
    cleaned = cleaned.str.replace(r'[,\.\(\)\[\]{}]', ' ', regex=True)
    cleaned = cleaned.str.replace(r'[^a-z0-9\s\-_]', '', regex=True)
    cleaned = cleaned.str.split().apply(lambda x: ' '.join(w for w in x if w not in common_words and len(w) > 1))
    
    cleaned_terms.update(cleaned[cleaned.str.len() >= 3])
    
    return preprocess_search_terms(list(cleaned_terms))

class AsyncRedditSearcher:
    """Improved Reddit searcher with connection pooling and caching"""
    def __init__(self):
        self.reddit = asyncpraw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
        self.cache = {}
        # Try both potential cache locations
        self.cache_file = 'data_out/search_cache.json'
        self.alt_cache_file = 'backend/data_out/search_cache.json'
        self.rate_limiter = AdaptiveRateLimiter()
        
    async def __aenter__(self):
        # No need to initialize session separately for asyncpraw
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Close the Reddit instance if needed
        await self.reddit.close()
            
    async def load_cache(self) -> None:
        """Load cache with error handling"""
        try:
            # Try primary location first
            if os.path.exists(self.cache_file):
                async with aiofiles.open(self.cache_file, 'r') as f:
                    self.cache = json.loads(await f.read())
                    logger.info(f"Loaded {len(self.cache)} items from cache at {self.cache_file}")
            # Try alternate location if primary failed
            elif os.path.exists(self.alt_cache_file):
                async with aiofiles.open(self.alt_cache_file, 'r') as f:
                    self.cache = json.loads(await f.read())
                    logger.info(f"Loaded {len(self.cache)} items from cache at {self.alt_cache_file}")
            else:
                logger.info("No cache file found, starting with empty cache")
                self.cache = {}
        except Exception as e:
            logger.error(f"Cache load error: {e}")
            self.cache = {}
            
    async def save_cache(self) -> None:
        """Save cache asynchronously"""
        try:
            # Try to create directories for both potential locations
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            
            try:
                # Try primary location first
                async with aiofiles.open(self.cache_file, 'w') as f:
                    await f.write(json.dumps(self.cache))
                logger.info(f"Saved cache to {self.cache_file}")
            except:
                # Fallback to alternate location
                os.makedirs(os.path.dirname(self.alt_cache_file), exist_ok=True)
                async with aiofiles.open(self.alt_cache_file, 'w') as f:
                    await f.write(json.dumps(self.cache))
                logger.info(f"Saved cache to {self.alt_cache_file}")
        except Exception as e:
            logger.error(f"Cache save error: {e}")
    
    async def process_submission(self, submission, term: str) -> bool:
        """Process a Reddit submission to determine if it's relevant"""
        # Check basic relevance
        if not submission.title or not submission.selftext:
            return False
            
        # Check for keyword relevance
        content = f"{submission.title.lower()} {submission.selftext.lower()}"
        if term.lower() not in content:
            return False
            
        # Check if it has enough relevant keywords
        relevance_count = sum(1 for keyword in RELEVANCE_KEYWORDS if keyword in content)
        return relevance_count >= 1
    
    async def format_result(self, submission, term: str) -> Dict[str, Any]:
        """Format a submission as a result dictionary"""
        # Extract relevant context
        snippet = extract_relevant_context(submission.selftext, term)
        
        # Format the result
        return {
            'title': submission.title,
            'url': f"https://www.reddit.com{submission.permalink}",
            'snippet': snippet,
            'search_term': term,
            'subreddit': submission.subreddit.display_name,
            'created_utc': submission.created_utc,
            'ups': submission.ups,
            'num_comments': submission.num_comments
        }
            
    async def search_subreddit(self, subreddit_name: str, term: str, search_queries: List[str], max_results: int):
        """Helper method to search a specific subreddit with proper error handling"""
        local_results = []  # Create a local results list for this subreddit
        try:
            subreddit = await self.reddit.subreddit(subreddit_name)
            for query in search_queries:
                await self.rate_limiter.wait()
                try:
                    async for submission in subreddit.search(query, limit=max_results):
                        try:
                            if await self.process_submission(submission, term):
                                local_results.append(await self.format_result(submission, term))
                                self.rate_limiter.success()
                        except Exception as e:
                            logger.error(f"Error processing submission: {e}")
                except Exception as e:
                    # Check for rate limit error
                    if "429" in str(e):
                        self.rate_limiter.failure(status_code=429)
                        logger.warning(f"Rate limited on subreddit {subreddit_name}, query: {query}")
                        # Skip this query attempt and continue with the next
                        continue
                    else:
                        self.rate_limiter.failure()
                        logger.error(f"Search error on subreddit {subreddit_name}, query: {query} - {e}")
        except Exception as e:
            logger.error(f"Subreddit error: {subreddit_name} - {e}")
            self.rate_limiter.failure()
        
        return local_results

    async def search_term(self, term: str, max_results: int = SEARCH_LIMIT) -> List[Dict[str, Any]]:
        """Improved search with better error handling and caching"""
        cache_key = f"term_{hashlib.md5(term.encode()).hexdigest()}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        results = []
        search_queries = [term, f'"{term}"', f'{term} (fishing OR vessel OR boat)', f'title:{term}']
        
        # Create search tasks but limit concurrent searches to avoid overwhelming Reddit
        search_tasks = []
        for sr in SUBREDDIT_RECOMMENDATIONS:
            search_tasks.append(self.search_subreddit(sr, term, search_queries, max_results))
        
        # Execute searches with more conservative concurrency
        for i in range(0, len(search_tasks), 3):  # Process 3 subreddits at a time
            batch = search_tasks[i:i+3]
            batch_results = await asyncio.gather(*batch)
            # Add results from each subreddit
            for subreddit_results in batch_results:
                results.extend(subreddit_results)
            # Brief pause between batches
            await asyncio.sleep(1.0)
        
        unique_results = deduplicate_and_score_results(results)
        self.cache[cache_key] = unique_results
        await self.save_cache()
        return unique_results

async def async_search_terms(terms: List[str]) -> List[Dict[str, Any]]:
    """Concurrent term searching with improved error handling"""
    async with AsyncRedditSearcher() as searcher:
        await searcher.load_cache()
        
        # Use a very limited number of concurrent tasks to avoid overloading the Reddit API
        max_concurrent = 2  # Reduced from 5 to 2
        all_results = []
        total_terms = len(terms)
        
        # Process in smaller batches with more space between them
        for i in range(0, total_terms, max_concurrent):
            batch = terms[i:i + max_concurrent]
            batch_num = i // max_concurrent + 1
            total_batches = (total_terms + max_concurrent - 1) // max_concurrent
            progress = (i / total_terms) * 100
            
            logger.info(f"Processing terms {i+1}-{i+len(batch)} of {total_terms} (Batch {batch_num}/{total_batches}, {progress:.1f}%)")
            print(f"Processing batch {batch_num}/{total_batches} ({progress:.1f}% complete)...")
            
            # Create tasks for each term in the batch
            tasks = [searcher.search_term(term) for term in batch if len(term) > 3]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Add valid results to all_results
            batch_result_count = 0
            for result in batch_results:
                if isinstance(result, list):
                    all_results.extend(result)
                    batch_result_count += len(result)
                elif isinstance(result, Exception):
                    logger.error(f"Term search error: {result}")
            
            print(f"  Found {batch_result_count} results in this batch. Total results so far: {len(all_results)}")
            logger.info(f"Batch {batch_num} complete. Added {batch_result_count} results. Total: {len(all_results)}")
            
            # Sleep longer between batches to avoid rate limiting
            if i + max_concurrent < total_terms:  # Skip waiting after last batch
                seconds_to_wait = 3.0
                print(f"  Waiting {seconds_to_wait}s before next batch...")
                await asyncio.sleep(seconds_to_wait)
        
        print(f"Search completed. Found {len(all_results)} total results.")
        return deduplicate_and_score_results(all_results)

def search_subreddits(terms: List[str]) -> List[Dict[str, Any]]:
    """Main search function with better error handling"""
    try:
        filtered_terms = list({term for term in terms if len(term) > 3})
        return asyncio.run(async_search_terms(filtered_terms))
    except Exception as e:
        logger.error(f"Search failed: {e}")
        return []

def deduplicate_and_score_results(results: List[Dict]) -> List[Dict]:
    """More efficient result deduplication and scoring"""
    seen = set()
    scored = []
    
    for result in results:
        content_key = f"{result['title']}:{result['snippet']}"
        if content_key in seen:
            continue
            
        score = calculate_result_score(result)
        result['relevance_score'] = min(100, score * 2)
        seen.add(content_key)
        scored.append(result)
        
    return sorted(scored, key=lambda x: x['relevance_score'], reverse=True)

async def async_main():
    """Improved main async function"""
    validate_env_vars()
    setup_logging()
    
    results = []
    try:
        # Create data_out directory if it doesn't exist (try both paths)
        os.makedirs('data_out', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        
        logger.info("Starting Reddit search for fishing vessels and plants")
        print("=" * 80)
        print("STARTING REDDIT SEARCH FOR FISHING VESSELS AND PLANTS")
        print("=" * 80)
        print("This process may take several hours due to Reddit's rate limits.")
        print("You can cancel anytime with Ctrl+C - results will be saved in cache.\n")
        
        start_time = time.time()
        plants_df, ships_df = load_input_data()
        search_terms = prepare_search_terms(plants_df, ships_df)
        
        print(f"Loaded {len(plants_df)} plants and {len(ships_df)} ships")
        print(f"Prepared {len(search_terms)} search terms to process\n")
        
        if not search_terms:
            print("Error: No valid search terms could be extracted from the input data.")
            logger.error("No valid search terms found in input data")
            return
        
        results = await process_search_terms(search_terms)
        await save_results(results)
        
        elapsed = time.time() - start_time
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        
        print(f"\nSearch completed in {hours}h {minutes}m {seconds}s!")
        print(f"Results saved to data_out/\n")
        
        print_summary(results)
    except KeyboardInterrupt:
        print("\nSearch interrupted by user. Saving partial results...")
        logger.info("Search interrupted by user.")
        
        if results:
            await save_results(results)
            print(f"Saved {len(results)} partial results to data_out/")
    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)
        print(f"Error: {e}")
        
        # Try to save partial results if available
        if results:
            try:
                await save_results(results)
                print(f"Saved {len(results)} partial results despite error")
            except:
                logger.error("Failed to save partial results after error")
    finally:
        cleanup_logging()

if __name__ == '__main__':
    asyncio.run(async_main())
