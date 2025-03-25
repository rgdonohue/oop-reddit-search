import os
import re
import time
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Dict, Any, Optional, Set
import csv

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Constants
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = 'script:fishing_vessel_search:v1.0 (by /u/your_actual_username)'
REDDIT_AUTH_URL = 'https://www.reddit.com/api/v1/access_token'
REDDIT_SEARCH_URL = 'https://oauth.reddit.com/r/{}/search'
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
GOOGLE_CX = os.getenv('GOOGLE_SEARCH_CX')
SEARCH_LIMIT = 100  # max posts to retrieve per request
REQUEST_SLEEP = 2  # seconds delay between requests

# Updated relevance keywords
RELEVANCE_KEYWORDS: Set[str] = {
    'fishing vessel', 'fishing boat', 'trawler', 'commercial fishing',
    'fishmeal', 'fish meal', 'fish oil', 'processing plant', 'seafood processing',
    'aquaculture', 'marine harvest', 'catch', 'landing', 'industrial fishing'
}

# Recommended subreddits
SUBREDDIT_RECOMMENDATIONS: List[str] = [
    'r/Fishing', 'r/CommercialFishing', 'r/OceanFishing', 'r/Seafood',
    'r/MarineBiology', 'r/MarineConservation', 'r/Maritime',
    'r/EnvironmentalScience', 'r/WorldNews', 'r/News', 'r/Aquaculture',
    'r/Sustainability', 'r/Oceans', 'r/FishingIndustry', 'r/AquaticAgriculture',
    'r/FoodProduction', 'r/SupplyChain', 'r/Industry', 'r/Business', 'r/GlobalTrade'
]

def clean_search_terms(df: pd.DataFrame, name_column: str) -> List[str]:
    """
    Clean and extract unique search terms from the given dataframe column.
    """
    common_words = {'the', 'ltd', 'inc', 'corporation', 'company', 'sa', 'sac', 'and',
                    'of', 'co', 'group', 'international', 'trading', 'enterprises',
                    'industries', 'limited', 'corp', 'services'}
    special_chars = r'[,\.&\(\)\[\]{}\-_/\\]'

    terms = df[name_column].dropna().unique().tolist()
    cleaned_terms = []

    for term in terms:
        if isinstance(term, str):
            # Normalize term: lowercase, remove special characters
            term = re.sub(special_chars, ' ', term.lower())
            words = [w for w in term.split() if w not in common_words]
            cleaned = ' '.join(words).strip()
            # Only include terms that are sufficiently long and not purely numeric
            if len(cleaned) > 3 and not cleaned.isdigit() and not all(c.isdigit() or c.isspace() for c in cleaned):
                cleaned_terms.append(cleaned)
    return list(set(cleaned_terms))

def get_reddit_token() -> Optional[str]:
    """
    Obtain a Reddit API access token.
    """
    auth = requests.auth.HTTPBasicAuth(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET)
    data = {'grant_type': 'client_credentials'}
    headers = {'User-Agent': REDDIT_USER_AGENT}

    try:
        response = requests.post(REDDIT_AUTH_URL, auth=auth, data=data, headers=headers, timeout=10)
        response.raise_for_status()
        token = response.json().get('access_token')
        if not token:
            logger.error("No token received: %s", response.json())
            return None
        logger.info("Successfully obtained Reddit token")
        return token
    except Exception as e:
        logger.error("Failed to get Reddit token: %s", e, exc_info=True)
        return None

def create_session() -> requests.Session:
    """
    Create a requests session with retry logic.
    """
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def extract_relevant_context(text, search_term, window=100):
    """
    Extract text window around relevant mentions
    window: number of characters before/after match
    """
    index = text.lower().find(search_term.lower())
    if index == -1:
        return ""
    
    start = max(0, index - window)
    end = min(len(text), index + len(search_term) + window)
    
    return text[start:end]

def search_subreddits(terms: List[str]) -> List[Dict[str, Any]]:
    """
    Search for provided terms across recommended subreddits.
    """
    search_results = []
    seen_posts: Set[str] = set()
    session = create_session()
    token = get_reddit_token()
    if not token:
        logger.error("Reddit token unavailable")
        return []

    headers = {
        'User-Agent': REDDIT_USER_AGENT,
        'Authorization': f'Bearer {token}'
    }

    for term in terms:
        logger.info("Searching for term: '%s'", term)
        search_start = datetime.now()
        for subreddit in SUBREDDIT_RECOMMENDATIONS:
            subreddit_name = subreddit.replace('r/', '')
            logger.info("Searching in subreddit: '%s'", subreddit_name)
            try:
                url = REDDIT_SEARCH_URL.format(subreddit_name)
                params = {
                    'q': term,
                    'limit': SEARCH_LIMIT,
                    'sort': 'new',
                    'restrict_sr': 'true'
                }
                response = session.get(url, params=params, headers=headers, timeout=10)
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning("Rate limited in '%s'. Sleeping %s seconds", subreddit_name, retry_after)
                    time.sleep(retry_after)
                    response = session.get(url, params=params, headers=headers, timeout=10)
                
                if response.status_code != 200:
                    logger.error("HTTP %s error in '%s': %s", response.status_code, subreddit_name, response.text)
                    continue

                data = response.json()
                posts = data.get('data', {}).get('children', [])
                for post in posts:
                    post_data = post.get('data', {})
                    post_id = post_data.get('id')
                    if not post_id or post_id in seen_posts:
                        continue

                    content = f"{post_data.get('title', '')} {post_data.get('selftext', '')}"
                    lower_content = content.lower()
                    matched_keywords = [kw for kw in RELEVANCE_KEYWORDS if kw in lower_content]
                    if matched_keywords:
                        seen_posts.add(post_id)
                        snippet = extract_relevant_context(content, term)
                        result = {
                            'subreddit': subreddit_name,
                            'id': post_id,
                            'title': post_data.get('title', ''),
                            'author': post_data.get('author', ''),
                            'created_utc': datetime.fromtimestamp(post_data.get('created_utc', 0)).strftime('%Y-%m-%d %H:%M:%S'),
                            'permalink': f"https://reddit.com{post_data.get('permalink', '')}",
                            'snippet': snippet,
                            'search_term': term,
                            'matched_keywords': ', '.join(matched_keywords),
                            'score': post_data.get('score', 0),
                            'num_comments': post_data.get('num_comments', 0)
                        }
                        search_results.append(result)
            except Exception as e:
                logger.error("Error searching '%s': %s", subreddit_name, e, exc_info=True)
            time.sleep(REQUEST_SLEEP)
        duration = datetime.now() - search_start
        logger.info("Completed search for '%s' in %s", term, duration)
    return search_results

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
            'engagement_stats': {'avg_score': 0, 'avg_comments': 0, 'max_score': 0, 'max_comments': 0}
        }
    
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
        }
    }

def is_relevant_post(text):
    industry_terms = [
        'fishmeal',
        'fish oil',
        'reduction plant',
        'processing facility',
        'landing site',
        'commercial fleet',
        'pelagic vessel'
    ]
    
    # Require at least one industry term
    if not any(term in text.lower() for term in industry_terms):
        return False
        
    # Exclude common false positives
    exclude_terms = [
        'aquarium',
        'pet food',
        'recreational fishing',
        'sport fishing'
    ]
    
    if any(term in text.lower() for term in exclude_terms):
        return False
        
    return True

def is_relevant_timeframe(post_date):
    # Focus on posts within last 5 years
    cutoff_date = datetime.now() - timedelta(days=5*365)
    return post_date >= cutoff_date

def determine_keyword_category(keyword):
    # Implement your logic here to determine if a keyword matches a facility or vessel
    pass

def organize_search_results(search_results_csv):
    # Initialize structure
    organized_data = {
        "facility_keywords": {},
        "vessel_keywords": {}
    }
    
    # Read CSV
    with open(search_results_csv, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Determine if result matches facility or vessel
            keyword = row['search_term']
            category = determine_keyword_category(keyword)
            
            # Create keyword entry if it doesn't exist
            if category == "facility":
                if keyword not in organized_data["facility_keywords"]:
                    organized_data["facility_keywords"][keyword] = {"mentions": []}
                target = organized_data["facility_keywords"][keyword]["mentions"]
            else:  # vessel
                if keyword not in organized_data["vessel_keywords"]:
                    organized_data["vessel_keywords"][keyword] = {"mentions": []}
                target = organized_data["vessel_keywords"][keyword]["mentions"]
            
            # Add mention
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
    
    return organized_data

# Save as JSON
def save_organized_results(organized_data, output_file):
    with open(output_file, 'w') as f:
        json.dump(organized_data, f, indent=2)

def main() -> None:
    """Main execution function."""
    logger.info("Loading CSV files...")
    try:
        plants_df = pd.read_csv('./data_in/Plants.csv')
        ships_df = pd.read_csv('./data_in/Ships.csv')
    except Exception as e:
        logger.error("Failed to load CSV files: %s", e, exc_info=True)
        return

    plant_search_terms = clean_search_terms(plants_df, 'Company name')
    ship_search_terms = clean_search_terms(ships_df, 'Vessel Name')
    ship_owner_terms = clean_search_terms(ships_df, 'Owner Name')  # Owner search added

    logger.info("Plant terms: %d, Vessel terms: %d, Owner terms: %d",
                len(plant_search_terms), len(ship_search_terms), len(ship_owner_terms))

    logger.info("Starting Reddit searches...")
    plant_results = search_subreddits(plant_search_terms[-4:])
    ship_results = search_subreddits(ship_search_terms[-4:])
    owner_results = search_subreddits(ship_owner_terms[-4:])

    logger.info("Processing search results...")
    combined_results = plant_results + ship_results + owner_results
    if combined_results:
        results_df = pd.DataFrame(combined_results).drop_duplicates(subset=['id', 'search_term'])
    else:
        logger.warning("No results found for any search terms")
        results_df = pd.DataFrame(columns=[
            'subreddit', 'id', 'title', 'author', 'created_utc',
            'permalink', 'snippet', 'search_term', 'matched_keywords',
            'score', 'num_comments'
        ])

    os.makedirs('data_out', exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_csv = f'data_out/search_results_{timestamp}.csv'
    summary_json = f'data_out/search_summary_{timestamp}.json'

    results_df.to_csv(results_csv, index=False)
    logger.info(f"Saved search results to {results_csv}")

    summary = generate_summary(results_df)
    with open(summary_json, 'w') as f:
        json.dump(summary, f, indent=2)
    logger.info(f"Saved summary to {summary_json}")

    organized_results = organize_search_results(results_csv)
    save_organized_results(organized_results, f'data_out/organized_results_{timestamp}.json')

    logger.info("Script completed successfully")

if __name__ == '__main__':
    main()
