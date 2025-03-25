import pandas as pd
from datetime import datetime
import logging
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
from dotenv import load_dotenv
import time
from collections import defaultdict
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Add these constants near the top of the file
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = 'script:fishing_vessel_search:v1.0 (by /u/your_actual_username)'
REDDIT_AUTH_URL = 'https://www.reddit.com/api/v1/access_token'
REDDIT_SEARCH_URL = 'https://oauth.reddit.com/r/{}/search'
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
GOOGLE_CX = os.getenv('GOOGLE_SEARCH_CX')

# Load and clean the CSV files
logger.info("Loading and cleaning CSV files...")
plants_df = pd.read_csv('./data_in/Plants.csv')
ships_df = pd.read_csv('./data_in/Ships.csv')

# Update the relevance keywords to be more specific
RELEVANCE_KEYWORDS = [
    'fishing vessel', 'fishing boat', 'trawler', 'commercial fishing',
    'fishmeal', 'fish meal', 'fish oil', 'processing plant', 'seafood processing',
    'aquaculture', 'marine harvest', 'catch', 'landing', 'industrial fishing'
]

# Add a function to improve term cleaning
def clean_search_terms(df, name_column):
    # Expand common words to remove
    common_words = [
        'the', 'ltd', 'inc', 'corporation', 'company', 'sa', 'sac', 'and',
        'of', 'co', 'group', 'international', 'trading', 'enterprises',
        'industries', 'limited', 'corp', 'services'
    ]
    
    # Add specific characters to remove
    special_chars = r'[,\.&\(\)\[\]{}\-_/\\]'
    
    terms = df[name_column].dropna().unique().tolist()
    cleaned_terms = []
    
    for term in terms:
        if isinstance(term, str):
            # Convert to lowercase and remove special characters
            term = re.sub(special_chars, ' ', term.lower())
            
            # Remove common words
            words = term.split()
            words = [w for w in words if w not in common_words]
            term = ' '.join(words).strip()
            
            # Only keep terms that are meaningful length and not just numbers
            if len(term) > 3 and not term.isdigit() and not all(c.isdigit() or c.isspace() for c in term):
                cleaned_terms.append(term)
    
    return list(set(cleaned_terms))  # Remove duplicates

# Get all search terms instead of just 2
plant_search_terms = clean_search_terms(plants_df, 'Company name')
ship_search_terms = clean_search_terms(ships_df, 'Vessel Name')
ship_owner_terms = clean_search_terms(ships_df, 'Owner Name')  # Added owner search as suggested

logger.info(f"Number of plant terms to search: {len(plant_search_terms)}")
logger.info(f"Number of vessel terms to search: {len(ship_search_terms)}")
logger.info(f"Number of owner terms to search: {len(ship_owner_terms)}")

# Subreddit recommendations for search results
subreddit_recommendations = [
    'r/Fishing',
    'r/CommercialFishing',
    'r/OceanFishing', 
    'r/Seafood',
    'r/MarineBiology',
    'r/MarineConservation',
    'r/Maritime',
    'r/EnvironmentalScience',
    'r/WorldNews',
    'r/News',
    'r/Aquaculture',
    'r/Sustainability',
    'r/Oceans',
    'r/FishingIndustry',
    'r/AquaticAgriculture',
    'r/FoodProduction',
    'r/SupplyChain',
    'r/Industry',
    'r/Business',
    'r/GlobalTrade'
]

# Add this function to handle authentication
def get_reddit_token():
    auth = requests.auth.HTTPBasicAuth(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET)
    data = {
        'grant_type': 'client_credentials',
    }
    headers = {
        'User-Agent': REDDIT_USER_AGENT
    }
    
    try:
        response = requests.post(
            REDDIT_AUTH_URL,
            auth=auth,
            data=data,
            headers=headers
        )
        response.raise_for_status()
        token = response.json().get('access_token')
        if not token:
            logger.error("No token in response: " + str(response.json()))
            return None
        logger.info("Successfully obtained Reddit token")
        return token
    except Exception as e:
        logger.error(f"Failed to get Reddit token: {str(e)}")
        return None

# Add this function to create a session with retry logic
def create_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

# Function to search for terms in subreddits
def search_subreddits(terms):
    search_results = []
    seen_posts = set()
    term_counts = defaultdict(int)
    term_subreddit_counts = defaultdict(lambda: defaultdict(int))
    
    # Create session and get token
    session = create_session()
    token = get_reddit_token()
    if not token:
        logger.error("Failed to get Reddit authentication token")
        return []

    headers = {
        'User-Agent': REDDIT_USER_AGENT,
        'Authorization': f'Bearer {token}'
    }

    for term in terms:
        logger.info(f"Searching for term: {term}")
        search_start = datetime.now()
        
        for subreddit in subreddit_recommendations:
            subreddit_name = subreddit.replace('r/', '')
            logger.info(f"Searching in subreddit: {subreddit_name}")
            
            try:
                url = REDDIT_SEARCH_URL.format(subreddit_name)
                params = {
                    'q': term,
                    'limit': 100,
                    'sort': 'new',
                    'restrict_sr': 'true'
                }
                
                response = session.get(url, params=params, headers=headers)
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds")
                    time.sleep(retry_after)
                    response = session.get(url, params=params, headers=headers)
                
                # Log full response for debugging
                if response.status_code != 200:
                    logger.error(f"HTTP {response.status_code} error for {subreddit_name}")
                    logger.error(f"Response headers: {dict(response.headers)}")
                    logger.error(f"Response body: {response.text}")
                    continue
                
                data = response.json()
                
                if 'data' not in data or 'children' not in data.get('data', {}):
                    logger.error(f"Unexpected response structure for {subreddit_name}: {data}")
                    continue
                
                posts = data['data']['children']
                
                # Improved post processing with better relevance checking
                for post in posts:
                    post_data = post['data']
                    post_id = post_data['id']
                    
                    if post_id in seen_posts:
                        continue
                    
                    content = f"{post_data.get('title', '')} {post_data.get('selftext', '')}"
                    
                    # More strict relevance checking
                    is_relevant = False
                    matched_keywords = []
                    for keyword in RELEVANCE_KEYWORDS:
                        if keyword in content.lower():
                            is_relevant = True
                            matched_keywords.append(keyword)
                    
                    if is_relevant:
                        seen_posts.add(post_id)
                        
                        # Find snippet with more context around the search term
                        term_index = content.lower().find(term.lower())
                        if term_index != -1:
                            start = max(0, term_index - 150)
                            end = min(len(content), term_index + len(term) + 150)
                            snippet = content[start:end]
                            
                            term_counts[term] += 1
                            term_subreddit_counts[term][subreddit_name] += 1
                            
                            result = {
                                'subreddit': subreddit_name,
                                'id': post_id,
                                'title': post_data['title'],
                                'author': post_data['author'],
                                'created_utc': datetime.fromtimestamp(post_data['created_utc']).strftime('%Y-%m-%d %H:%M:%S'),
                                'permalink': f"https://reddit.com{post_data['permalink']}",
                                'snippet': snippet,
                                'search_term': term,
                                'matched_keywords': ', '.join(matched_keywords),
                                'score': post_data.get('score', 0),
                                'num_comments': post_data.get('num_comments', 0)
                            }
                            search_results.append(result)
            
            except Exception as e:
                logger.error(f"Error in search for {subreddit}: {str(e)}")
                logger.exception("Full traceback:")
            
            time.sleep(2)
                
        search_end = datetime.now()
        search_duration = search_end - search_start
        logger.info(f"Completed search for term '{term}' in {search_duration}")
        
    return search_results

# Search for all terms
logger.info("Starting searches...")
plant_results = search_subreddits(plant_search_terms[-2:])
ship_results = search_subreddits(ship_search_terms[-2:])
owner_results = search_subreddits(ship_owner_terms[-2:])

# Combine and save all results
logger.info("Processing results...")
if not (plant_results or ship_results or owner_results):
    logger.warning("No results found for any search terms")
    all_results = pd.DataFrame(columns=[
        'subreddit', 'id', 'title', 'author', 'created_utc', 
        'permalink', 'snippet', 'search_term', 'matched_keywords',
        'score', 'num_comments'
    ])
else:
    all_results = pd.DataFrame(plant_results + ship_results + owner_results)

# Remove duplicates based on post ID and search term
if not all_results.empty:
    all_results = all_results.drop_duplicates(subset=['id', 'search_term'])

# Save results with type indicators
logger.info("Saving results to CSV...")
all_results.to_csv('data_out/search_results.csv', index=False)

# Update the summary statistics
def generate_summary(results_df):
    if results_df.empty:
        return {
            'total_results': 0,
            'unique_posts': 0,
            'results_by_subreddit': {},
            'results_by_keyword': {},
            'top_terms': {},
            'date_range': {
                'earliest': None,
                'latest': None
            },
            'engagement_stats': {
                'avg_score': 0,
                'avg_comments': 0,
                'max_score': 0,
                'max_comments': 0
            }
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

# Generate summary statistics
summary = generate_summary(all_results)

# Save summary to JSON
with open('data_out/search_summary.json', 'w') as f:
    json.dump(summary, f, indent=2)

logger.info("Script completed successfully")
