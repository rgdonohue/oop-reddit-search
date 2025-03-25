# Reddit Search & Analysis for Global Fishmeal/Fish Oil Plants and Associated Fishing Vessels

## Overview

This project aims to discover and analyze mentions of fishmeal/fish oil processing plants and their associated fishing vessels on Reddit. The goal is to gather relevant discussions, news, and information about these entities to understand their presence and impact in online discussions.

## Project Status

Currently in Phase 1: Data Collection
- Developing and testing automated Reddit search scripts
- Implementing robust error handling and rate limiting
- Building efficient data processing pipelines
- Note: This phase may take several hours to complete due to Reddit's API rate limits

## Features

- Automated Reddit API search across multiple relevant subreddits
- Intelligent search term processing and deduplication
- Adaptive rate limiting to respect Reddit's API constraints
- Comprehensive error handling and logging
- Results caching to prevent duplicate searches
- Multiple output formats (JSON and CSV)

## Setup

1. Clone the repository
2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `.env` file with your Reddit API credentials:
   ```
   REDDIT_CLIENT_ID=your_client_id
   REDDIT_CLIENT_SECRET=your_client_secret
   REDDIT_USER_AGENT=script:fishing_vessel_search:v1.0
   ```

## Input Data

The script expects two CSV files in the `backend/data_in/` directory:
- `Plants.csv`: Contains fishmeal/fish oil processing plant information
- `Ships.csv`: Contains associated fishing vessel information

## Usage

Run the search script:
```bash
python backend/search-08.py
```

The script will:
1. Load and process input data
2. Search Reddit for mentions of plants and vessels
3. Save results to `backend/data_out/` directory
4. Generate both JSON and CSV output files

## Output

Results are saved in two formats:
- `results.json`: Complete search results with metadata
- `results.csv`: Tabular format for easy analysis

Each result includes:
- Title
- URL
- Relevant snippet
- Search term matched
- Subreddit
- Creation date
- Upvotes
- Number of comments
- Relevance score

## Development Notes

- The script implements adaptive rate limiting to handle Reddit's API constraints
- Results are cached to prevent duplicate searches
- Progress is logged to both console and log files
- The search can be interrupted safely (Ctrl+C) - partial results are saved

## Future Phases

1. Phase 1: Data Collection (Current)
   - Automated Reddit search implementation
   - Basic data processing and storage

2. Phase 2: Analysis
   - Sentiment analysis of mentions
   - Trend analysis over time
   - Geographic distribution of mentions

3. Phase 3: Visualization
   - Interactive dashboards
   - Geographic mapping
   - Temporal analysis visualizations

## Contributing

Feel free to submit issues and enhancement requests!

## License

[MIT License](LICENSE)
