**Reddit Search Fishmeal/Fish Oil Plants and Their Associated Fishing Vessels**

---

## **Overview**

You have a list of fishmeal and fish oil processing plants from around the world, along with the fishing vessels that supply them. The goal is to discover any mentions or discussions on Reddit related to these plants or vessels. This project consists of two major steps:

1. **Manual “Proof of Concept” Search**  
2. **Programmatic Reddit API Search**

The outcome should help determine whether there is substantial Reddit content around these entities (facilities and vessels) and to collect any relevant mentions and context.

---

## **Objectives**

1. **Identify Mentions on Reddit**

   * Manually (as a proof of concept) locate at least a few references to fishmeal/fish oil plants or their supplying vessels to confirm whether relevant discussions exist on Reddit.  
2. **Scrape/Extract Data via Reddit API**

   * Use Reddit’s API to systematically search for posts/comments mentioning any item from your provided list.  
3. **Collate and Organize Results**

   * Gather relevant metadata (e.g., subreddit name, post title, author, date/time, link to the post/comment, snippet of the mention).

---

## **Scope & Requirements**

[For Reddit Search](https://docs.google.com/spreadsheets/d/1_nMC0J9VwTYgKlMIEMu88SQTtp8D4tehObcsqPHdwZU/edit?usp=sharing)

### *Manual Search (Proof of Concept)*

1. **Subreddit Recommendations**  
   * r/Fishing  
   * r/CommercialFishing  
   * r/OceanFishing  
   * r/Seafood  
   * r/MarineBiology  
   * r/MarineConservation  
   * r/Maritime  
   * r/EnvironmentalScience  
   * r/WorldNews  
   * r/News  
   * r/Local subreddits  
   * Any other sub-reddits you can think of

2. **Manual Search Method**

   * For vessels, I recommend trying to search for the owner name first. Keep in mind there are a lot of duplicate owner names.   
   * Use Reddit’s native search bar within the site (or Google’s site-specific search site:reddit.com) to query:  
     * Each **fishmeal/fish oil plant name**  
     * Each **fishing vessel name**  
   * Record any mentions found:  
     * **Subreddit**  
     * **Post Title**  
     * **Post Author**  
     * **Date**  
     * **URL**  
     * **Relevant Excerpt** or screenshot  
3. **Deliverable for Proof of Concept**  
   A short write-up (or a small CSV/Excel sheet) summarizing the results of this initial, **manual** search.  
   * At least a few mentions (if any exist) to confirm the feasibility of deeper research.

### **Programmatic Search** 

* Two options  
  *  **Pushshift API** (unofficial but widely used for historical data) or the **Reddit API** (for more recent data) or a combination of both for broader coverage.

**Data to Capture**

* **Subreddit** name  
  * **Post/Comment ID**  
  * **Title** (for posts) or **Context** (for comments)  
  * **Author**  
  * **Date/Time** of submission/comment  
  * **Permalink** (URL)  
  * **Snippet** containing the mention of the plant/vessel (to quickly identify relevance)

**Deduplication & Filtering**

* If the same mention appears multiple times or is cross-posted, only keep one record in the final output or clearly mark duplicates.  
  * Filter out non-relevant results if the search term is used in an unrelated context (e.g., if the name happens to be a common term).

---

**Deliverables**

1. **Automated Script or Tool**  
   * Runs through all \~1,200 plants & \~ 1,200 vessels.  
     1. Might have more luck running through the vessel owner names?   
     2. You will need to do some cleaning to make the list better for search  
   * Searches for each through a series of subreddits.  
   * Parses the JSON response and updates the master spreadsheet with the relevant information  
2. **Documentation**  
   * Includes all relevant code files  
   * **Excludes all API keys and passwords** so the code can be shared publicly.

