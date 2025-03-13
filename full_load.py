import os
import sys
import json
import time
import random
import requests
import logging
import re
import asyncio
import aiohttp
from datetime import datetime

# Constants
BASE_URL = "https://it.pracuj.pl/praca"
RESULTS_PER_PAGE = 50  # Most job sites show 20 results per page
DATA_DIR = "job_data"
PROGRESS_FILE = "scraping_progress.json"
LOG_FILE = "scraper_log.txt"

# Environment variable flags (0=disabled, 1=enabled)
SAVE_RAW_PAGES = os.environ.get('SAVE_RAW_PAGES', '0') == '1'
SAVE_RAW_JOBS = os.environ.get('SAVE_RAW_JOBS', '0') == '1'

# Browser-like headers to avoid being detected as a bot
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    
}

# Create necessary directories
os.makedirs(DATA_DIR, exist_ok=True)

def log_message(message):
    """Log a message with timestamp to the log file"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_entry)
    
    print(message)

def log_timing(operation, start_time, end_time=None):
    """Log the time taken for an operation"""
    if end_time is None:
        end_time = time.time()
    
    duration = end_time - start_time
    
    # Format duration based on length
    if duration < 1:
        formatted_duration = f"{duration * 1000:.2f} ms"
    elif duration < 60:
        formatted_duration = f"{duration:.2f} seconds"
    elif duration < 3600:
        formatted_duration = f"{duration / 60:.2f} minutes"
    else:
        formatted_duration = f"{duration / 3600:.2f} hours"
    
    log_message(f"TIMING: {operation} took {formatted_duration}")
    return duration

def save_progress(current_page, total_pages, jobs_collected):
    """Save current progress to resume later if needed"""
    try:
        progress = {
            "current_page": current_page,
            "total_pages": total_pages,
            "jobs_collected": jobs_collected,
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, indent=2)
        
        return True
    except Exception as e:
        log_message(f"Error saving progress: {e}")
        return False

def load_progress():
    """Load previous progress if it exists"""
    try:
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                progress = json.load(f)
            
            current_page = progress.get("current_page", 1)
            total_pages = progress.get("total_pages", None)
            jobs_collected = progress.get("jobs_collected", 0)
            
            log_message(f"Resuming from page {current_page}, already collected {jobs_collected} jobs")
            return current_page, total_pages, jobs_collected
        else:
            log_message("No progress file found, starting from scratch")
            return None, None, 0
    except Exception as e:
        log_message(f"Error loading progress: {e}")
        return None, None, 0

def extract_job_data(html):
    """Extract the job data from the HTML"""
    # Look for the JSON data in the __NEXT_DATA__ script tag
    pattern = r'<script\s+id="__NEXT_DATA__"\s+type="application/json">(.*?)</script>'
    match = re.search(pattern, html)
    
    if not match:
        log_message("Could not find __NEXT_DATA__ script tag in the HTML")
        return None
    
    try:
        data = json.loads(match.group(1))
        return data
    except json.JSONDecodeError as e:
        log_message(f"Error parsing JSON: {e}")
        return None

def get_total_pages(data):
    """Extract the total number of pages from the JSON data"""
    try:
        # Try to navigate to pagination info based on known structure
        try:
            pagination = data['props']['pageProps']['dehydratedState']['queries'][0]['state']['data']['pagination']
            total_results = pagination['totalResults']
            log_message(f"Found pagination data: total results = {total_results}")
            # Calculate total pages
            total_pages = (total_results + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
            return total_pages, total_results
        except (KeyError, TypeError) as e:
            log_message(f"Error with primary pagination path: {e}")
        
        # Try alternative paths for pagination data
        try:
            # Alternative path 1
            offer_list = data['props']['pageProps']['initialState']['offers']['offer_list']
            if 'pagination' in offer_list:
                total_results = offer_list['pagination']['total_results']
                log_message(f"Found pagination in alternative path 1: {total_results}")
                total_pages = (total_results + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
                return total_pages, total_results
        except (KeyError, TypeError) as e:
            log_message(f"Error with alternative path 1: {e}")
        
        try:
            # Alternative path 2 - try to count offers directly
            offers = data['props']['pageProps']['dehydratedState']['queries'][0]['state']['data']['groupedOffers']
            if offers:
                offer_count = len(offers)
                log_message(f"No pagination found, but found {offer_count} offers on the first page")
                # Since we know there are ~900+ pages, use a more accurate estimate
                return 900, offer_count * 900
        except (KeyError, TypeError) as e:
            log_message(f"Error counting offers: {e}")
        
        # If all attempts fail, dump a portion of the structure for debugging
        log_message("Dumping data structure keys for debugging:")
        if 'props' in data:
            log_message(f"props keys: {list(data['props'].keys())}")
            if 'pageProps' in data['props']:
                log_message(f"pageProps keys: {list(data['props']['pageProps'].keys())}")
        
        # Fallback to a higher default since we know there are ~900+ pages
        log_message("Using default estimate of 900 pages")
        return 900, 18000  # Assuming ~20 jobs per page
        
    except Exception as e:
        log_message(f"Unexpected error in get_total_pages: {e}")
        # Fallback to a reasonable default if all else fails
        return 900, 18000  # Assuming ~20 jobs per page

def extract_job_listings(data):
    """Extract basic info for all job listings from the search results data"""
    try:
        # Navigate to job offers
        grouped_offers = data['props']['pageProps']['dehydratedState']['queries'][0]['state']['data']['groupedOffers']
        
        job_listings = []
        for offer in grouped_offers:
            try:
                # Extract basic info
                job_id = offer['offers'][0]['partitionId']
                job_title = offer.get('jobTitle', 'No Title')
                company_name = offer.get('companyName', 'Unknown Company')
                workplace = offer['offers'][0].get('displayWorkplace', 'No Location')
                is_one_click = offer.get('isOneClickApply', False)
                
                # Extract technologies if available in search results
                technologies = offer.get('technologies', [])
                
                # Extract URL for fetching detailed info later
                url = offer['offers'][0].get('offerAbsoluteUri', '')
                
                # Get additional details available in search results
                position_levels = offer.get('positionLevels', [])
                contract_types = offer.get('typesOfContract', [])
                work_schedules = offer.get('workSchedules', [])
                work_modes = offer.get('workModes', [])
                salary = offer.get('salaryDisplayText', '')
                job_description = offer.get('jobDescription', '')
                
                # Create job listing object with basic info
                job_listing = {
                    "id": job_id,
                    "title": job_title,
                    "company": company_name,
                    "location": workplace,
                    "technologies": technologies,
                    "is_one_click_apply": is_one_click,
                    "position_level": position_levels,
                    "contract_types": contract_types,
                    "work_schedules": work_schedules,
                    "work_modes": work_modes,
                    "salary": salary,
                    "description": job_description,
                    "url": url,
                    "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    # Initialize detailed info fields as empty
                    "requirements": [],
                    "responsibilities": [],
                    "offered": [],
                    "benefits": [],
                    "work_organization": {}
                }
                
                job_listings.append(job_listing)
                
            except Exception as e:
                log_message(f"Error processing job offer: {e}")
                continue
        
        return job_listings
    
    except KeyError as e:
        log_message(f"Error navigating JSON structure: {e}")
        return []

def extract_job_details(html, job_listing):
    """Extract detailed information from a specific job listing page"""
    try:
        # Find the JSON data in the script tag
        match = re.search(r'<script id="__NEXT_DATA__" type="application\/json">(.*?)<\/script>', html, re.DOTALL)
        if not match:
            log_message(f"Could not find job data in HTML for job {job_listing['id']}")
            return job_listing
        
        # Parse the JSON data
        job_data = json.loads(match.group(1))
        
        # Check if we have the expected structure
        if 'props' not in job_data or 'pageProps' not in job_data['props']:
            log_message(f"Unexpected JSON structure for job {job_listing['id']}")
            return job_listing
        
        # Try to extract the job offer info
        if 'offerId' in job_data['props']['pageProps']:
            # Get all queries from dehydratedState
            if 'dehydratedState' in job_data['props']['pageProps'] and 'queries' in job_data['props']['pageProps']['dehydratedState']:
                queries = job_data['props']['pageProps']['dehydratedState']['queries']
                
                # Look for the query containing job offer data
                job_offer_data = None
                for query in queries:
                    if 'queryKey' in query and query['queryKey'][0] == 'jobOffer':
                        if 'state' in query and 'data' in query['state']:
                            job_offer_data = query['state']['data']
                            break
                
                if job_offer_data:
                    # Extract sections data if available
                    if 'sections' in job_offer_data:
                        sections = job_offer_data['sections']
                        
                        # Process each section type
                        for section in sections:
                            section_type = section.get('sectionType', '')
                            
                            # Extract technologies
                            if section_type == 'technologies' and 'subSections' in section:
                                for subsection in section['subSections']:
                                    if subsection.get('sectionType') == 'technologies-expected' and 'model' in subsection:
                                        tech_items = []
                                        
                                        # Extract from customItems
                                        if 'customItems' in subsection['model']:
                                            for item in subsection['model']['customItems']:
                                                if 'name' in item and item['name'] not in tech_items:
                                                    tech_items.append(item['name'])
                                        
                                        # Extract from items
                                        if 'items' in subsection['model']:
                                            for item in subsection['model']['items']:
                                                if 'name' in item and item['name'] not in tech_items:
                                                    tech_items.append(item['name'])
                                        
                                        # Add to existing technologies
                                        for tech in tech_items:
                                            if tech not in job_listing['technologies']:
                                                job_listing['technologies'].append(tech)
                            
                            # Extract requirements
                            elif section_type == 'requirements' and 'subSections' in section:
                                for subsection in section['subSections']:
                                    if 'model' in subsection and 'bullets' in subsection['model']:
                                        job_listing['requirements'].extend(subsection['model']['bullets'])
                            
                            # Extract responsibilities
                            elif section_type == 'responsibilities' and 'model' in section and 'bullets' in section['model']:
                                job_listing['responsibilities'] = section['model']['bullets']
                            
                            # Extract what company offers
                            elif section_type == 'offered' and 'model' in section and 'bullets' in section['model']:
                                job_listing['offered'] = section['model']['bullets']
                            
                            # Extract benefits
                            elif section_type == 'benefits' and 'model' in section:
                                benefits = []
                                
                                # Extract from customItems
                                if 'customItems' in section['model']:
                                    for item in section['model']['customItems']:
                                        if 'name' in item:
                                            benefits.append(item['name'])
                                
                                # Extract from items
                                if 'items' in section['model']:
                                    for item in section['model']['items']:
                                        if 'name' in item:
                                            benefits.append(item['name'])
                                
                                job_listing['benefits'] = benefits
                            
                            # Extract work organization
                            elif section_type == 'work-organization' and 'subSections' in section:
                                work_organization = {}
                                
                                for subsection in section['subSections']:
                                    subsection_type = subsection.get('sectionType', '')
                                    
                                    if subsection_type == 'work-organization-team-size' and 'model' in subsection and 'paragraphs' in subsection['model']:
                                        work_organization['team_size'] = subsection['model']['paragraphs'][0] if subsection['model']['paragraphs'] else ''
                                    
                                    elif subsection_type == 'work-organization-work-style' and 'model' in subsection and 'items' in subsection['model']:
                                        work_styles = []
                                        for item in subsection['model']['items']:
                                            if 'name' in item:
                                                work_styles.append(item['name'])
                                        work_organization['work_style'] = work_styles
                                    
                                    elif subsection_type == 'work-organization-team-members' and 'model' in subsection and 'items' in subsection['model']:
                                        team_members = []
                                        for item in subsection['model']['items']:
                                            if 'name' in item:
                                                team_members.append(item['name'])
                                        work_organization['team_members'] = team_members
                                
                                job_listing['work_organization'] = work_organization
                    
                    # Try to extract from textSections if available
                    if 'textSections' in job_offer_data:
                        for section in job_offer_data['textSections']:
                            section_type = section.get('sectionType', '')
                            
                            # Backup for technologies
                            if section_type == 'technologies-expected' and 'textElements' in section and not job_listing['technologies']:
                                job_listing['technologies'] = section['textElements']
                            
                            # Backup for requirements
                            elif section_type == 'requirements-expected' and 'textElements' in section and not job_listing['requirements']:
                                job_listing['requirements'] = section['textElements']
                            
                            # Backup for responsibilities
                            elif section_type == 'responsibilities' and 'textElements' in section and not job_listing['responsibilities']:
                                job_listing['responsibilities'] = section['textElements']
                            
                            # Backup for offered
                            elif section_type == 'offered' and 'textElements' in section and not job_listing['offered']:
                                job_listing['offered'] = section['textElements']
                            
                            # Backup for benefits
                            elif section_type == 'benefits' and 'textElements' in section and not job_listing['benefits']:
                                job_listing['benefits'] = section['textElements']
        
        return job_listing
    
    except Exception as e:
        log_message(f"Error extracting detailed job info: {e}")
        return job_listing

async def fetch_job_details_async(job_listings, max_concurrent=12):
    """Fetch detailed information for each job listing asynchronously"""
    if not job_listings:
        log_message("No job listings provided for detail fetching")
        return []
    
    log_message(f"Fetching details for {len(job_listings)} jobs asynchronously (max_concurrent={max_concurrent})")
    enhanced_listings = []
    
    # Create a semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(max_concurrent)
    
    # Create a shared session timeout configuration
    timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_connect=10, sock_read=15)
    
    # Create headers with a random user agent
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
    ]
    
    async def fetch_one_job(job, job_index):
        """Fetch details for a single job"""
        async with semaphore:
            try:
                # Add dynamic delay based on job index to avoid too many requests
                # More aggressive rate limiting: base delay of 4-8 seconds
                delay = 4.0 + random.uniform(1.0, 4.0) + (0.2 * (job_index // 3))
                log_message(f"Delaying request for job {job['id']} by {delay:.2f} seconds")
                await asyncio.sleep(delay)
                
                # Use a new session for each request but with shared cookies
                # Create a copy of headers with a random user agent
                headers = dict(HEADERS)
                headers["User-Agent"] = random.choice(user_agents)
                
                async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                    # Add a random "Referer" header to simulate coming from various pages
                    referers = [
                        "https://www.pracuj.pl/praca",
                        f"https://www.pracuj.pl/praca?pn={random.randint(1, 10)}",
                        "https://www.pracuj.pl/praca/it",
                        "https://www.google.com/search?q=pracuj+pl+jobs"
                    ]
                    headers["Referer"] = random.choice(referers)
                    
                    async with session.get(job['url']) as response:
                        if response.status != 200:
                            log_message(f"Failed to fetch details for job {job['id']}: HTTP {response.status}")
                            if response.status == 429:  # Too Many Requests
                                log_message("Received 429 Too Many Requests - adding extra delay")
                                await asyncio.sleep(90 + random.uniform(0, 30))  # Add a 1.5-2 minute delay
                            return job
                        
                        html = await response.text()
                        
                        # Save raw job HTML for debugging only if enabled
                        if SAVE_RAW_JOBS:
                            job_dir = os.path.join(DATA_DIR, "raw_jobs")
                            os.makedirs(job_dir, exist_ok=True)
                            with open(os.path.join(job_dir, f"job_{job['id']}.html"), 'w', encoding='utf-8') as f:
                                f.write(html)
                        
                        # Extract job details from HTML
                        extract_job_details(html, job)
                        return job
            
            except Exception as e:
                log_message(f"Error fetching details for job {job['id']}: {e}")
                return job
    
    # Create tasks for all jobs
    tasks = [fetch_one_job(job, i) for i, job in enumerate(job_listings)]
    
    # Wait for all tasks to complete
    results = await asyncio.gather(*tasks)
    
    # Filter out None results
    enhanced_listings = [job for job in results if job]
    
    log_message(f"Successfully fetched details for {len(enhanced_listings)} jobs")
    return enhanced_listings

def fetch_job_details(job_listings, max_concurrent=12):
    """Synchronous wrapper for async fetch_job_details_async function"""
    try:
        return asyncio.run(fetch_job_details_async(job_listings, max_concurrent))
    except Exception as e:
        log_message(f"Error in fetch_job_details: {e}")
        # Fallback to synchronous fetching if async fails
        return fetch_job_details_sync(job_listings)

def fetch_job_details_sync(job_listings):
    """Fetch detailed information for each job listing synchronously (fallback)"""
    if not job_listings:
        log_message("No job listings provided for detail fetching")
        return []
    
    log_message(f"Fetching details for {len(job_listings)} jobs synchronously")
    enhanced_listings = []
    
    # Create a session for all requests to maintain cookies
    session = get_session()
    
    for job in job_listings:
        try:
            log_message(f"Fetching details for job {job['id']}: {job['title']}")
            
            # Add delay with some randomness to mimic human behavior
            delay = random.uniform(3, 7)
            log_message(f"Waiting {delay:.2f} seconds before requesting job details")
            time.sleep(delay)
            
            # Use session instead of direct requests
            response = session.get(job['url'])
            
            if response.status_code != 200:
                log_message(f"Failed to fetch details for job {job['id']}: HTTP {response.status_code}")
                if response.status_code == 429:  # Too Many Requests
                    log_message("Received 429 Too Many Requests - adding extra delay")
                    time.sleep(60 + random.uniform(0, 30))  # Add a 1-1.5 minute delay
                enhanced_listings.append(job)
                continue
            
            html = response.text
            
            # Save raw job HTML for debugging only if enabled
            if SAVE_RAW_JOBS:
                job_dir = os.path.join(DATA_DIR, "raw_jobs")
                os.makedirs(job_dir, exist_ok=True)
                with open(os.path.join(job_dir, f"job_{job['id']}.html"), 'w', encoding='utf-8') as f:
                    f.write(html)
            
            # Extract job details from HTML
            extract_job_details(html, job)
            enhanced_listings.append(job)
            
        except Exception as e:
            log_message(f"Error fetching details for job {job['id']}: {e}")
            enhanced_listings.append(job)
    
    log_message(f"Successfully fetched details for {len(enhanced_listings)} jobs")
    return enhanced_listings

def get_session():
    """Create and return a requests Session with persistent cookies and randomized User-Agent"""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
    ]
    
    session = requests.Session()
    
    # Copy all headers to the session
    for key, value in HEADERS.items():
        session.headers[key] = value
    
    # Set a random User-Agent from our list to add variety
    session.headers["User-Agent"] = random.choice(user_agents)
    
    # Enable cookie persistence
    session.cookies.clear_session_cookies()
    
    log_message(f"Created new session with User-Agent: {session.headers['User-Agent']}")
    return session

def scrape_page(page_number):
    """Scrape a specific page of job listings"""
    url = f"{BASE_URL}?pn={page_number}"
    
    page_start_time = time.time()
    try:
        # Make the request
        log_message(f"Fetching page {page_number} from {url}")
        request_start = time.time()
        response = requests.get(url, headers=HEADERS)
        request_end = time.time()
        log_timing(f"HTTP request for page {page_number}", request_start, request_end)
        
        # Check if the request was successful
        if response.status_code != 200:
            log_message(f"Failed to fetch page {page_number}: HTTP {response.status_code}")
            return []
        
        # Save the raw HTML for debugging only if enabled
        if SAVE_RAW_PAGES:
            raw_dir = os.path.join(DATA_DIR, "raw_pages")
            os.makedirs(raw_dir, exist_ok=True)
            with open(os.path.join(raw_dir, f"page_{page_number}.html"), 'w', encoding='utf-8') as f:
                f.write(response.text)
        
        # Extract job data
        extraction_start = time.time()
        job_data = extract_job_data(response.text)
        if not job_data:
            log_message(f"No job data found on page {page_number}")
            return []
        
        # Extract job listings
        job_listings = extract_job_listings(job_data)
        extraction_end = time.time()
        log_timing(f"Data extraction for page {page_number}", extraction_start, extraction_end)
        
        if not job_listings:
            log_message(f"No job listings found on page {page_number}")
            return []
        
        # Fetch detailed information for each job listing
        log_message(f"Fetching detailed information for {len(job_listings)} jobs on page {page_number}")
        detail_start = time.time()
        enhanced_listings = fetch_job_details(job_listings)
        detail_end = time.time()
        log_timing(f"Fetching details for {len(job_listings)} jobs on page {page_number}", detail_start, detail_end)
        
        page_end_time = time.time()
        log_timing(f"Complete processing of page {page_number}", page_start_time, page_end_time)
        log_message(f"Successfully scraped {len(enhanced_listings)} jobs from page {page_number}")
        return enhanced_listings
    
    except Exception as e:
        page_end_time = time.time()
        log_timing(f"Failed processing of page {page_number}", page_start_time, page_end_time)
        log_message(f"Error scraping page {page_number}: {e}")
        return []

def add_rate_limiting_delay(current_page):
    """Add a delay between requests with exponential backoff for higher page numbers"""
    # Base delay between 3-5 seconds
    base_delay = random.uniform(3, 5)
    
    # Add exponential backoff for higher page numbers
    # Every 10 pages, increase the delay by 20%
    backoff_factor = 1.0 + (0.2 * (current_page // 10))
    
    # Cap the maximum delay at 15 seconds
    delay = min(base_delay * backoff_factor, 15)
    
    log_message(f"Rate limiting: Waiting {delay:.2f} seconds before next request")
    time.sleep(delay)

def save_batch(job_listings, batch_number):
    """Save a batch of job listings to a file"""
    if not job_listings:
        return
    
    os.makedirs(f"{DATA_DIR}/batches", exist_ok=True)
    filename = f"{DATA_DIR}/batches/jobs_batch_{batch_number}.json"
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(job_listings, f, indent=4, ensure_ascii=False)
    
    log_message(f"Saved {len(job_listings)} job listings to {filename}")

def consolidate_data():
    """Consolidate all batches into a single file"""
    all_jobs = []
    batch_dir = f"{DATA_DIR}/batches"
    
    if not os.path.exists(batch_dir):
        log_message("No batches directory found")
        return
    
    for filename in os.listdir(batch_dir):
        if filename.endswith(".json"):
            try:
                with open(os.path.join(batch_dir, filename), "r", encoding="utf-8") as f:
                    batch_jobs = json.load(f)
                    all_jobs.extend(batch_jobs)
            except Exception as e:
                log_message(f"Error reading batch file {filename}: {e}")
    
    # Deduplicate by job ID
    unique_jobs = {}
    for job in all_jobs:
        key = f"{job['title']}_{job['company']}_{job['location']}"
        unique_jobs[key] = job
    
    all_unique_jobs = list(unique_jobs.values())
    
    with open(f"{DATA_DIR}/all_jobs_consolidated.json", "w", encoding="utf-8") as f:
        json.dump(all_unique_jobs, f, indent=4, ensure_ascii=False)
    
    log_message(f"Consolidated {len(all_unique_jobs)} unique job listings into all_jobs_consolidated.json")
    return all_unique_jobs

def main():
    """Main function to run the scraper"""
    log_message("Starting job scraper...")
    
    # Try to load previous progress
    start_page, total_pages, jobs_collected = load_progress()
    
    if not start_page:
        # First run, get total pages
        log_message("First run, getting total number of pages...")
        
        try:
            session = get_session()
            response = session.get(BASE_URL)
            
            if response.status_code != 200:
                log_message(f"Failed to access {BASE_URL}: HTTP {response.status_code}")
                return
            
            # Save the initial page for debugging
            debug_dir = os.path.join(DATA_DIR, "debug")
            os.makedirs(debug_dir, exist_ok=True)
            with open(os.path.join(debug_dir, "initial_page.html"), 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            job_data = extract_job_data(response.text)
            if not job_data:
                log_message("Failed to extract initial job data. Saving raw HTML for debugging.")
                return
            
            # Save the extracted JSON data for debugging
            with open(os.path.join(debug_dir, "initial_data.json"), 'w', encoding='utf-8') as f:
                json.dump(job_data, f, indent=2)
            
            total_pages, total_jobs = get_total_pages(job_data)
            log_message(f"Detected {total_jobs} jobs across {total_pages} pages")
            
            start_page = 1
            jobs_collected = 0
        except Exception as e:
            log_message(f"Error during initialization: {e}")
            return
    
    # Set up batch processing
    BATCH_SIZE = 100
    current_batch = []
    batch_number = 1
    
    try:
        # Scrape each page
        for current_page in range(start_page, total_pages + 1):
            log_message(f"Processing page {current_page}/{total_pages}...")
            
            # Scrape the page
            job_listings = scrape_page(current_page)
            
            if job_listings:
                current_batch.extend(job_listings)
                jobs_collected += len(job_listings)
                log_message(f"Page {current_page}/{total_pages}: Found {len(job_listings)} jobs, total collected: {jobs_collected}")
                
                # Save batch if it's full
                if len(current_batch) >= BATCH_SIZE:
                    save_batch(current_batch, batch_number)
                    batch_number += 1
                    current_batch = []
            else:
                log_message(f"No job listings found on page {current_page}")
            
            # Save progress
            save_progress(current_page + 1, total_pages, jobs_collected)
            
            # Add rate limiting delay
            add_rate_limiting_delay(current_page)
    
    except KeyboardInterrupt:
        log_message("Scraping interrupted by user")
    except Exception as e:
        log_message(f"Error during scraping: {e}")
    finally:
        # Save any remaining jobs in the current batch
        if current_batch:
            save_batch(current_batch, batch_number)
        
        # Consolidate all batches into a single file
        consolidate_data()
        
        log_message(f"Scraping completed. Collected {jobs_collected} job listings")

if __name__ == "__main__":
    main()