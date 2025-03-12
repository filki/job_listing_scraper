import requests
import json
import re
import os
import time
from datetime import datetime
import random

# Constants
BASE_URL = "https://www.pracuj.pl/praca"
RESULTS_PER_PAGE = 50 # Default results per page on pracuj.pl
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
DATA_DIR = "job_data"
PROGRESS_FILE = "scraping_progress.json"
LOG_FILE = "scraper_log.txt"

# Create necessary directories
os.makedirs(DATA_DIR, exist_ok=True)

def log_message(message):
    """Log a message with timestamp to the log file"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_entry)
    
    print(message)

def save_progress(current_page, total_pages, jobs_collected):
    """Save current progress to resume later if needed"""
    progress = {
        "current_page": current_page,
        "total_pages": total_pages,
        "jobs_collected": jobs_collected,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, indent=4)

def load_progress():
    """Load previous progress if it exists"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"current_page": 1, "total_pages": None, "jobs_collected": 0}

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
        # Navigate to pagination info
        pagination = data['props']['pageProps']['dehydratedState']['queries'][0]['state']['data']['pagination']
        total_results = pagination['totalResults']
        # Calculate total pages
        total_pages = (total_results + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
        return total_pages, total_results
    except (KeyError, TypeError):
        log_message("Could not determine total pages from data")
        return None, None

def extract_job_listings(data):
    """Extract all job listings from the data"""
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
                technologies = offer.get('technologies', [])
                job_description = offer.get('jobDescription', '')
                url = offer['offers'][0].get('offerAbsoluteUri', '')
                
                # Get additional details
                position_levels = offer.get('positionLevels', [])
                contract_types = offer.get('typesOfContract', [])
                work_schedules = offer.get('workSchedules', [])
                work_modes = offer.get('workModes', [])
                salary = offer.get('salaryDisplayText', '')
                
                # Create job listing object
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
                    "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                job_listings.append(job_listing)
                
            except Exception as e:
                log_message(f"Error processing job offer: {e}")
                continue
        
        return job_listings
    
    except KeyError as e:
        log_message(f"Error navigating JSON structure: {e}")
        return []

def scrape_page(page_number):
    """Scrape a specific page of job listings"""
    url = f"{BASE_URL}?pn={page_number}"
    log_message(f"Scraping page {page_number}: {url}")
    
    try:
        response = requests.get(url, headers=HEADERS)
        
        if response.status_code != 200:
            log_message(f"Failed to fetch page {page_number}: HTTP {response.status_code}")
            return None
        
        html = response.text
        job_data = extract_job_data(html)
        
        if not job_data:
            log_message(f"No job data found on page {page_number}")
            return None
        
        # Save the raw page data for debugging if needed
        os.makedirs(f"{DATA_DIR}/raw_pages", exist_ok=True)
        with open(f"{DATA_DIR}/raw_pages/page_{page_number}.html", "w", encoding="utf-8") as f:
            f.write(html)
        
        return job_data
    
    except Exception as e:
        log_message(f"Error scraping page {page_number}: {e}")
        return None

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
        unique_jobs[job["id"]] = job
    
    all_unique_jobs = list(unique_jobs.values())
    
    with open(f"{DATA_DIR}/all_jobs_consolidated.json", "w", encoding="utf-8") as f:
        json.dump(all_unique_jobs, f, indent=4, ensure_ascii=False)
    
    log_message(f"Consolidated {len(all_unique_jobs)} unique job listings into all_jobs_consolidated.json")
    return all_unique_jobs

def main():
    """Main function to scrape all job listings"""
    log_message("Starting bulk job scraper for pracuj.pl")
    
    # Load progress if available
    progress = load_progress()
    current_page = progress["current_page"]
    total_pages = progress["total_pages"]
    jobs_collected = progress["jobs_collected"]
    
    log_message(f"Resuming from page {current_page}, already collected {jobs_collected} jobs")
    
    # If we don't know the total pages yet, get it from the first page
    if not total_pages:
        first_page_data = scrape_page(1)
        if first_page_data:
            total_pages, total_results = get_total_pages(first_page_data)
            if total_pages:
                log_message(f"Found {total_results} total job listings across {total_pages} pages")
                save_progress(current_page, total_pages, jobs_collected)
            else:
                log_message("Could not determine total pages, assuming 3400 pages (68000/20)")
                total_pages = 1100 # Assumption based on 68000 jobs with 20 per page
    
    batch_size = 100  # Number of pages per batch
    current_batch = []
    batch_number = (current_page - 1) // batch_size + 1
    
    try:
        # Start scraping from the current page
        while current_page <= total_pages:
            # Add random delay to avoid getting blocked
            delay = random.uniform(1.0, 3.0)
            time.sleep(delay)
            
            job_data = scrape_page(current_page)
            
            if job_data:
                job_listings = extract_job_listings(job_data)
                
                if job_listings:
                    current_batch.extend(job_listings)
                    jobs_collected += len(job_listings)
                    log_message(f"Page {current_page}/{total_pages}: Found {len(job_listings)} jobs, total collected: {jobs_collected}")
                else:
                    log_message(f"No job listings found on page {current_page}")
            else:
                log_message(f"Failed to get data from page {current_page}, skipping")
            
            # Save batch when we reach batch_size or at the end
            if len(current_batch) >= batch_size * RESULTS_PER_PAGE or current_page == total_pages:
                save_batch(current_batch, batch_number)
                current_batch = []
                batch_number += 1
            
            # Update progress after each page
            current_page += 1
            save_progress(current_page, total_pages, jobs_collected)
            
            # Periodically consolidate data
            if current_page % 100 == 0:
                log_message("Periodically consolidating data...")
                consolidate_data()
    
    except KeyboardInterrupt:
        log_message("Scraping interrupted by user")
    except Exception as e:
        log_message(f"Unexpected error: {e}")
    finally:
        # Save any remaining job listings
        if current_batch:
            save_batch(current_batch, batch_number)
        
        # Final consolidation
        all_jobs = consolidate_data()
        log_message(f"Scraping completed. Total unique job listings collected: {len(all_jobs) if all_jobs else 0}")

if __name__ == "__main__":
    main()