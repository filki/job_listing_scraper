from bs4 import BeautifulSoup
import requests
import time
import csv

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
}

def select_job_value(job, html_value):
     value = job.select_one(html_value).text.strip() if job.select_one(
        html_value) else "N/A"
     return value

def scrape_page(url):
    jobs = []
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to scrape the page. Status code: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")

    for job in soup.select('div[class="tiles_cjkyq1p"]'):
        title = job.select_one('h2[class="tiles_h1p4o5k6"]').text.strip()
        link = job.select_one('a')['href']
        location = select_job_value(job, 'h4[data-test="text-region"]')
        company = select_job_value(job, 'h3[data-test="text-company-name"]')
        level = select_job_value(job, 'li[data-test="offer-additional-info-0"]')
        job_type = select_job_value(job, 'li[data-test="offer-additional-info-1"]')
        skills = [span.text.strip() for span in job.select('div[data-test="technologies-list"] span[data-test="technologies-item"]')]
        pay = ' '.join(job.select_one('span[class="tiles_s1x1fda3"]').stripped_strings).replace('\xa0', ' ') if job.select_one('span[class="tiles_s1x1fda3"]') else "N/A"

        skills_str = ', '.join([span.strip() for span in skills]) if skills else "N/A"

        jobs.append({
            'title': title,
            'link': f"https://it.pracuj.pl{link}",
            'location': location,
            'company': company,
            'level' : level,
            'type' : job_type,
            'skills' : skills_str,
            'pay' : pay
        })
    return jobs

def scrape_pages(base_url, start_page=1, max_pages=20):
    all_jobs = []
    for page_num in range(start_page, max_pages + 1):
        print(f"Scraping page {page_num}...")
        url = f"{base_url}&pn={page_num}"
        jobs = scrape_page(url)
        all_jobs.extend(jobs)

        time.sleep(1)

    return all_jobs

def save_to_csv(jobs, filename='jobs_pracujpl.csv'):
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=jobs[0].keys())
        writer.writeheader()
        writer.writerows(jobs)

if __name__ == "__main__":
    base_url = "https://it.pracuj.pl/praca?its=big-data-science%2Cai-ml%2Cbusiness-analytics"
    max_pages = 13  # Set the maximum number of pages you want to scrape

    job_listings = scrape_pages(base_url, max_pages=max_pages)

    # Save the scraped jobs to a CSV file
    if job_listings:
        save_to_csv(job_listings)
        print(f"Scraping completed. Data saved to 'jobs_pracujpl.csv'.")
    else:
        print("No jobs found.")

    # Uncomment if u want listings printed in console
    # for job in job_listings:
    #     print(job)
