"""
The following script is designed to crawl a website recursively, extract internal links, and check the status of each URL using Playwright and asyncio in Python.
"""

import requests
import csv
from bs4 import BeautifulSoup
import aiohttp
import asyncio
from playwright.async_api import async_playwright
from tqdm import tqdm
from urllib.parse import urljoin, urlparse
import time
from datetime import datetime
import random


# Function to check if a URL belongs to the allowed domain or subdomains
def is_internal_link(base_domain, url):
    try:
        parsed_url = urlparse(url)
        return base_domain in parsed_url.netloc  # Matches main domain and subdomains
    except Exception:
        return False


# Function to extract links and anchor texts from a given page
async def extract_links(browser, base_url, base_domain):
    print(f"Crawling with JavaScript rendering: {base_url}")
    try:
        page = await browser.new_page()
        await page.goto(base_url, timeout=30000)  # Set a timeout of 30 seconds
        content = await page.content()  # Rendered HTML content
        soup = BeautifulSoup(content, 'html.parser')

        # Define URL patterns to exclude
        exclude_patterns = [
            "Add URLs to exclude here",
            "to prevent crawling certain paths"
        ]

        # Find all links
        links = soup.find_all('a', href=True)
        all_links = []

        for link in links:
            full_url = urljoin(base_url, link['href'])

            if (
                is_internal_link(base_domain, full_url) and  # Internal links only
                not any(
                    full_url.startswith(f"http://{pattern}") or \
                    full_url.startswith(f"https://{pattern}")
                    for pattern in exclude_patterns
                )
            ):
                all_links.append({
                    "Anchor Text": link.text.strip(),
                    "URL": full_url
                })

        await page.close()
        print(f"Total internal links extracted: {len(all_links)}")
        return all_links
    except Exception as e:
        print(f"Error rendering JavaScript for {base_url}: {e}")
        return []


# Function to recursively crawl all internal links starting from the base URL
async def crawl_recursively(base_url, base_domain, max_depth=2):
    print("Starting crawl...")
    to_crawl = [(base_url, 0)]  # Queue of URLs to crawl with their depth
    crawled = set()  # Set to track already crawled URLs
    all_links = []  # List to store all discovered links

    # Launch the Playwright browser
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        while to_crawl:
            current_url, depth = to_crawl.pop(0)  # Get the next URL and its depth
            if current_url in crawled or depth > max_depth:
                continue

            crawled.add(current_url)  # Mark the URL as crawled
            links = await extract_links(browser, current_url, base_domain)

            for link in links:
                link["Page Linked from"] = current_url
                all_links.append(link)
                if link["URL"] not in crawled:  # Add new links to the queue
                    to_crawl.append((link["URL"], depth + 1))

        await browser.close()  # Close the browser after crawling is complete

    print(f"Recursive crawl complete. Total links found: {len(all_links)}")
    return all_links


# Asynchronous function to fetch the status of a URL
async def fetch_status(session, url, retries=3, delay=2):
    visited_urls = set()  # Track visited URLs in the redirect chain
    MAX_REDIRECTS = 5  # Prevent excessive redirects

    for attempt in range(retries):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
            }

            async with session.get(url, allow_redirects=False, headers=headers, timeout=60) as response:
                destination_url = response.headers.get('Location', url)

                # Normalize relative URLs
                if destination_url and not destination_url.startswith("http"):
                    destination_url = urljoin(url, destination_url)

                # Ensure destination_url is not empty or invalid
                if not destination_url or destination_url == url:
                    return url, response.status, ""

                # Detect Redirect Loops
                if destination_url in visited_urls:
                    print(f"⚠️ Redirect Loop Detected: {url} → {destination_url}")
                    return url, "Redirect Loop", destination_url

                visited_urls.add(destination_url)

                # Prevent excessive redirects
                if len(visited_urls) > MAX_REDIRECTS:
                    print(f"⚠️ Too Many Redirects: {url} → {destination_url}")
                    return url, "Too Many Redirects", destination_url

                # Skip URLs with a 200 status code
                if response.status == 200:
                    print(f"Skipping {url} (200 OK)")
                    return None  # Indicate this URL should be skipped

                # Return the original URL, status code, and final destination
                return url, response.status, destination_url
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(delay)
                print(f"Retrying {url}... (Attempt {attempt + 2}/{retries})")
            else:
                print(f"Failed to fetch {url} after {retries} attempts: {e}")
                return url, "Error", str(e)



# Asynchronous processing of all URLs
async def process_urls_async(links, max_concurrent_requests=30):
    print("Processing URLs asynchronously...")
    semaphore = asyncio.Semaphore(max_concurrent_requests)  # Limit concurrency
    tasks = []
    results = []

    request_counter = 0
    start_time = time.time()

    with tqdm(total=len(links), desc="Processing URLs") as pbar:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
            async def fetch_with_limit(link):
                nonlocal request_counter
                async with semaphore:
                    await asyncio.sleep(random.uniform(0.5, 2))  # Random delay (0.5 - 2s)
                    result = await fetch_status(session, link["URL"])

                    request_counter += 1
                    elapsed_time = time.time() - start_time
                    if elapsed_time > 0:
                        requests_per_minute = request_counter / (elapsed_time / 60)
                        print(f"Requests Sent: {request_counter} | Rate: {requests_per_minute:.2f} requests/min")

                    return result

            for link in links:
                tasks.append(fetch_with_limit(link))

            for idx, task in enumerate(asyncio.as_completed(tasks)):
                result = await task
                if result is not None:  # Skip URLs with 200 OK
                    url, status_code, destination = result
                    results.append({
                        "Page Linked from": links[idx]["Page Linked from"],
                        "Anchor Text": links[idx]["Anchor Text"],
                        "URL (linked)": url,
                        "Response Code": status_code,
                        "Destination URL": destination if destination != url else ""
                    })
                pbar.update(1)

    return results


# Function to save results to a CSV file
def save_to_csv(data, output_file):
    print(f"Saving results to {output_file}...")
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ["Page Linked from", "Anchor Text", "URL (linked)", "Response Code", "Destination URL"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(data)


# Main asynchronous workflow
async def main_async(base_url, base_domain):
    # Step 1: Crawl all internal links recursively
    links = await crawl_recursively(base_url, base_domain)  # Add `await` here

    # Step 2: Check the status of each URL
    url_data = await process_urls_async(links)

    return url_data


# Main script
def main():
    date = datetime.now().strftime('%Y-%m-%d')
    base_url = "Set the base URL here"
    base_domain = "Set the base domain here"  # Allow other subdomains
    keyword = base_url.rstrip('/').split('/')[-1]  # Get the last part of the URL path
    output_file = f"{date}_{keyword}_status_report.csv"

    # Start the timer
    start_time = time.time()

    # Run the asynchronous process
    url_data = asyncio.run(main_async(base_url, base_domain))

    # End the timer
    end_time = time.time()

    # Save results to CSV
    save_to_csv(url_data, output_file)
    print(f"Results saved to {output_file}")
    print(f"Total time taken: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
