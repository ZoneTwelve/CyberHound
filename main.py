import os
import time
import queue
import logging
import threading
import requests
import concurrent.futures
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')

# Global variables
web_data_queue = queue.Queue()
data_lock = threading.Lock()
robots_parsers = {}
rate_limits = {}
domain_last_access = {}

# Constants
THREAD_COUNT = 10  # Adjust based on your system's capabilities
RATE_LIMIT_DELAY = 1  # Seconds

# Function to check robots.txt compliance
def is_allowed(url):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    if domain not in robots_parsers:
        robots_url = f"{parsed_url.scheme}://{domain}/robots.txt"
        rp = RobotFileParser()
        try:
            rp.set_url(robots_url)
            rp.read()
            robots_parsers[domain] = rp
        except Exception as e:
            logging.warning(f"Failed to read robots.txt from {robots_url}: {e}")
            robots_parsers[domain] = None  # Assume allowed if robots.txt cannot be fetched
    rp = robots_parsers.get(domain)
    if rp:
        return rp.can_fetch('*', url)
    return True

# Function to enforce rate limiting per domain
def rate_limit(domain):
    with data_lock:
        last_access = domain_last_access.get(domain, 0)
        elapsed = time.time() - last_access
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)
        domain_last_access[domain] = time.time()

# Data Segmentation
def segment_data(large_text_data, n_segments):
    urls = large_text_data.strip().split('\n')
    segments = [[] for _ in range(n_segments)]
    for i, url in enumerate(urls):
        segments[i % n_segments].append(url.strip())
    logging.info(f"Data segmented into {n_segments} segments.")
    return segments

# URL Crawling
def crawl_urls(segment):
    crawled_data = []
    for url in segment:
        if not is_allowed(url):
            logging.info(f"Crawling disallowed by robots.txt: {url}")
            continue
        parsed_url = urlparse(url)
        rate_limit(parsed_url.netloc)
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            logging.info(f"Crawled URL: {url}")
            web_data_queue.put((url, response.text))
        except requests.RequestException as e:
            logging.warning(f"Failed to crawl URL: {url}, Error: {e}")
    logging.info("Finished crawling segment.")

# Targeted Data Extraction
def extract_data():
    while True:
        try:
            url, html_content = web_data_queue.get(timeout=10)
            soup = BeautifulSoup(html_content, 'html.parser')
            # Example extraction: extract page title
            title = soup.title.string if soup.title else 'No Title'
            logging.info(f"Extracted data from URL: {url}")
            write_data(url, title)
            web_data_queue.task_done()
        except queue.Empty:
            logging.info("No more data to extract.")
            break

# Data Writing
def write_data(url, data):
    filename = 'targeted_data.txt'
    with data_lock:
        with open(filename, 'a', encoding='utf-8') as f:
            f.write(f"{url}\t{data}\n")
    logging.info(f"Wrote data for URL: {url}")

# Result Aggregation
def aggregate_results():
    # In this simple example, data is already written to a single file.
    # You can implement further aggregation logic if needed.
    logging.info("Aggregation complete.")

# Main function
def main():
    # Sample large text data containing URLs
    large_text_data = "https://blog.zonetwelve.io"  # Replicate URLs for demonstration

    n_segments = THREAD_COUNT
    segments = segment_data(large_text_data, n_segments)

    # Start crawling threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_segments) as executor:
        futures = [executor.submit(crawl_urls, segment) for segment in segments]

    # Start extraction threads
    extraction_threads = []
    for _ in range(THREAD_COUNT):
        t = threading.Thread(target=extract_data)
        t.start()
        extraction_threads.append(t)

    # Wait for crawling to finish
    for future in futures:
        future.result()

    # Wait for the queue to be empty
    web_data_queue.join()

    # Wait for extraction threads to finish
    for t in extraction_threads:
        t.join()

    # Aggregate results
    aggregate_results()

    logging.info("Web crawling process completed.")

if __name__ == "__main__":
    main()
