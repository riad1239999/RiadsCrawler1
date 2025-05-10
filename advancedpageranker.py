
from bs4 import BeautifulSoup
import requests
import time
import random
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import threading
from urllib.parse import urlparse
import csv
import sys
import os
# Add the root directory to sys.path
# This is to be able to import modules from other directories (indexing and serving) idk why...
# any imports from indexing/serving need to happen under this
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from indexing.advanced_indexing import advanced_index_page
from serving.pagerank import compute_pagerank


# Function to check robots.txt for permission to crawl
# If we don't do this, we could get blocked/banned
# since we don't have permission to crawl.
def can_crawl(url):
    parsed_url = urlparse(url)
    robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
    print(f"Checking robots.txt for: {robots_url}")
    time.sleep(random.uniform(1, 3))
    try:
        response = requests.get(robots_url, timeout=5)
        response.raise_for_status()
        disallowed_paths = []
        for line in response.text.splitlines():
            if line.startswith("Disallow"):
                parts = line.split()
                if len(parts) > 1:
                    disallowed_paths.append(parts[1])
        for path in disallowed_paths:
            if urlparse(url).path.startswith(path):
                print(f"Disallowed by robots.txt: {url}")
                return False
        return True
    except requests.RequestException:
        print(f"Failed to access robots.txt: {robots_url}")
        return False  # If we can't access robots.txt, assume we can't crawl (we're being nice here)

# Function to fetch and parse URL
def crawl(args):
    queue = args['queue']
    visited_urls = args['visited_urls']
    crawl_count = args['crawl_count']
    CRAWL_LIMIT = args['CRAWL_LIMIT']
    lock = args['lock']
    index = args['index']
    webpage_info = args['webpage_info']
    webpage_id_counter = args['webpage_id_counter']
    pagerank_graph = args['pagerank_graph']
    stop_crawl = args['stop_crawl']

    while not stop_crawl.is_set():
        try:
            current_url = queue.get(timeout=5)
            print("Time to crawl: " + current_url)
        except Exception:
            break  # Exit if no more URLs are available to crawl

        with lock:
            if crawl_count[0] >= CRAWL_LIMIT:
                queue.queue.clear()  # Clear remaining URLs to stop processing
                print("Crawl limit reached. Exiting...")
                stop_crawl.set()
                break
            if current_url in visited_urls:
                queue.task_done()
                continue
            visited_urls.add(current_url)

        """ ok yay
        """
        if not can_crawl(current_url):
            queue.task_done()
            continue

        time.sleep(random.uniform(2, 5))
        try:
            response = requests.get(current_url, timeout=5)
            response.raise_for_status()  # Check for request errors
            content = response.content

            """ ok yay
            """
            if 'noindex' in content.decode('utf-8').lower():
               print(f"Noindex found, skipping: {current_url}")
               queue.task_done()
               continue
            

            # Parse the fetched content to find new URLs
            webpage = BeautifulSoup(content, "html.parser")

            # Index the webpage
            indexed_page = advanced_index_page(webpage, current_url)
            with lock:
                for word in indexed_page["words"]:
                    if word not in index:
                        index[word] = set()
                    index[word].add(webpage_id_counter[0])
                webpage_info[webpage_id_counter[0]] = indexed_page
                webpage_id_counter[0] += 1

            hyperlinks = webpage.select("a[href]")
            #NEW: Add hyperlink connections for pagerank
            new_urls, hyperlink_connections = parse_links(hyperlinks, current_url)
            pagerank_graph[current_url] = hyperlink_connections

            with lock:
                for new_url in new_urls:
                    if new_url not in visited_urls:
                        queue.put(new_url)
                crawl_count[0] += 1

        except requests.RequestException as e:
            print(f"Failed to fetch {current_url}: {e}")
        finally:
            queue.task_done()

# Function to parse links from HTML content
def parse_links(hyperlinks, current_url):
    urls = []
    #NEW: Add hyperlink connections for pagerank
    hyperlink_connections = set()
    for hyperlink in hyperlinks:
        url = hyperlink["href"]

        # Format the URL into a proper URL
        if url.startswith("#"):
            continue  # Skip same-page anchors
        if url.startswith("//"):
            url = "https:" + url  # Add scheme to protocol-relative URLs
        elif url.startswith("/"):
            # Construct full URL for relative links
            base_url = "{0.scheme}://{0.netloc}".format(requests.utils.urlparse(current_url))
            url = base_url + url
        elif not url.startswith("http"):
            continue  # Skip non-HTTP links
        url = url.split("#")[0]  # Remove anchor

        hyperlink_connections.add(url)
        urls.append(url)
    return urls, hyperlink_connections

# Main crawling function
def sloth_bot():
    # Start with the initial pages to crawl
    starting_urls = [
        "https://www.wikipedia.org/wiki/Cats",
        "https://www.youtube.com",
        "https://news.ycombinator.com/",
    ]

    urls_to_crawl = Queue()
    for seed_url in starting_urls:
        urls_to_crawl.put(seed_url)

    visited_urls = set()  # URL tracking
    CRAWL_LIMIT = 20  # Set crawl limit
    crawl_count = [0]  # Shared counter
    lock = threading.Lock()  # Thread safety lock
    index = {}
    webpage_info = {}
    #NEW: pagerank graph for pagerank.
    # This will be used to store the connections between hyperlinks
    pagerank_graph = {}
    webpage_id_counter = [0]
    stop_crawl = threading.Event()

    # Start concurrent crawling with ThreadPoolExecutor
    #Concurrency = speed
    #Threads go BRRRRR
    #Increase this if you want more threads, but be careful with these.
    NUM_WORKERS = 100
    #Setting up arguments for the crawl function
    args = {
        'queue': urls_to_crawl,
        'visited_urls': visited_urls,
        'crawl_count': crawl_count,
        'CRAWL_LIMIT': CRAWL_LIMIT,
        'lock': lock,
        'index': index,
        'webpage_info': webpage_info,
        'webpage_id_counter': webpage_id_counter,
        'pagerank_graph': pagerank_graph,
        'stop_crawl': stop_crawl
    }

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for _ in range(NUM_WORKERS):
            executor.submit(crawl, args)

        print("URLS HAVE BEEN CRAWLED!")

    #NEW: Computes pagerank
    pagerank_scores = compute_pagerank(pagerank_graph)
    
    
    """ This part is for saving the data to CSV files.
        However, if you don't want to save the data, you can remove/comment out this part.
        If you want to use a database, you can replace this part with a database connection.
    """
    with open('advanced_pagerank_inverted_index.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['word', 'doc_ids']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for word, doc_ids in index.items():
            writer.writerow({'word': word, 'doc_ids': list(doc_ids)})

    with open('advanced_pagerank.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['doc_id', 'url', 'title', 'description', 'pagerank']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for doc_id, info in webpage_info.items():
            writer.writerow({
                'doc_id': doc_id,
                'url': info['url'],
                'title': info['title'],
                'description': info['description'],
                'pagerank': pagerank_scores.get(info['url'], 0)
            })

# Entry point for the script
def main():
    sloth_bot()

if __name__ == "__main__":
    main()
