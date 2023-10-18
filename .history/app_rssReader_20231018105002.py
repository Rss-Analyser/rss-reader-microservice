import os
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor
from tenacity import RetryError
import yaml
from rssReader import RSSReader
import sqlite3
import datetime
import threading
import time

app = Flask(__name__)

CONFIG_PATH = "./config_rssReader.yaml"

# Load config from config.yaml
with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

cockroachdb_conn_str = "dbname=" + config['database'].get('dbname', '')
if 'user' in config['database'] and config['database']['user']:
    cockroachdb_conn_str += " user=" + config['database']['user']
if 'password' in config['database'] and config['database']['password']:
    cockroachdb_conn_str += " password=" + config['database']['password']
cockroachdb_conn_str += " host=" + config['database'].get('host', '')
cockroachdb_conn_str += " port=" + config['database'].get('port', '')

DATABASE_PATH = cockroachdb_conn_str
CHUNK_SIZE = config.get('chunk_size', 50)  # Gets chunk size from config or defaults to 50
DAYS_TO_CRAWL = config.get('days_to_crawl', 1)

RSS_READER_STATUS = {
    "status": "idle",
    "entries_crawled": 0,
    "rss_feeds_crawled":0,
    "start_time":None,
    "runtime":None,
    "current_runtime":None
}

entries_lock = threading.Lock()  # Lock for thread-safe updates

def fetch_rss_links_from_db(chunk_size=CHUNK_SIZE):
    """Fetch RSS links from the database and split them into chunks."""
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT link FROM rss_links")
            all_links = [row[0] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []

    # Splitting the all_links list into chunks of size chunk_size
    for i in range(0, len(all_links), chunk_size):
        yield all_links[i:i + chunk_size]

def fetch_single_rss_link(rss_link, done_event):
    """
    Fetch a single RSS link using the RSSReader.
    Signals the done_event when finished.
    """
    try:
        reader = RSSReader(DATABASE_PATH, days_to_crawl=DAYS_TO_CRAWL)
        reader.start([rss_link])
    finally:
        done_event.set()



import threading

def fetch_rss_data_chunk(rss_links_chunk):
    global RSS_READER_STATUS

    TIMEOUT_DURATION = 10  # e.g., 10 seconds

    for rss_link in rss_links_chunk:
        print(rss_link)

        max_attempts = 3
        attempts = 0

        while attempts < max_attempts:
            done_event = threading.Event()
            rss_thread = threading.Thread(target=fetch_single_rss_link, args=(rss_link, done_event))
            rss_thread.start()

            done_event.wait(timeout=TIMEOUT_DURATION)
            
            if not done_event.is_set():
                print(f"Fetching RSS from {rss_link} took too long. Skipping this link.")
                with entries_lock:  # Ensuring thread-safe updates
                    RSS_READER_STATUS["rss_feeds_crawled"] += 1
                break

            try:
                with sqlite3.connect(DATABASE_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM rss_entries_{datetime.datetime.now().strftime('%Y%m%d')}")
                    count = cursor.fetchone()[0]

                    with entries_lock:  # Ensuring thread-safe updates
                        RSS_READER_STATUS["entries_crawled"] += count
                        RSS_READER_STATUS["rss_feeds_crawled"] += 1
                break

            except RetryError:
                print(f"Failed to fetch RSS from {rss_link} after multiple retries. Skipping this link.")
                with entries_lock:  # Ensuring thread-safe updates
                    RSS_READER_STATUS["rss_feeds_crawled"] += 1
                break

            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    # Database is locked, wait and retry
                    time.sleep(1)  # Wait for a second before retrying
                    attempts += 1
                    print(e)
                    print(f"retry: {attempts}")

            except Exception as e:
                print(f"Unexpected error for {rss_link}: {str(e)}")
                with entries_lock:  # Ensuring thread-safe updates
                    RSS_READER_STATUS["rss_feeds_crawled"] += 1
                break

    return "success"

        

def run_rss_reader():
    global RSS_READER_STATUS

    RSS_READER_STATUS["start_time"] = time.time()


    chunks = list(fetch_rss_links_from_db())
    
    # with ThreadPoolExecutor() as executor:
    #     results = list(executor.map(fetch_rss_data_chunk, chunks))

    with ThreadPoolExecutor() as executor:
            futures = [executor.submit(fetch_rss_data_chunk, chunk) for chunk in chunks]


    # At this point, all threads have completed, as `map` waits for all threads to finish
    print("finish")
    end_time = time.time()  # Capture the end time

    # Calculate the runtime
    RSS_READER_STATUS["runtime"] = end_time - RSS_READER_STATUS["start_time"]

    print("finish")
    RSS_READER_STATUS["status"] = "idle"

@app.route('/start-rss-read', methods=['POST'])
def start_rss_read():
    global RSS_READER_STATUS
    
    if RSS_READER_STATUS["status"] == "idle":
        RSS_READER_STATUS["status"] = "running"
        
        threading.Thread(target=run_rss_reader).start()
        
        return jsonify({"message": "RSS reading started."})
    else:
        return jsonify({"message": "RSS Reader is already running."})

@app.route('/status', methods=['GET'])
def get_status():
    global RSS_READER_STATUS

    # If the process is still running, calculate the current runtime
    if RSS_READER_STATUS["status"] == "running":
        current_time = time.time()
        RSS_READER_STATUS["current_runtime"] = current_time - RSS_READER_STATUS["start_time"]
    else:
        # If the process is not running, you might want to set "current_runtime" to None.
        RSS_READER_STATUS["current_runtime"] = None

    return jsonify(RSS_READER_STATUS)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5002)
