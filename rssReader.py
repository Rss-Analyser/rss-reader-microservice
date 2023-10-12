import feedparser
from datetime import datetime, timedelta
import sqlite3
from tenacity import retry, stop_after_attempt, wait_fixed


class RSSReader:

    def __init__(self, database="rss_links.db", days_to_crawl=1):
        self.database = database
        self.days_to_crawl = days_to_crawl

    def _get_rss_links(self):
        with sqlite3.connect(self.database) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT link FROM rss_links")
            return [row[0] for row in cursor.fetchall()]

    def _load_existing_entries(self, table_name):
        with sqlite3.connect(self.database) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT publisher, title, link, published, language FROM {table_name}")
            return set(cursor.fetchall())
        
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def _fetch_rss_entries(self, rss_link):

            feed = feedparser.parse(rss_link)
            current_entries = set()
            cutoff_date = datetime.now() - timedelta(days=self.days_to_crawl)

            publisher_name = feed.feed.title if hasattr(feed.feed, 'title') else "Unknown Publisher"
            language = feed.feed.language if hasattr(feed.feed, 'language') else "Unknown Language" 

            for entry in feed.entries:

                try:
                    title = entry.title if hasattr(entry, 'title') else ""
                    link = entry.link if hasattr(entry, 'link') else ""
                    published = entry.published if hasattr(entry, 'published') else ""

                    try:
                        pub_date = datetime.strptime(published, '%a, %d %b %Y %H:%M:%S %Z')
                        if pub_date >= cutoff_date:
                            current_entries.add((publisher_name, title, link, published, language))
                    except ValueError:
                        pass
                except Exception as e:
                    print(e)
                    pass

            return current_entries

    def _save_to_db(self, entries, table_name):
        with sqlite3.connect(self.database) as conn:
            cursor = conn.cursor()
            for entry in entries:
                try:
                    cursor.execute(f'''
                    INSERT INTO {table_name} (publisher, title, link, published, language)
                    VALUES (?, ?, ?, ?, ?)
                    ''', entry)
                except sqlite3.IntegrityError:  # Link already exists
                    pass

    def create_table_for_run(self):
        table_name = "rss_entries_" + datetime.now().strftime("%Y%m%d")
        with sqlite3.connect(self.database) as conn:
            cursor = conn.cursor()
            cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                publisher TEXT NOT NULL,
                title TEXT NOT NULL,
                link TEXT UNIQUE NOT NULL,
                published TEXT NOT NULL,
                language TEXT NOT NULL
            )
            ''')
        return table_name

    def start(self, rss_links):
        table_name = self.create_table_for_run()

        for rss_link in rss_links:
            current_entries = self._fetch_rss_entries(rss_link)
            existing_entries = self._load_existing_entries(table_name)
            new_entries = current_entries - existing_entries

            if new_entries:
                self._save_to_db(new_entries, table_name)

