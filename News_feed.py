import sqlite3
import requests
import pandas as pd
from bs4 import BeautifulSoup
from langdetect import detect
import html
from datetime import datetime

class NewsScraper:
    def __init__(self, rss_feeds):
        self.rss_feeds = rss_feeds
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
            )
        }
        self.db_name = "news.db"
        self._init_db()

    def _init_db(self):
        """Initialize SQLite database and create table"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS news_articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Title TEXT NOT NULL,
                Summary TEXT,
                Published TEXT,
                Link TEXT UNIQUE,
                Source TEXT,
                Country TEXT,
                Language TEXT,
                NoofArticles INTEGER,
                ScrapedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()

    def _store_in_db(self, entries):
        """Store entries in SQLite database"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        # First count articles per country
        country_counts = {}
        for entry in entries:
            country = entry["Country"]
            country_counts[country] = country_counts.get(country, 0) + 1
        
        # Then store with correct counts
        for entry in entries:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO news_articles 
                    (Title, Summary, Published, Link, Source, Country, Language, NoofArticles)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    entry["Title"],
                    entry["Summary"],
                    entry["Published"],
                    entry["Link"],
                    entry["Source"],
                    entry["Country"],
                    entry["Language"],
                    country_counts[entry["Country"]]
                ))
            except sqlite3.Error as e:
                print(f"SQLite error for {entry['Link']}: {e}")
        conn.commit()
        conn.close()

    def is_url_accessible(self, url):
        """Check if URL is reachable"""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def clean_summary(self, summary):
        """Clean HTML tags from summary"""
        soup = BeautifulSoup(summary, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        return html.unescape(text)

    def parse_feed(self, feed):
        """Parse individual RSS feed"""
        entries = []
        if not self.is_url_accessible(feed["url"]):
            print(f"Network error: Cannot access {feed['url']}")
            return entries

        try:
            response = requests.get(feed["url"], headers=self.headers)
            soup = BeautifulSoup(response.content, features="xml")
            items = soup.find_all("item") or soup.find_all("entry")

            for item in items:
                title_tag = item.find("title")
                summary_tag = item.find("description") or item.find("summary") or item.find("content")
                pub_date_tag = item.find("pubDate") or item.find("updated")
                link_tag = item.find("link")

                link = link_tag["href"] if link_tag and link_tag.has_attr("href") else link_tag.text.strip() if link_tag else ""
                title = title_tag.text.strip() if title_tag else ""
                summary = self.clean_summary(summary_tag.text) if summary_tag else ""
                published = pub_date_tag.text.strip() if pub_date_tag else ""

                text_for_lang = f"{title} {summary}".strip()
                try:
                    language = detect(text_for_lang) if text_for_lang else "unknown"
                except:
                    language = "unknown"

                if title and link:
                    entries.append({
                        "Title": title,
                        "Summary": summary,
                        "Published": published,
                        "Link": link,
                        "Source": feed["source"],
                        "Country": feed["country"],
                        "Language": language
                    })

        except Exception as e:
            print(f"Error parsing feed {feed['url']}: {e}")
        return entries

    def scrape_all_feeds(self):
        """Scrape all configured RSS feeds"""
        all_entries = []
        for feed in self.rss_feeds:
            print(f"Processing {feed['country']} - {feed['source']}")
            feed_entries = self.parse_feed(feed)
            all_entries.extend(feed_entries)
            print(f"  - Added {len(feed_entries)} entries")
        return all_entries

    def remove_duplicates(self, entries):
        """Remove duplicate entries using pandas"""
        df = pd.DataFrame(entries)
        df.drop_duplicates(subset=["Title", "Link"], inplace=True)
        return df

    def save_to_csv(self, df, filename="news_data.csv"):
        """Save data to CSV file"""
        # Calculate NoOfArticles before saving to CSV
        country_counts = df['Country'].value_counts().to_dict()
        df['NoOfArticles'] = df['Country'].map(country_counts)
        df.to_csv(filename, index=False, encoding="utf-8")
        print(f"Saved news data to {filename}")

    def run(self):
        """Main execution method"""
        print(f"Job started at {datetime.now()}")
        entries = self.scrape_all_feeds()
        df_cleaned = self.remove_duplicates(entries)
        
        # Store in SQLite
        self._store_in_db(df_cleaned.to_dict("records"))
        print(f"Stored {len(df_cleaned)} entries in SQLite DB: {self.db_name}")
        
        # Save to CSV
        self.save_to_csv(df_cleaned)
        print(f"Job finished at {datetime.now()}\n")


# RSS Feeds configuration
RSS_FEEDS = [
    {"country": "UK", "source": "BBC", "url": "http://feeds.bbci.co.uk/news/rss.xml"},
    {"country": "US", "source": "CNN", "url": "http://rss.cnn.com/rss/edition.rss"},
    {"country": "Canada", "source": "CBC", "url": "https://rss.cbc.ca/lineup/topstories.xml"},
    {"country": "Australia", "source": "ABC", "url": "https://www.abc.net.au/news/feed/51120/rss.xml"},
    {"country": "India", "source": "NDTV", "url": "https://feeds.feedburner.com/ndtvnews-top-stories"},
    {"country": "Germany", "source": "Deutsche Welle", "url": "https://rss.dw.com/rdf/rss-en-all"},
    {"country": "France", "source": "France 24", "url": "https://www.france24.com/en/rss"},
    {"country": "Japan", "source": "NHK", "url": "https://www3.nhk.or.jp/rss/news/cat0.xml"},
    {"country": "China", "source": "China Daily", "url": "http://www.chinadaily.com.cn/rss/china_rss.xml"},
    {"country": "Russia", "source": "RT News", "url": "https://www.rt.com/rss/news/"},
    {"country": "Brazil", "source": "G1 Globo", "url": "https://g1.globo.com/rss/g1/"},
    {"country": "South Africa", "source": "News24", "url": "https://feeds.news24.com/articles/news24/TopStories/rss"},
    {"country": "Italy", "source": "ANSA", "url": "https://www.ansa.it/sito/ansait_rss.xml"},
    {"country": "Spain", "source": "El Pais", "url": "https://feeds.elpais.com/mrss-s/pages/ep/site/elpais.com/portada"},
    {"country": "Mexico", "source": "Excelsior", "url": "https://www.excelsior.com.mx/rss.xml"},  
    {"country": "Turkey", "source": "Daily Sabah", "url": "https://www.dailysabah.com/rss/turkey"},
    {"country": "South Korea", "source": "Korea.net", "url": "https://www.korea.net/koreanet/rss/news/3"},
    {"country": "New Zealand", "source": "RNZ National", "url": "https://www.rnz.co.nz/rss/national.xml"},
    {"country": "Singapore", "source": "CNA", "url": "https://www.channelnewsasia.com/rssfeeds/8395986"},
    {"country": "Nigeria", "source": "Daily Post Nigeria", "url": "https://dailypost.ng/feed/"},
]

if __name__ == "__main__":
    scraper = NewsScraper(RSS_FEEDS)
    scraper.run()