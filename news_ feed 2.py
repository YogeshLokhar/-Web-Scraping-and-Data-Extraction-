import sqlite3
import requests
import pandas as pd
from bs4 import BeautifulSoup
from langdetect import detect
import html
from datetime import datetime, timedelta
from dateutil import parser
import re

class NewsScraper:
    def __init__(self, rss_feeds):
        self.rss_feeds = rss_feeds
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }
        self.db_name = "news.db"
        self._init_db()

    def _init_db(self):
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
                Duration TEXT,
                ScrapedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()

    def _store_in_db(self, entries):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        country_counts = {}
        
        for entry in entries:
            country = entry["Country"]
            country_counts[country] = country_counts.get(country, 0) + 1

        for entry in entries:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO news_articles 
                    (Title, Summary, Published, Link, Source, Country, Language, NoofArticles, Duration)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    entry["Title"],
                    entry["Summary"],
                    entry["Published"],
                    entry["Link"],
                    entry["Source"],
                    entry["Country"],
                    entry["Language"],
                    country_counts[entry["Country"]],
                    entry["Duration"]
                ))
            except sqlite3.Error as e:
                print(f"SQLite error for {entry['Link']}: {e}")
        conn.commit()
        conn.close()

    def is_url_accessible(self, url):
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def clean_summary(self, summary):
        soup = BeautifulSoup(summary, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        return html.unescape(text)

    def clean_date_string(self, date_str):
        """Preprocess date strings to handle common irregularities"""
        if not date_str:
            return None
        
        # Remove day names in non-English languages
        date_str = re.sub(r'^[A-Za-z]{2,9},\s*', '', date_str)
        
        # Handle timezone abbreviations
        date_str = re.sub(r'\b(?:EST|EDT|CST|CDT|MST|MDT|PST|PDT)\b', '', date_str)
        
        # Remove multiple spaces
        date_str = ' '.join(date_str.split())
        
        return date_str.strip()

    def calculate_duration(self, published_date):
        """Improved date parsing with multiple fallback formats"""
        if not published_date:
            return "Unknown"
        
        cleaned_date = self.clean_date_string(published_date)
        if not cleaned_date:
            return "Unknown"
        
        date_formats = [
            '%a, %d %b %Y %H:%M:%S %z',  # RFC 2822 with timezone
            '%a, %d %b %Y %H:%M:%S %Z',  # RFC 2822 with timezone name
            '%Y-%m-%dT%H:%M:%S%z',       # ISO 8601
            '%Y-%m-%d %H:%M:%S',         # Simple datetime
            '%d %b %Y %H:%M:%S',         # Day Month Year time
            '%a, %d %b %Y %H:%M:%S',     # RFC 2822 without timezone
            '%Y-%m-%d',                   # Just date
            '%b %d, %Y %H:%M:%S',        # Month day, Year time
            '%d-%m-%Y %H:%M:%S',          # European date format
            '%m/%d/%Y %H:%M:%S',         # US date format
            '%Y%m%d'                      # Compact date format
        ]
        
        pub_date = None
        # First try dateutil's parser
        try:
            pub_date = parser.parse(cleaned_date)
        except:
            pass
        
        # If that fails, try manual formats
        if not pub_date:
            for fmt in date_formats:
                try:
                    pub_date = datetime.strptime(cleaned_date, fmt)
                    break
                except ValueError:
                    continue
        
        if not pub_date:
            return "Unknown"
        
        now = datetime.now(pub_date.tzinfo) if pub_date.tzinfo else datetime.now()
        delta = now - pub_date
        
        if delta.days == 0:
            return "Today"
        elif delta.days <= 7:
            return "This Week"
        elif delta.days <= 30:
            return "This Month"
        elif delta.days <= 365:
            return "1 Year Ago"
        elif delta.days <= 730:
            return "2 Years Ago"
        else:
            return "Older"

    def parse_feed(self, feed):
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
                pub_date_tag = item.find("pubDate") or item.find("updated") or item.find("date")
                link_tag = item.find("link")

                link = link_tag["href"] if link_tag and link_tag.has_attr("href") else link_tag.text.strip() if link_tag else ""
                title = title_tag.text.strip() if title_tag else ""
                summary = self.clean_summary(summary_tag.text) if summary_tag else ""
                published = pub_date_tag.text.strip() if pub_date_tag else ""
                
                duration = self.calculate_duration(published)

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
                        "Language": language,
                        "Duration": duration
                    })

        except Exception as e:
            print(f"Error parsing feed {feed['url']}: {e}")
        return entries

    def scrape_all_feeds(self):
        all_entries = []
        for feed in self.rss_feeds:
            print(f"Processing {feed['country']} - {feed['source']}")
            feed_entries = self.parse_feed(feed)
            all_entries.extend(feed_entries)
            print(f"  - Added {len(feed_entries)} entries")
        return all_entries

    def remove_duplicates(self, entries):
        df = pd.DataFrame(entries)
        df.drop_duplicates(subset=["Title", "Link"], inplace=True)
        return df

    def save_to_csv(self, df, filename="news_data.csv"):
        country_counts = df['Country'].value_counts().to_dict()
        df['NoOfArticles'] = df['Country'].map(country_counts)
        df.to_csv(filename, index=False, encoding="utf-8")
        print(f"Saved news data to {filename}")

    def run(self):
        print(f"Job started at {datetime.now()}")
        entries = self.scrape_all_feeds()
        df_cleaned = self.remove_duplicates(entries)
        self._store_in_db(df_cleaned.to_dict("records"))
        print(f"Stored {len(df_cleaned)} entries in SQLite DB: {self.db_name}")
        self.save_to_csv(df_cleaned)
        print(f"Job finished at {datetime.now()}\n")

# Enhanced RSS feed configuration
RSS_FEEDS = [

    # UK
    {"country": "UK", "source": "BBC", "url": "http://feeds.bbci.co.uk/news/rss.xml"},
    {"country": "UK", "source": "The Guardian", "url": "https://www.theguardian.com/uk/rss"},

    # US
    {"country": "US", "source": "CNN", "url": "http://rss.cnn.com/rss/edition.rss"},
    {"country": "US", "source": "New York Times", "url": "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml"},
    {"country": "US", "source": "Washington Post", "url": "https://feeds.washingtonpost.com/rss/politics"},

    # Canada
    {"country": "Canada", "source": "CBC", "url": "https://rss.cbc.ca/lineup/topstories.xml"},
    {"country": "Canada", "source": "Global News", "url": "https://globalnews.ca/feed/"},

    # India
    {"country": "India", "source": "NDTV", "url": "https://feeds.feedburner.com/ndtvnews-top-stories"},
    {"country": "India", "source": "Times of India", "url": "https://timesofindia.indiatimes.com/rssfeedstopstories.cms"},
    {"country": "India", "source": "The Hindu", "url": "https://www.thehindu.com/feeder/default.rss"},

    # Japan
    {"country": "Japan", "source": "NHK", "url": "https://www3.nhk.or.jp/rss/news/cat0.xml"},
    {"country": "Japan", "source": "The Japan Times", "url": "https://www.japantimes.co.jp/feed/topstories/"},
    {"country": "Japan", "source": "Asahi Shimbun", "url": "https://www.asahi.com/rss/asahi/newsheadlines.rdf"},

    # China
    {"country": "China", "source": "China Daily", "url": "http://www.chinadaily.com.cn/rss/china_rss.xml"},
    {"country": "China", "source": "South China Morning Post", "url": "https://www.scmp.com/rss/91/feed"},

    # Russia
    {"country": "Russia", "source": "RT News", "url": "https://www.rt.com/rss/news/"},
    {"country": "Russia", "source": "TASS", "url": "https://tass.com/rss/v2.xml"},
    {"country": "Russia", "source": "Moscow Times", "url": "https://www.themoscowtimes.com/rss/news"},

    # South Korea
    {"country": "South Korea", "source": "Korea.net", "url": "https://www.korea.net/koreanet/rss/news/3"},
    {"country": "South Korea", "source": "Yonhap News", "url": "https://en.yna.co.kr/RSS/news.xml"},

    # Singapore
    {"country": "Singapore", "source": "Straits Times", "url": "https://www.straitstimes.com/news/singapore/rss.xml"},
    {"country": "Singapore", "source": "CNA", "url": "https://www.channelnewsasia.com/rssfeeds/8395986"},

    # Pakistan
    {"country": "Pakistan", "source": "Dawn News", "url": "https://www.dawn.com/feeds/home"},
   
    {"country": "Pakistan", "source": "The News International", "url": "https://www.thenews.com.pk/rss/1/1"},

    # Bangladesh
    {"country": "Bangladesh", "source": "The Daily Star", "url": "https://www.thedailystar.net/frontpage/rss.xml"},
    {"country": "Bangladesh", "source": "Dhaka Tribune", "url": "https://www.dhakatribune.com/feed/"},
   
    # Sri Lanka
   {"country": "Sri Lanka", "source": "Ada Derana", "url": "https://www.adaderana.lk/rss.php"},
    {"country": "Sri Lanka", "source": "EconomyNext", "url": "https://economynext.com/feed"},
   {"country": "Sri Lanka", "source": "Colombo Gazette", "url": "https://colombogazette.com/feed/"},

    # Thailand
    {"country": "Thailand", "source": "Bangkok Post", "url": "https://www.bangkokpost.com/rss/data/topstories.xml"},
    {"country": "Thailand", "source": "The Thaiger", "url": "https://thethaiger.com/feed"},
    {"country": "Thailand", "source": "Khaosod English", "url": "https://www.khaosodenglish.com/feed/"},

    # Hong Kong
    {"country": "Hong Kong", "source": "RTHK News", "url": "https://rthk.hk/rthk/news/rss/e_expressnews_elocal.xml"},
    {"country": "Hong Kong", "source": "South China Morning Post", "url": "https://www.scmp.com/rss/91/feed"},
    
    # Malaysia
   {"country": "Malaysia", "source": "The Star", "url": "https://www.thestar.com.my/rss/News/"},
    {"country": "Malaysia", "source": "The Sun Daily", "url": "https://www.thesundaily.my/rss/Home.xml"},
    {"country": "Malaysia", "source": "BERNAMA", "url": "https://www.bernama.com/en/rssfeed.php"},

    # Nepal
    {"country": "Nepal", "source": "The Kathmandu Post", "url": "https://kathmandupost.com/rss"},
    {"country": "Nepal", "source": "Online Khabar", "url": "https://english.onlinekhabar.com/feed"},
    {"country": "Nepal", "source": "Nepal News", "url": "https://www.nepalnews.com/rss"},

    # Indonesia
    {"country": "Indonesia", "source": "Antara News", "url": "https://en.antaranews.com/rss/news.xml"},
    {"country": "Indonesia", "source": "Tempo", "url": "https://rss.tempo.co/"},

    # Philippines
    {"country": "Philippines", "source": "Rappler", "url": "https://www.rappler.com/rss/"},
    {"country": "Philippines", "source": "Philippine Star", "url": "https://www.philstar.com/rss/headlines"},
   

    # Vietnam
    {"country": "Vietnam", "source": "VNExpress", "url": "https://vnexpress.net/rss/tin-moi-nhat.rss"},
    {"country": "Vietnam", "source": "Tuoi Tre News", "url": "https://tuoitrenews.vn/rss/home.rss"},
    {"country": "Vietnam",  "source": "Vietnam News","url": "https://vietnamnews.vn/rss" }

]


if __name__ == "__main__":
    scraper = NewsScraper(RSS_FEEDS)
    scraper.run()