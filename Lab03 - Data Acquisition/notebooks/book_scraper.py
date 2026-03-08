import requests
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import time
import logging
import json

from bs4 import BeautifulSoup
from datetime import datetime
from collections import deque
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse, urljoin


def scrape_travel_books():
    """
    Scrape all travel books.

    Requirements:
    - Handle pagination
    - Add 1 second delay between pages
    - Extract all required fields

    Returns:
        DataFrame with book data
    """
    url = 'http://books.toscrape.com/catalogue/category/books/travel_2/index.html'

    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Educational Purpose) TravelScraper/1.0'})

    rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
    books = []
    page = 1

    while url:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        for article in soup.select('article.product_pod'):
            book = {}

            title_element = article.select_one('h3 a')
            book['title'] = title_element.get('title')

            price_text = article.select_one('.price_color').text.strip()
            book['price'] = float(price_text.replace('£', '').replace('Â', ''))

            rating_class = article.select_one('.star-rating').get('class')[1]
            book['rating'] = rating_map.get(rating_class, 0)

            availability = article.select_one('.availability').text.strip()
            book['availability'] = availability
            book['in_stock'] = 'In stock' in availability

            books.append(book)

        print(f"  Found {len(soup.select('article.product_pod'))} books on this page")

        next_btn = soup.select_one('li.next a')
        if next_btn:
            url = urljoin(url, next_btn['href'])
            page += 1
            time.sleep(1)
        else:
            url = None

    print(f"\nTotal travel books scraped: {len(books)}")
    df = pd.DataFrame(books)
    df.to_csv('task1_travel_books.csv', index=False)
    print("Saved to task1_travel_books.csv")
    return df





class CategoryScraper:
    """
    Scrape multiple categories and compare.
    """

    CATEGORY_SLUGS = {
        'Fiction':            'fiction_10',
        'Mystery':            'mystery_3',
        'Historical Fiction': 'historical-fiction_4',
        'Science Fiction':    'science-fiction_16',
    }

    RATING_MAP = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}

    def __init__(self):
        self.base_url = 'http://books.toscrape.com'
        self.session = requests.Session()
        self.session.headers.update(
            {'User-Agent': 'Mozilla/5.0 (Educational Purpose) CategoryScraper/1.0'}
        )

    def scrape_category(self, category_name, max_pages=2):
        """
        Scrape books from a category (up to max_pages).

        Args:
            category_name: Human-readable category name (must be in CATEGORY_SLUGS)
            max_pages:     Maximum pages to scrape

        Returns:
            list: Book dictionaries with title, price, rating, category
        """
        slug = self.CATEGORY_SLUGS.get(category_name)
        if slug is None:
            raise ValueError(
                f"Unknown category '{category_name}'. "
                f"Available: {list(self.CATEGORY_SLUGS.keys())}"
            )

        url = f"{self.base_url}/catalogue/category/books/{slug}/index.html"
        books = []
        page = 1

        while url and page <= max_pages:
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'lxml')

                for article in soup.select('article.product_pod'):
                    book = {}

                    title_el = article.select_one('h3 a')
                    book['title'] = title_el.get('title', '').strip()

                    price_text = article.select_one('.price_color').text.strip()
                    book['price'] = float(
                        price_text.replace('£', '').replace('Â', '').strip()
                    )

                    rating_class = article.select_one('.star-rating').get('class')[1]
                    book['rating'] = self.RATING_MAP.get(rating_class, 0)

                    availability = article.select_one('.availability').text.strip()
                    book['in_stock'] = 'In stock' in availability
                    book['category'] = category_name
                    books.append(book)

                next_btn = soup.select_one('li.next a')
                url = urljoin(url, next_btn['href']) if next_btn else None
                page += 1

                if url and page <= max_pages:
                    time.sleep(1)

            except Exception as e:
                print(f"Error scraping {url}: {e}")
                break

        print(f"{len(books)} books collected from '{category_name}'")
        return books

    def scrape_multiple_categories(self, categories):
        """
        Scrape multiple categories and return a combined DataFrame.

        Args:
            categories: list of category names

        Returns:
            DataFrame with columns: title, price, rating, in_stock, category
        """
        all_books = []
        for cat in categories:
            books = self.scrape_category(cat, max_pages=2)
            all_books.extend(books)

        df = pd.DataFrame(all_books)
        df.to_csv('task2_categories.csv', index=False)
        print(f"\nCombined data saved to task2_categories.csv  ({len(df)} rows)")
        return df

    def compare_categories(self, df):
        """
        Comparative analysis across categories.

        Computes:
        - Average price per category
        - Average rating per category
        - In-stock percentage per category

        Returns:
            dict: Comparison statistics
        """
        stats = {}

        price_stats  = df.groupby('category')['price'].agg(['mean', 'min', 'max'])
        rating_stats = df.groupby('category')['rating'].mean()
        stock_pct    = df.groupby('category')['in_stock'].mean() * 100

        for cat in df['category'].unique():
            stats[cat] = {
                'avg_price':    round(price_stats.loc[cat, 'mean'], 2),
                'min_price':    round(price_stats.loc[cat, 'min'],  2),
                'max_price':    round(price_stats.loc[cat, 'max'],  2),
                'avg_rating':   round(rating_stats[cat], 2),
                'pct_in_stock': round(stock_pct[cat],    1),
                'book_count':   int((df['category'] == cat).sum()),
            }

        best_rated = max(stats, key=lambda c: stats[c]['avg_rating'])
        most_expensive = max(stats, key=lambda c: stats[c]['avg_price'])

        print("\n" + "=" * 60)
        print("CATEGORY COMPARISON REPORT")
        print("=" * 60)
        for cat, s in stats.items():
            print(
                f"\n{cat} ({s['book_count']} books)\n"
                f"   Avg Price  : £{s['avg_price']:.2f}  "
                f"(£{s['min_price']:.2f} – £{s['max_price']:.2f})\n"
                f"   Avg Rating : {s['avg_rating']:.2f}\n"
                f"   In Stock   : {s['pct_in_stock']:.1f}%"
            )

        print(f"\nHighest avg rating  : {best_rated}")
        print(f"Most expensive (avg): {most_expensive}")

        categories_order = list(stats.keys())
        summary = pd.DataFrame(stats).T.reindex(categories_order).reset_index()
        summary.columns = ['category', 'avg_price', 'min_price', 'max_price', 'avg_rating', 'pct_in_stock', 'book_count']
        summary = summary.astype({'avg_price': float, 'avg_rating': float, 'pct_in_stock': float, 'book_count': int})

        colors = ['#4C72B0', '#DD8452', '#55A868', '#C44E52']
        x = np.arange(len(summary))
        bar_w = 0.55

        fig, axes = plt.subplots(1, 3, figsize=(16, 5))
        fig.suptitle('Book Categories — Comparative Analysis', fontsize=15, fontweight='bold', y=1.02)

        ax1 = axes[0]
        bars1 = ax1.bar(x, summary['avg_price'], width=bar_w, color=colors, edgecolor='white', linewidth=0.8)
        ax1.set_title('Average Price (£)', fontsize=12, fontweight='bold')
        ax1.set_xticks(x)
        ax1.set_xticklabels(summary['category'], rotation=20, ha='right', fontsize=9)
        ax1.set_ylabel('Price (£)')
        ax1.yaxis.set_major_formatter(ticker.FormatStrFormatter('£%.1f'))
        ax1.bar_label(bars1, fmt='£%.2f', padding=3, fontsize=8)
        idx_exp = summary[summary['category'] == most_expensive].index[0]
        bars1[idx_exp].set_edgecolor('gold')
        bars1[idx_exp].set_linewidth(2.5)

        ax2 = axes[1]
        bars2 = ax2.bar(x, summary['avg_rating'], width=bar_w, color=colors,edgecolor='white', linewidth=0.8)
        ax2.set_title('Average Rating', fontsize=12, fontweight='bold')
        ax2.set_xticks(x)
        ax2.set_xticklabels(summary['category'], rotation=20, ha='right', fontsize=9)
        ax2.set_ylabel('Rating (1–5)')
        ax2.set_ylim(0, 5.5)
        ax2.bar_label(bars2, fmt='%.2f', padding=3, fontsize=8)
        idx_rat = summary[summary['category'] == best_rated].index[0]
        bars2[idx_rat].set_edgecolor('gold')
        bars2[idx_rat].set_linewidth(2.5)

        ax3 = axes[2]
        bars3 = ax3.bar(x, summary['pct_in_stock'], width=bar_w, color=colors,
                        edgecolor='white', linewidth=0.8)
        ax3.set_title('In-Stock Availability (%)', fontsize=12, fontweight='bold')
        ax3.set_xticks(x)
        ax3.set_xticklabels(summary['category'], rotation=20, ha='right', fontsize=9)
        ax3.set_ylabel('% In Stock')
        ax3.set_ylim(0, 115)
        ax3.bar_label(bars3, fmt='%.1f%%', padding=3, fontsize=8)

        plt.tight_layout()
        plt.savefig('task2_comparison.png', dpi=150, bbox_inches='tight')
        plt.show()
        print("\nsaved to task2_comparison.png")
        return stats


class AdvancedBookScraper:
    """
    Production-ready book scraper with logging, rate limiting, and error recovery.
    """
    CATEGORY_SLUGS = {
        'Fiction':            'fiction_10',
        'Mystery':            'mystery_3',
        'Historical Fiction': 'historical-fiction_4',
        'Science Fiction':    'science-fiction_16',
        'Fantasy':            'fantasy_19',
    }

    RATING_MAP = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}

    def __init__(self, output_dir='scraped_data'):
        """
        Initialize scraper with logging and rate limiting.
        """
        # Setup file-based logging — all events will be written to scraper.log
        logging.basicConfig(
            filename='scraper.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
        )
        self.logger = logging.getLogger(__name__)

        # TODO: Add the following:
        # - Rate limiter (track request times to enforce max 10/min)
        # - requests.Session with proper headers
        # - Progress tracker (dict mapping page -> status)
        self.output_dir = output_dir
        self.rate_limiter = deque()
        self.max_requests  = 10
        self.time_window   = 60.0
        self.progress      = {}  
        self.base_url = 'http://books.toscrape.com'
        self.session = requests.Session()
        self.session.headers.update(
            {'User-Agent': 'Mozilla/5.0 (Educational Purpose) CategoryScraper/1.0'}
        )

    def wait_for_rate_limit(self):
        now = time.time()
        while self.rate_limiter and now - self.rate_limiter[0] > self.time_window:
            self.rate_limiter.popleft()

        if len(self.rate_limiter) >= self.max_requests:
            wait_time = self.time_window - (now - self.rate_limiter[0])
            print(f"Rate limit reached — waiting {wait_time:.1f}s...")
            time.sleep(wait_time)

        self.rate_limiter.append(time.time())

    def check_robots_txt(self, url):
        """
        Check if scraping is allowed for the given URL.
        """
        try:
            parsed = urlparse(url)
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
            rp = RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            allowed = rp.can_fetch(self.session.headers['User-Agent'], url)
            return allowed

        except Exception as e:
            return False

    def scrape_with_retry(self, url, max_attempts=3):
        """
        Scrape a URL with exponential backoff on failure.
        Retry delays: 1s → 2s → 4s
        """
        for attempt in range(1, max_attempts + 1):
            try:
                self.wait_for_rate_limit()
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                return response.text

            except Exception as e:
                wait_time = 2 ** (attempt - 1)
                self.logger.warning(f"Attempt {attempt} failed for {url}: {e}")
                if attempt < max_attempts:
                    print(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"All attempts failed for {url}")
                    return None

    def validate_book_data(self, book):
        """
        Validate a book record before adding to results.

        Checks:
        - Price is a valid positive number
        - Rating is between 1 and 5
        - Title is not empty

        Returns:
            bool: True if valid
        """
        if not book.get('title'):
            self.logger.warning("Invalid book data: missing title")
            return False
        if not isinstance(book.get('price'), (int, float)) or book['price'] <= 0:
            self.logger.warning(f"Invalid price for book '{book.get('title')}': {book.get('price')}")
            return False
        if not (1 <= book.get('rating', 0) <= 5):
            self.logger.warning(f"Invalid rating for book '{book.get('title')}': {book.get('rating')}")
            return False
        return True

    def save_progress(self, books, filename='progress.json'):
        """
        Save current scraping progress to disk (for resumability).
        """
        data = {
            'timestamp': datetime.now().isoformat(),
            'book_count': len(books),
            'books': books,
        }
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        self.logger.info(f"Progress saved: {len(books)} books → {filename}")

    def load_progress(self, filename='progress.json'):
        """
        Load previous progress from disk to resume interrupted scraping.
        """
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            books = data.get('books', []) if isinstance(data, dict) else data
            self.progress = books
            self.logger.info(f"Resumed progress: {len(books)} books loaded from {filename}")
            return books

        except FileNotFoundError:
            self.logger.info("No previous progress found — starting fresh")
            return []

        except json.JSONDecodeError as e:
            self.logger.error(f"Corrupted progress file: {e}")
            return []

    def export_data(self, books, base_filename='task3_books'):
        """
        Export collected data to multiple file formats.

        Creates:
        - task3_books.csv   (UTF-8 encoded)
        - task3_books.xlsx  (with formatted headers)
        - task3_books.json  (properly structured)
        """
        df = pd.DataFrame(books)
        df.to_csv(f'{base_filename}.csv', index=False, encoding='utf-8')
        df.to_excel(f'{base_filename}.xlsx', index=False)
        with open(f'{base_filename}.json', 'w', encoding='utf-8') as f:
            json.dump(books, f, indent=2)
        self.logger.info(f"Data exported to {base_filename}.csv, .xlsx, and .json")
        self.logger.info(f"Exported {len(books)} books to {base_filename}.csv / .xlsx / .json")
        print(f"Exported {len(books)} books to {base_filename}.csv / .xlsx / .json")

    def run_full_pipeline(self, categories, max_pages_per_category=5):
        """
        Complete end-to-end scraping pipeline.

        Steps:
        1. Check robots.txt
        2. Load previous progress (if exists)
        3. Scrape each category
        4. Validate data
        5. Save progress after each category
        6. Export final results
        7. Generate summary report
        """
        accessible = self.check_robots_txt(self.base_url)

        if not accessible:
            self.logger.warning("Scraping not allowed by robots.txt")
            return

        previous = self.load_progress()
        if not previous:
            self.logger.info("No previous progress found, starting fresh.")

        all_books = list(previous)

        for category in categories:
            self.logger.info(f"Starting scrape for category: {category}")
            url = f"{self.base_url}/catalogue/category/books/{self.CATEGORY_SLUGS[category]}/index.html"
            books = []
            page = 1

            while url and page <= max_pages_per_category:
                html = self.scrape_with_retry(url)

                if not html:
                    self.logger.error(f"Failed to retrieve {url}")
                    break

                soup = BeautifulSoup(html, 'lxml')

                for article in soup.select('article.product_pod'):
                    book = {}

                    title_el = article.select_one('h3 a')
                    book['title'] = title_el.get('title', '').strip()

                    price_text = article.select_one('.price_color').text.strip()
                    book['price'] = float(
                        price_text.replace('£', '').replace('Â', '').strip()
                    )

                    rating_class = article.select_one('.star-rating').get('class')[1]
                    book['rating'] = self.RATING_MAP.get(rating_class, 0)

                    availability = article.select_one('.availability').text.strip()
                    book['in_stock'] = 'In stock' in availability
                    book['category'] = category
                    books.append(book)

                next_btn = soup.select_one('li.next a')
                url = urljoin(url, next_btn['href']) if next_btn else None
                page += 1

                if url and page <= max_pages_per_category:
                    time.sleep(1)

            valid_books = [b for b in books if self.validate_book_data(b)]
            all_books.extend(valid_books)
            self.save_progress(all_books)
            self.logger.info(f"Finished '{category}': {len(valid_books)} valid books")

        self.export_data(all_books)
        self.logger.info("Scraping pipeline completed.")

        print(f"\n{'='*50}")
        print(f"PIPELINE COMPLETE — {len(all_books)} total books")
        df_summary = pd.DataFrame(all_books)
        if not df_summary.empty:
            print(df_summary.groupby('category')[['price', 'rating']].mean().round(2))
        print(f"{'='*50}")



df_travel = scrape_travel_books()
df_travel.head()

cat_scraper = CategoryScraper()
categories = ['Fiction', 'Mystery', 'Historical Fiction', 'Science Fiction']
df_cats = cat_scraper.scrape_multiple_categories(categories)
stats = cat_scraper.compare_categories(df_cats)
df_cats.head()

scraper = AdvancedBookScraper()
scraper.run_full_pipeline(
    categories=['Mystery', 'Science Fiction', 'Fantasy'], max_pages_per_category=3
)