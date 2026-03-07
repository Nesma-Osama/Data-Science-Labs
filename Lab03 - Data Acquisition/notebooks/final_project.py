import sqlite3
import logging
import time
import os
from datetime import datetime
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np


class BookMarketIntelligence:
    CATEGORY_SLUGS = {
        'Fiction':         'fiction_10',
        'Mystery':         'mystery_3',
        'Science Fiction': 'science-fiction_16',
        'History':         'history_32',
    }

    RATING_MAP = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}

    def __init__(self, db_path='market_intelligence.db'):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('pipeline.log'),
                logging.StreamHandler(),
            ],
        )
        self.logger = logging.getLogger('BookMarket')

        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self._create_tables()

        self.session = requests.Session()
        self.session.headers.update(
            {'User-Agent': 'BookMarketIntelligence/1.0 (Educational)'}
        )

        self.logger.info(f"Pipeline initialised — DB: {db_path}")


    def _create_tables(self):
        cur = self.conn.cursor()

        # Books scraped from the web
        cur.execute('''
            CREATE TABLE IF NOT EXISTS web_books (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                title       TEXT    NOT NULL,
                price       REAL,
                rating      INTEGER,
                in_stock    INTEGER,   -- 1 = in stock, 0 = out of stock
                category    TEXT,
                scraped_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Repositories from GitHub API
        cur.execute('''
            CREATE TABLE IF NOT EXISTS github_repos (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                name         TEXT,
                full_name    TEXT,
                stars        INTEGER,
                forks        INTEGER,
                language     TEXT,
                description  TEXT,
                html_url     TEXT,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Books from local library.db (joined with authors)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS library_books (
                book_id          INTEGER PRIMARY KEY,
                title            TEXT,
                author           TEXT,
                genre            TEXT,
                publication_year INTEGER,
                copies_available INTEGER
            )
        ''')

        # Pipeline audit log
        cur.execute('''
            CREATE TABLE IF NOT EXISTS pipeline_logs (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type       TEXT,
                records_collected INTEGER,
                status            TEXT,         -- 'success' | 'error'
                error_message     TEXT,         -- NULL on success
                timestamp         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        self.conn.commit()


    def _log(self, source_type, records, status, error=None):
        self.conn.cursor().execute(
            '''INSERT INTO pipeline_logs
               (source_type, records_collected, status, error_message)
               VALUES (?, ?, ?, ?)''',
            (source_type, records, status, error),
        )
        self.conn.commit()


    def collect_from_database(self, source_db_path='library.db'):
        self.logger.info(f"[DB] Connecting to {source_db_path}")
        try:
            src = sqlite3.connect(source_db_path)
            df = pd.read_sql_query(
                '''
                SELECT b.book_id,
                       b.title,
                       a.name             AS author,
                       b.genre,
                       b.publication_year,
                       b.copies_available
                FROM   books   b
                JOIN   authors a ON b.author_id = a.author_id
                ''',
                src,
            )
            src.close()

            df.to_sql('library_books', self.conn, if_exists='replace', index=False)
            self.conn.commit()

            self.logger.info(f"[DB] ✓ {len(df)} library books collected")
            self._log('database', len(df), 'success')
            return df

        except Exception as e:
            self.logger.error(f"[DB] ✗ {e}")
            self._log('database', 0, 'error', str(e))
            return pd.DataFrame()

    def collect_from_api(self, query='books python', per_page=30):
        self.logger.info(f"[API] Searching GitHub: '{query}'")
        try:
            response = self.session.get(
                'https://api.github.com/search/repositories',
                params={
                    'q':        query,
                    'sort':     'stars',
                    'order':    'desc',
                    'per_page': per_page,
                },
                timeout=10,
            )
            response.raise_for_status()
            items = response.json().get('items', [])

            rows = [
                {
                    'name':        repo.get('name'),
                    'full_name':   repo.get('full_name'),
                    'stars':       repo.get('stargazers_count', 0),
                    'forks':       repo.get('forks_count', 0),
                    'language':    repo.get('language'),
                    'description': repo.get('description'),
                    'html_url':    repo.get('html_url'),
                }
                for repo in items
            ]

            df = pd.DataFrame(rows)
            df.to_sql('github_repos', self.conn, if_exists='replace', index=False)
            self.conn.commit()

            self.logger.info(f"[API] ✓ {len(df)} repos collected")
            self._log('api', len(df), 'success')
            return df

        except Exception as e:
            self.logger.error(f"[API] ✗ {e}")
            self._log('api', 0, 'error', str(e))
            return pd.DataFrame()


    def collect_from_web(self, categories=None, max_pages=2):
        if categories is None:
            categories = list(self.CATEGORY_SLUGS.keys())

        base = 'http://books.toscrape.com'
        all_books = []

        for cat in categories:
            slug = self.CATEGORY_SLUGS.get(cat)
            if not slug:
                self.logger.warning(f"[WEB] Unknown category: {cat}")
                continue

            url = f"{base}/catalogue/category/books/{slug}/index.html"
            page = 1

            while url and page <= max_pages:
                try:
                    resp = self.session.get(url, timeout=10)
                    resp.raise_for_status()
                    soup = BeautifulSoup(resp.text, 'lxml')

                    for article in soup.select('article.product_pod'):
                        title_el   = article.select_one('h3 a')
                        price_text = article.select_one('.price_color').text.strip()
                        rating_cls = article.select_one('.star-rating').get('class')[1]
                        avail      = article.select_one('.availability').text.strip()

                        try:
                            price = float(
                                price_text.replace('£', '').replace('Â', '').strip()
                            )
                        except ValueError:
                            self.logger.warning(f"[WEB] Skipping invalid price: {price_text}")
                            continue

                        rating = self.RATING_MAP.get(rating_cls, 0)
                        if not (1 <= rating <= 5):
                            self.logger.warning(f"[WEB] Skipping invalid rating: {rating_cls}")
                            continue

                        title = title_el.get('title', '').strip()
                        if not title:
                            continue

                        all_books.append({
                            'title':    title,
                            'price':    price,
                            'rating':   rating,
                            'in_stock': int('In stock' in avail),
                            'category': cat,
                        })

                    next_btn = soup.select_one('li.next a')
                    url = urljoin(url, next_btn['href']) if next_btn else None
                    page += 1

                    if url and page <= max_pages:
                        time.sleep(1)

                except Exception as e:
                    self.logger.error(f"[WEB] Error on {url}: {e}")
                    break

            cat_count = sum(1 for b in all_books if b['category'] == cat)
            self.logger.info(f"[WEB] '{cat}' — {cat_count} books")

        df = pd.DataFrame(all_books)

        if not df.empty:
            before = len(df)
            df.drop_duplicates(subset=['title', 'category'], inplace=True)
            if len(df) < before:
                self.logger.info(f"[WEB] Removed {before - len(df)} duplicate books")

            df.to_sql('web_books', self.conn, if_exists='replace', index=False)
            self.conn.commit()

        self.logger.info(f"[WEB] ✓ {len(df)} total web books collected")
        self._log('web', len(df), 'success')
        return df


    def analyze_and_visualize(self, df_web, df_api, df_db):
        COLORS = ['#4C72B0', '#DD8452', '#55A868', '#C44E52',
                  '#8172B2', '#937860', '#DA8BC3', '#8C8C8C']

        fig, axes = plt.subplots(2, 3, figsize=(20, 11))
        fig.suptitle(
            'Book Market Intelligence — Analysis Report',
            fontsize=17, fontweight='bold', y=1.01,
        )

        ax1 = axes[0, 0]
        avg_price = (
            df_web.groupby('category')['price'].mean().sort_values(ascending=False)
        )
        bars1 = ax1.bar(avg_price.index, avg_price.values,
                        color=COLORS[:len(avg_price)], edgecolor='white')
        ax1.set_title('Avg Price by Category (£)', fontweight='bold')
        ax1.set_ylabel('Price (£)')
        ax1.set_xticklabels(avg_price.index, rotation=15, ha='right')
        ax1.bar_label(bars1, fmt='£%.2f', padding=3, fontsize=8)
        ax1.yaxis.set_major_formatter(ticker.FormatStrFormatter('£%.0f'))

        ax2 = axes[0, 1]
        avg_rating = (
            df_web.groupby('category')['rating'].mean().sort_values(ascending=False)
        )
        bars2 = ax2.bar(avg_rating.index, avg_rating.values,
                        color=COLORS[:len(avg_rating)], edgecolor='white')
        ax2.set_title('Avg Rating by Category (1–5)', fontweight='bold')
        ax2.set_ylabel('Rating')
        ax2.set_ylim(0, 5.5)
        ax2.set_xticklabels(avg_rating.index, rotation=15, ha='right')
        ax2.bar_label(bars2, fmt='%.2f', padding=3, fontsize=8)

        ax3 = axes[0, 2]
        rating_counts = df_web['rating'].value_counts().sort_index()
        ax3.bar(rating_counts.index, rating_counts.values,
                color='#4C72B0', edgecolor='white')
        ax3.set_title('Rating Distribution (Web Books)', fontweight='bold')
        ax3.set_xlabel('Star Rating')
        ax3.set_ylabel('Number of Books')
        ax3.set_xticks([1, 2, 3, 4, 5])
        for i, v in zip(rating_counts.index, rating_counts.values):
            ax3.text(i, v + 0.3, str(v), ha='center', fontsize=9)

        ax4 = axes[1, 0]
        stock_pct = (
            df_web.groupby('category')['in_stock'].mean() * 100
        ).sort_values(ascending=False)
        bars4 = ax4.bar(stock_pct.index, stock_pct.values,
                        color=COLORS[:len(stock_pct)], edgecolor='white')
        ax4.set_title('In-Stock Availability (%)', fontweight='bold')
        ax4.set_ylabel('% In Stock')
        ax4.set_ylim(0, 115)
        ax4.set_xticklabels(stock_pct.index, rotation=15, ha='right')
        ax4.bar_label(bars4, fmt='%.1f%%', padding=3, fontsize=8)

        ax5 = axes[1, 1]
        if not df_api.empty:
            top10 = df_api.nlargest(10, 'stars')
            ax5.barh(top10['name'], top10['stars'],
                     color='#55A868', edgecolor='white')
            ax5.set_title('Top 10 Book Repos on GitHub (⭐)', fontweight='bold')
            ax5.set_xlabel('Stars')
            ax5.invert_yaxis()
        else:
            ax5.text(0.5, 0.5, 'No API data available',
                     ha='center', va='center', transform=ax5.transAxes, fontsize=12)
            ax5.set_title('GitHub Repos', fontweight='bold')

        ax6 = axes[1, 2]
        if not df_db.empty and 'genre' in df_db.columns:
            genre_counts = df_db['genre'].value_counts()
            ax6.pie(
                genre_counts.values,
                labels=genre_counts.index,
                autopct='%1.0f%%',
                startangle=140,
                colors=COLORS[:len(genre_counts)],
            )
            ax6.set_title('Library Genres (Database)', fontweight='bold')
        else:
            ax6.text(0.5, 0.5, 'No DB data available',
                     ha='center', va='center', transform=ax6.transAxes, fontsize=12)
            ax6.set_title('Library Genres (Database)', fontweight='bold')

        plt.tight_layout()
        plt.savefig('market_analysis.png', dpi=150, bbox_inches='tight')
        plt.show()
        print("Saved market_analysis.png")

        insights = {
            'most_expensive_category': avg_price.idxmax(),
            'cheapest_category':       avg_price.idxmin(),
            'highest_rated_category':  avg_rating.idxmax(),
            'total_web_books':         len(df_web),
            'total_github_repos':      len(df_api),
            'total_library_books':     len(df_db),
            'avg_price_overall':       round(df_web['price'].mean(), 2),
            'avg_rating_overall':      round(df_web['rating'].mean(), 2),
            'pct_in_stock':            round(df_web['in_stock'].mean() * 100, 1),
        }
        return insights


    def generate_report(self, insights):
        """
        Write analysis.html — a standalone HTML report with insights and charts.

        Args:
            insights: dict returned by analyze_and_visualize()
        """
        now = datetime.now().strftime('%Y-%m-%d %H:%M')

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Book Market Intelligence Report</title>
  <style>
    body  {{ font-family: Arial, sans-serif; max-width: 960px;
             margin: 40px auto; color: #333; line-height: 1.6; }}
    h1    {{ color: #2c5282; border-bottom: 3px solid #2c5282; padding-bottom: 8px; }}
    h2    {{ color: #2b6cb0; margin-top: 36px; }}
    table {{ border-collapse: collapse; width: 100%; margin: 16px 0; }}
    th, td {{ border: 1px solid #ddd; padding: 10px 14px; text-align: left; }}
    th    {{ background: #2c5282; color: #fff; }}
    tr:nth-child(even) {{ background: #f7fafc; }}
    .cards {{ display: flex; flex-wrap: wrap; gap: 12px; margin: 16px 0; }}
    .card  {{ background: #ebf8ff; border-left: 4px solid #3182ce;
              padding: 14px 22px; border-radius: 4px; min-width: 160px; }}
    .card b {{ display: block; font-size: 1.4em; color: #2c5282; }}
    img   {{ max-width: 100%; border-radius: 6px;
             box-shadow: 0 2px 10px rgba(0,0,0,.15); margin-top: 20px; }}
    ul li {{ margin-bottom: 6px; }}
    footer {{ color: #aaa; font-size: .85em; margin-top: 48px;
              border-top: 1px solid #eee; padding-top: 12px; }}
  </style>
</head>
<body>

<h1>Book Market Intelligence Report</h1>
<p>Generated: <strong>{now}</strong></p>

<h2>Executive Summary</h2>
<div class="cards">
  <div class="card"><b>{insights['total_web_books']}</b>Web Books Scraped</div>
  <div class="card"><b>{insights['total_github_repos']}</b>GitHub Repos</div>
  <div class="card"><b>{insights['total_library_books']}</b>Library Books (DB)</div>
  <div class="card"><b>£{insights['avg_price_overall']}</b>Avg Web Price</div>
  <div class="card"><b>{insights['avg_rating_overall']} ★</b>Avg Rating</div>
  <div class="card"><b>{insights['pct_in_stock']}%</b>In Stock</div>
</div>

<h2>Market Insights</h2>
<table>
  <tr><th>Metric</th><th>Finding</th></tr>
  <tr><td>Most expensive category (avg)</td><td><strong>{insights['most_expensive_category']}</strong></td></tr>
  <tr><td>Cheapest category (avg)</td><td><strong>{insights['cheapest_category']}</strong></td></tr>
  <tr><td>Highest rated category</td><td><strong>{insights['highest_rated_category']}</strong></td></tr>
  <tr><td>Overall average price</td><td>£{insights['avg_price_overall']}</td></tr>
  <tr><td>Overall average rating</td><td>{insights['avg_rating_overall']} / 5</td></tr>
  <tr><td>In-stock percentage</td><td>{insights['pct_in_stock']}%</td></tr>
</table>

<h2>Visualisations</h2>
<img src="market_analysis.png" alt="Market Analysis Charts — 5 panels">

<h2>Recommendations</h2>
<ul>
  <li><strong>{insights['highest_rated_category']}</strong> books receive the
      highest customer ratings — prioritise stocking this genre.</li>
  <li><strong>{insights['cheapest_category']}</strong> has the lowest average
      price — a strong entry point for budget-conscious buyers.</li>
  <li>With <strong>{insights['pct_in_stock']}%</strong> of titles in stock,
      availability is healthy, but supply for top-rated genres should be
      monitored closely.</li>
  <li>GitHub trends show strong developer interest in book-related tools —
      digital/API integrations represent a growing opportunity.</li>
  <li>Cross-referencing web prices with library circulation data can reveal
      which genres offer the best value relative to reader demand.</li>
</ul>

<h2>Data Collection Statistics</h2>
<table>
  <tr><th>Source</th><th>Records Collected</th></tr>
  <tr><td>Web Scraping (books.toscrape.com)</td><td>{insights['total_web_books']}</td></tr>
  <tr><td>GitHub API</td><td>{insights['total_github_repos']}</td></tr>
  <tr><td>Library Database (library.db)</td><td>{insights['total_library_books']}</td></tr>
</table>

<footer>
  Book Market Intelligence System &mdash; Data Science Lab 03 &mdash; {now}
</footer>
</body>
</html>"""

        with open('analysis.html', 'w', encoding='utf-8') as f:
            f.write(html)
        print("Saved analysis.html")
        

    def export_all_data(self, output_dir='exports'):
        """
        Export every pipeline table to a CSV file in output_dir.

        Files created:
            exports/web_books.csv
            exports/github_repos.csv
            exports/library_books.csv
            exports/pipeline_logs.csv
        """
        os.makedirs(output_dir, exist_ok=True)
        for table in ['web_books', 'github_repos', 'library_books', 'pipeline_logs']:
            df = pd.read_sql_query(f"SELECT * FROM {table}", self.conn)
            path = f"{output_dir}/{table}.csv"
            df.to_csv(path, index=False, encoding='utf-8')
            print(f" {path}  ({len(df)} rows)")
        self.logger.info(f"All data exported to {output_dir}/")

    def close(self):
        """Close the database connection cleanly."""
        self.conn.close()
        self.logger.info("Pipeline closed")


    def run(self):
        """
        Execute all five pipeline steps end-to-end:
          1. Collect from database
          2. Collect from GitHub API
          3. Scrape books.toscrape.com
          4. Analyse + visualise
          5. Generate HTML report + export CSVs
        """
        print("=" * 60)
        print("  BOOK MARKET INTELLIGENCE SYSTEM")
        print("=" * 60)

        print("\n[1/5] Collecting from database (library.db)...")
        df_db = self.collect_from_database()

        print("\n[2/5] Collecting from GitHub API...")
        df_api = self.collect_from_api(query='books python data', per_page=20)

        print("\n[3/5] Scraping books.toscrape.com (2 pages × 4 categories)...")
        df_web = self.collect_from_web(max_pages=2)

        print("\n[4/5] Analysing data & generating charts...")
        insights = self.analyze_and_visualize(df_web, df_api, df_db)

        print("\n[5/5] Writing HTML report & exporting CSVs...")
        self.generate_report(insights)
        self.export_all_data()

        print("\n" + "=" * 60)
        print("  PIPELINE COMPLETE")
        print(f"      Web books      : {insights['total_web_books']}")
        print(f"      GitHub repos   : {insights['total_github_repos']}")
        print(f"      Library books  : {insights['total_library_books']}")
        print()
        print("  Output files:")
        print(f"      market_intelligence.db")
        print(f"      market_analysis.png")
        print(f"      analysis.html")
        print(f"      exports/  (4 CSV files)")
        print(f"      pipeline.log")
        print("=" * 60)

        self.close()
        return insights



if __name__ == '__main__':
    pipeline = BookMarketIntelligence(db_path='market_intelligence.db')
    pipeline.run()
