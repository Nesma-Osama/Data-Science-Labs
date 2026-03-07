# Book Market Intelligence System

A complete data collection and analysis pipeline that gathers book data
from three sources, stores it in SQLite, and generates market insights.

---

## 📦 Project Structure

```
notebooks/
├── final_project.py          ← main pipeline (run this)
├── market_intelligence.db    ← output database (auto-created)
├── market_analysis.png       ← 5-panel visualisation (auto-created)
├── analysis.html             ← HTML report (auto-created)
├── pipeline.log              ← execution log (auto-created)
├── exports/
│   ├── web_books.csv
│   ├── github_repos.csv
│   ├── library_books.csv
│   └── pipeline_logs.csv
└── SQL_notebook/
    └── library.db            ← source database (must exist from Part 1)
```

---

## ⚙️ Installation

```bash
pip install requests beautifulsoup4 lxml pandas matplotlib numpy openpyxl
```

---

## 🚀 How to Run

```bash
cd "Lab03 - Data Acquisition/notebooks"
python final_project.py
```

The script runs all 5 steps automatically and prints progress to the console.

---

## 🗄️ Data Sources

| # | Source | Details |
|---|--------|---------|
| 1 | **SQLite Database** | `SQL_notebook/library.db` — books joined with authors from Part 1 |
| 2 | **GitHub REST API** | `api.github.com/search/repositories` — top repos matching "books python data" |
| 3 | **Web Scraping** | `books.toscrape.com` — Fiction, Mystery, Science Fiction, History (2 pages each) |

---

## 🗂️ Database Schema

```
market_intelligence.db
│
├── library_books       ← from library.db
│   book_id, title, author, genre, publication_year, copies_available
│
├── github_repos        ← from GitHub API
│   id, name, full_name, stars, forks, language, description, html_url
│
├── web_books           ← from books.toscrape.com
│   id, title, price, rating, in_stock, category, scraped_at
│
└── pipeline_logs       ← audit trail
    id, source_type, records_collected, status, error_message, timestamp
```

---

## 📊 Output Files

| File | Description |
|------|-------------|
| `market_intelligence.db` | SQLite database with all collected data |
| `market_analysis.png` | 5 charts: price, rating, distribution, stock %, GitHub top repos |
| `analysis.html` | Standalone HTML report with insights and visualisations |
| `exports/*.csv` | All four tables exported as UTF-8 CSV |
| `pipeline.log` | Timestamped log of every pipeline event |

---

## 🔑 Key Findings (example — actual values depend on live data)

- **Highest-rated category**: determined at runtime from scraped data
- **Most expensive category**: determined at runtime from scraped data
- **GitHub**: top book-related repos are Python-based, confirming strong ecosystem interest
- **Library DB**: genre distribution shows fiction and history are most stocked

---

## 🏗️ Architecture

```
BookMarketIntelligence
│
├── collect_from_database()   → reads library.db, stores in library_books
├── collect_from_api()        → queries GitHub API, stores in github_repos
├── collect_from_web()        → scrapes 4 categories, stores in web_books
│
├── analyze_and_visualize()   → generates 5-panel PNG chart
├── generate_report()         → writes analysis.html
├── export_all_data()         → writes exports/*.csv
│
└── run()                     → orchestrates all steps above
```

Data validation is applied during web scraping:
- Prices must be valid positive floats
- Ratings must be integers 1–5
- Titles must be non-empty
- Duplicates (title + category) are removed

---

## 📝 Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `requests` | ≥ 2.28 | HTTP for API + scraping |
| `beautifulsoup4` | ≥ 4.12 | HTML parsing |
| `lxml` | ≥ 4.9 | Fast HTML parser backend |
| `pandas` | ≥ 2.0 | Data manipulation + DB I/O |
| `matplotlib` | ≥ 3.7 | Visualisations |
| `numpy` | ≥ 1.24 | Numeric helpers |
| `openpyxl` | ≥ 3.1 | Excel export (optional) |
