# Market Intelligence Platform

Data engineering platform integrating e-commerce sales, market data, and news sentiment into a unified data warehouse. Built with PySpark, implements ETL pipelines, sentiment analysis, and star schema design for analytics and forecasting.

## Overview

This platform combines historical e-commerce data with real-time market intelligence to enable comprehensive business analytics. The system extracts data from multiple sources, applies transformations including sentiment analysis, and loads into a cloud data warehouse following dimensional modeling best practices.

**Key Capabilities:**
- Multi-source ETL pipelines (CSV files, REST APIs)
- Distributed processing with PySpark
- Real-time news sentiment analysis
- Star schema data warehouse
- Scalable cloud architecture

## Tech Stack

**Core:**
- Python 3.10
- PySpark (distributed processing)
- Pandas (data manipulation)
- PostgreSQL (Supabase)

**APIs & Integration:**
- Alpha Vantage (stock market data)
- NewsAPI (news articles)
- Supabase client (database operations)

**ML/Analytics:**
- TextBlob (sentiment analysis)
- NumPy (numerical operations)

## Architecture

### Data Sources
- **E-Commerce Sales:** Kaggle public dataset ([source](https://www.kaggle.com/datasets/aniltomar/e-commerce-sales-dataset))
- **Stock Market:** Alpha Vantage API (OHLCV data)
- **News Articles:** NewsAPI (business news with sentiment)
- **Financial Metrics:** CSV files (P&L, expenses, cloud costs)

### ETL Pipeline

**Extract:**
- CSV file parsing with Pandas
- REST API calls with error handling and rate limiting
- Concurrent data fetching for multiple sources

**Transform:**
- Data standardization and cleaning
- Star schema generation (dimension and fact tables)
- Sentiment analysis on news text
- Date synchronization across sources
- PySpark for distributed transformations

**Load:**
- Batch inserts with configurable chunk size
- Upsert operations for dimension tables
- Foreign key relationship management
- Data validation and type conversion

## Setup

### Prerequisites
- Python 3.10
- Conda
- Supabase account
- API keys (Alpha Vantage, NewsAPI)

### Installation

1. Clone repository:
```bash
git clone https://github.com/Ragoubi57/Market_Intelligence.git
cd Market_Intelligence
```

2. Create environment:
```bash
conda create -n market310 python=3.10
conda activate market310
pip install pandas numpy pyspark textblob supabase python-dotenv requests
```

3. Download data:
- Get Kaggle dataset from [here](https://www.kaggle.com/datasets/aniltomar/e-commerce-sales-dataset)
- Place CSV files in `Data/` directory

4. Configure environment:
Create `.env` file:
```
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_key
NEWS_API_KEY=your_news_api_key
```

### Usage

Run ETL pipeline:
```bash
conda activate market310
python main.py
```

Verify data:
```bash
python verify_data.py
```

## Database Schema

### Dimension Tables
- `dim_date` - Calendar dimension with year, month, day, quarter
- `dim_product` - Product catalog with SKU, name, category
- `dim_region` - Geographic regions with country, currency
- `dim_channel` - Sales channels (Amazon, International)

### Fact Tables
- `fact_sales` - E-commerce transactions (940K+ records)
- `fact_expense` - Business expenses linked to dates
- `fact_cloud_cost` - Cloud infrastructure costs
- `fact_pandl` - Profit & Loss statements
- `fact_stock_prices` - Stock market OHLCV data
- `fact_market_news` - News articles with sentiment scores

All fact tables link to dimensions via foreign keys. Auto-generated primary keys use identity columns.

## Project Structure

```
Market_Intelligence/
├── main.py                      # Pipeline orchestrator
├── verify_data.py               # Data validation
├── etl_pipeline/
│   ├── config.py                # Source configuration
│   ├── extract.py               # CSV extraction
│   ├── transform.py             # Data transformations
│   ├── load.py                  # Database loading
│   ├── market_extract.py        # API fetching
│   ├── market_transform.py      # Market data processing
│   └── market_load.py           # Market data loading
└── Data/                        # CSV files
```

## Current Status

**Implemented:**
- Sales ETL pipeline (CSV-based)
- Financial metrics pipelines (expenses, P&L, cloud costs)
- Market data pipeline (stocks + news)
- Sentiment analysis (TextBlob)
- Star schema with 330+ dates, 940K+ sales records
- 100 stock prices, 98 news articles

**Planned:**
- Multi-symbol stock support
- Advanced sentiment (FinBERT)
- Time series forecasting
- Interactive dashboard
- Real-time updates