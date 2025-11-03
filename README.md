# Market Intelligence Platform

Data engineering platform integrating e-commerce sales, stock market data, and news sentiment analysis into a unified data warehouse. Built with PySpark for distributed processing and designed for scalability.

## Overview

This platform demonstrates end-to-end data engineering capabilities:
- ETL pipelines for multiple data sources (CSV files and external APIs)
- Star schema data warehouse design in PostgreSQL
- Distributed data processing with PySpark
- Real-time sentiment analysis on financial news
- Automated incremental data loading

## Architecture

The system follows a modular ETL architecture with three main components:

**Extract Layer**
- CSV data ingestion for e-commerce transactions
- API integration for real-time market data (Alpha Vantage, NewsAPI)
- Error handling and data validation

**Transform Layer**
- Data normalization and schema mapping
- Star schema transformation (fact and dimension tables)
- Sentiment analysis using TextBlob
- Date synchronization across data sources

**Load Layer**
- Batch loading with upsert operations
- Foreign key relationship management
- Incremental updates to avoid full refreshes

## Tech Stack

**Core Technologies:**
- Python 3.10
- PySpark (distributed processing)
- Pandas (data manipulation)
- PostgreSQL via Supabase

**APIs:**
- Alpha Vantage (stock market data)
- NewsAPI (financial news)

**Infrastructure:**
- Conda environment management
- python-dotenv for configuration

## Data Sources

**Public Datasets:**
- E-Commerce Sales Dataset from Kaggle (https://www.kaggle.com/datasets/aniltomar/e-commerce-sales-dataset)
- Attribution: ANil

**Real-time APIs:**
- Alpha Vantage for stock market OHLCV data
- NewsAPI for financial news articles

## Database Schema

**Dimension Tables:**
- dim_date: Calendar dimension with year, month, quarter
- dim_product: Product catalog with SKU, category
- dim_region: Geographic regions and currency
- dim_channel: Sales channels

**Fact Tables:**
- fact_sales: E-commerce transactions (940K+ records)
- fact_expense: Business expenses
- fact_cloud_cost: Infrastructure costs
- fact_pandl: Profit and loss statements
- fact_stock_prices: Stock market OHLCV data
- fact_market_news: News articles with sentiment scores

## Setup

**Prerequisites:**
- Anaconda or Miniconda
- Python 3.10
- PostgreSQL database (Supabase account)

**Environment Setup:**
```bash
conda create -n market310 python=3.10
conda activate market310
pip install pandas numpy pyspark textblob supabase python-dotenv requests
```

**Configuration:**

Create `.env` file in project root:
```
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_service_role_key
ALPHA_VANTAGE_API_KEY=your_api_key
NEWS_API_KEY=your_api_key
```

**Data Setup:**
1. Download Kaggle dataset and place CSV files in `Data/` directory
2. Ensure filenames match those in `etl_pipeline/config.py`

## Usage

**Run specific pipeline:**
```bash
# Sales data
python main.py  # Uncomment run_sales_etl_pipeline()

# Market data (stocks + news)
python main.py  # Uncomment run_market_data_etl_pipeline()
```

**Verify loaded data:**
```bash
python verify_data.py
```

## Project Structure

```
Market_Intelligence/
├── main.py                      # Pipeline orchestrator
├── etl_pipeline/
│   ├── config.py               # Data source paths and mappings
│   ├── extract.py              # CSV data extraction
│   ├── transform.py            # Data transformations and star schema
│   ├── load.py                 # Database loading operations
│   ├── market_extract.py       # API data fetching
│   ├── market_transform.py     # Market data processing with PySpark
│   └── market_load.py          # Market data loading
├── Data/                        # CSV files (Kaggle dataset)
└── verify_data.py              # Data validation script
```

## Current Implementation

**Completed Features:**
- Multi-source ETL pipelines (CSV and API)
- Star schema data warehouse with proper foreign keys
- Sentiment analysis on financial news (TextBlob)
- PySpark integration for distributed processing
- Incremental date synchronization
- Batch loading optimization

**Data Loaded:**
- 940K+ e-commerce sales transactions
- 100 stock price records (AMZN)
- 98 news articles with sentiment analysis
- 330 date dimension records

**Planned Enhancements:**
- Multi-stock symbol support
- Advanced sentiment analysis (FinBERT)
- Time series forecasting
- Interactive dashboard (Streamlit)
- Real-time data updates