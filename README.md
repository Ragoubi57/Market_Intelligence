# Market Intelligence Platform

A comprehensive data engineering and analytics platform that integrates multiple data sources (e-commerce sales, market data, news) into a unified data warehouse, powered by ETL pipelines, sentiment analysis, and ready for AI-driven insights.

## 🎯 Project Vision

Build a unified data warehouse that can power analytics, AI-driven forecasting models, and an interactive business intelligence dashboard by combining:
- E-commerce sales data
- Real-time stock market data
- News sentiment analysis
- Financial metrics (P&L, expenses, cloud costs)

## ✨ Key Features

- **Multi-Source ETL Pipelines:** Modular architecture for CSV files and external APIs
- **PySpark Integration:** Scalable data processing framework ready for big data
- **Sentiment Analysis:** Real-time news analysis using TextBlob (ready for FinBERT)
- **Star Schema Design:** Optimized analytical data warehouse in Supabase (PostgreSQL)
- **API Integration:** Alpha Vantage (stocks) and NewsAPI (market news)
- **Cloud Database:** Supabase with proper foreign key relationships

## 🛠️ Tech Stack

- **Language:** Python 3.10
- **Data Processing:** Pandas, PySpark
- **AI/ML:** TextBlob (sentiment), ready for Transformers
- **Database:** Supabase (PostgreSQL)
- **APIs:** Alpha Vantage, NewsAPI
- **Environment:** Conda
- **Infrastructure:** dotenv, requests

## Dataset

This project uses the publicly available **E-Commerce Sales Dataset** from Kaggle.

- **Source:** [Kaggle E-Commerce Sales Dataset](https://www.kaggle.com/datasets/aniltomar/e-commerce-sales-dataset)
- **Creator:** ANil

A huge thank you to ANil for providing this comprehensive dataset for public use. The data provides a detailed look at sales across multiple channels and product categories.

## Project Architecture

The ETL process flows in three distinct stages:

1.  **Extract:** Raw data files (CSVs) are loaded into Pandas DataFrames.
2.  **Transform:**
    - The raw DataFrames are unified into a single, standardized format using mappings from `config.py`.
    - This unified data is then used to generate a series of clean DataFrames that conform to a star schema (e.g., `dim_date`, `dim_product`, `fact_sales`).
3.  **Load:**
    - The dimension tables are loaded first into Supabase using an `upsert` operation to prevent duplicates.
    - Their database-generated primary keys are fetched back.
    - These keys are mapped as foreign keys to the fact table.
    - The complete fact table is loaded into Supabase.

## How to Run

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd etl_project
    ```

2.  **Set up the Conda environment:**
    ```bash
    # Ensure you have conda installed
    conda create --name market_intel python=3.11
    conda activate market_intel
    pip install -r requirements.txt
    ```
    *(Note: You will need to create a `requirements.txt` file with `pip freeze > requirements.txt`)*

3.  **Download the Data:**
    - Go to the [Kaggle Dataset Page](https://www.kaggle.com/datasets/aniltomar/e-commerce-sales-dataset) and download the data.
    - Create a directory named `data` in the project root.
    - Unzip the contents and place the CSV files into the `data/` directory. Ensure the filenames match those in `etl_pipeline/config.py`.

4.  **Create your `.env` file:**
    - In the project root, create a file named `.env`.
    - Add your Supabase credentials:
      ```
      SUPABASE_URL="your_supabase_url_here"
      SUPABASE_KEY="your_supabase_service_role_key_here"
      ```

5.  **Run the pipeline:**
    ```bash
    python main.py
    ```

## 📊 Current Status

### ✅ Phase 1: Data Foundation - **COMPLETE**
- [x] Sales ETL pipeline (Amazon, International)
- [x] Expense tracking pipeline
- [x] Cloud cost monitoring pipeline
- [x] P&L (Profit & Loss) pipeline
- [x] **Market data pipeline** (Stock prices + News sentiment)
- [x] Star schema in Supabase with 330+ dates
- [x] 100 stock price records (AMZN)
- [x] 98 news articles with sentiment scores

See [PHASE1_COMPLETE.md](PHASE1_COMPLETE.md) for detailed accomplishments.

### 🚧 Phase 2: Intelligence Layer - **NEXT**
- [ ] Jupyter Notebooks for EDA
- [ ] FinBERT for financial sentiment
- [ ] Time series forecasting (Prophet, LSTM)
- [ ] Correlation analysis between news sentiment and stock prices

### 📅 Phase 3: Delivery Layer - **PLANNED**
- [ ] Streamlit dashboard
- [ ] FastAPI backend
- [ ] Real-time data updates
- [ ] Predictive analytics visualization

## 🗂️ Database Schema

### Dimension Tables
- `dim_date` - Calendar dimension (330 records)
- `dim_product` - Product catalog
- `dim_region` - Geographic regions
- `dim_channel` - Sales channels

### Fact Tables
- `fact_sales` - E-commerce transactions
- `fact_expense` - Business expenses
- `fact_cloud_cost` - Cloud infrastructure costs
- `fact_pandl` - Profit & Loss statements
- `fact_stock_prices` - Stock market OHLCV data ✨
- `fact_market_news` - News with sentiment analysis ✨

## 🚀 Quick Start

### Prerequisites
```bash
# Install Conda
# Create environment
conda create -n market310 python=3.10
conda activate market310
pip install pandas numpy pyspark textblob supabase python-dotenv requests
```

### Configuration
Create `.env` file in project root:
```env
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_key
NEWS_API_KEY=your_news_api_key
```

### Run Pipeline
```bash
# Activate environment
conda activate market310

# Run market data pipeline
python main.py

# Verify loaded data
python verify_data.py
```

## 📁 Project Structure

```
Market_Intelligence/
├── main.py                      # Pipeline orchestrator
├── etl_pipeline/
│   ├── config.py               # Data source configuration
│   ├── extract.py              # CSV extraction
│   ├── transform.py            # Data transformations
│   ├── load.py                 # Database operations
│   ├── market_extract.py       # API data fetching
│   ├── market_transform.py     # Market data processing
│   └── market_load.py          # Market data loading
├── Data/                        # CSV data files
├── verify_data.py              # Data verification
├── PHASE1_COMPLETE.md          # Phase 1 summary
└── README.md                   # This file
```