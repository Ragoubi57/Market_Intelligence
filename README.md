# E-Commerce ETL & Analytics Pipeline

This project is a comprehensive, scalable ETL (Extract, Transform, Load) pipeline built with Python. It is designed to process a public e-commerce sales dataset, transform it into a clean analytical format, and load it into a cloud database for business intelligence and data science applications.

## Key Features

- **Modular ETL Architecture:** The pipeline is cleanly separated into Extract, Transform, and Load modules for maintainability and scalability.
- **Configuration-Driven:** New data sources can be added by simply updating a configuration file, requiring no changes to the core pipeline logic.
- **Cloud Database Backend:** Utilizes **Supabase** (PostgreSQL) for robust, scalable, and accessible data storage, leveraging its generous free tier.
- **Analytical Data Modeling:** Transforms raw, flat data into a powerful **Star Schema**, the industry standard for efficient data warehousing and analytics.
- **Automated & Robust:** The pipeline is designed to be run from a single command, with built-in data cleaning, error handling, and dependency management.

## Tech Stack

- **Language:** Python 3.11+
- **Data Manipulation:** Pandas
- **Database:** Supabase (PostgreSQL)
- **Environment:** Conda
- **Infrastructure:** `dotenv` for environment management

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

## Future Goals (Roadmap)

- [ ] **Phase 2: Big Data & APIs:** Integrate **PySpark** for large-scale transformations and connect to a **News API** for real-time market data.
- [ ] **Phase 3: AI & Data Science:** Implement **Transformer models** to generate embeddings from text data, storing them in Supabase with `pgvector` for semantic search and sentiment analysis.
- [ ] **Phase 4: Visualization:** Build an interactive dashboard with **Streamlit** to visualize sales trends and market insights.