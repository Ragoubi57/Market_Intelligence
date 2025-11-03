# etl_pipeline/market_extract.py

import os
import requests
from concurrent.futures import ThreadPoolExecutor

def fetch_stock_data(symbol: str) -> list[dict]:
    """Fetches daily time series data for a given stock symbol."""
    print(f"  - Fetching daily stock data for symbol: {symbol}...")
    ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not ALPHA_VANTAGE_API_KEY:
        print("    [ERROR] ALPHA_VANTAGE_API_KEY not found.")
        return []

    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={ALPHA_VANTAGE_API_KEY}'
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()

        if "Time Series (Daily)" not in data:
            print(f"    [WARN] Could not find time series data for {symbol}. Response: {data}")
            return []

        records = []
        for date_str, values in data["Time Series (Daily)"].items():
            records.append({
                "date": date_str,
                "symbol": symbol,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"])
            })
        print(f"    - Fetched {len(records)} daily records for {symbol}.")
        return records
    except requests.exceptions.RequestException as e:
        print(f"    [ERROR] Request failed for {symbol}: {e}")
        return []

def fetch_multiple_stocks(symbols: list[str]) -> list[dict]:
    """Fetches stock data for multiple symbols concurrently."""
    print(f"  - Starting concurrent fetch for {len(symbols)} symbols...")
    all_data = []
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = executor.map(fetch_stock_data, symbols)
    
    for result in results:
        all_data.extend(result)
    
    print(f"  - Total records fetched: {len(all_data)}")
    return all_data

def fetch_news_data(query: str) -> list[dict]:
    """Fetches news articles for a given query."""
    print(f"  - Fetching news data for query: '{query}'...")
    NEWS_API_KEY = os.getenv("NEWS_API_KEY")
    if not NEWS_API_KEY:
        print("    [ERROR] NEWS_API_KEY not found.")
        return []
    
    url = f"https://newsapi.org/v2/everything?q={query}&language=en&sortBy=publishedAt&apiKey={NEWS_API_KEY}"
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        articles = data.get("articles", [])
        
        records = []
        for article in articles:
            if article.get("title"):
                records.append({
                    "published_at": article.get("publishedAt"),
                    "source_name": article.get("source", {}).get("name"),
                    "title": article.get("title")
                })
        print(f"    - Fetched {len(records)} articles for '{query}'.")
        return records
    except requests.exceptions.RequestException as e:
        print(f"    [ERROR] Request failed for '{query}': {e}")
        return []