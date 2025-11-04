# etl_pipeline/market_extract.py

import os
import requests
from concurrent.futures import ThreadPoolExecutor

def fetch_stock_data(symbol: str, outputsize: str = "compact") -> list[dict]:
    """Fetches daily time series data for a given stock symbol with adjustable output size."""
    print(f"  - Fetching daily stock data for symbol: {symbol} (outputsize={outputsize})...")
    ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not ALPHA_VANTAGE_API_KEY:
        print("    [ERROR] ALPHA_VANTAGE_API_KEY not found.")
        return []

    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize={outputsize}&apikey={ALPHA_VANTAGE_API_KEY}'
    
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

def fetch_multiple_stocks(symbols: list[str], outputsize: str = "compact") -> list[dict]:
    """Fetches stock data for multiple symbols concurrently with adjustable output size."""
    print(f"  - Starting concurrent fetch for {len(symbols)} symbols (outputsize={outputsize})...")
    all_data = []
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = executor.map(lambda symbol: fetch_stock_data(symbol, outputsize), symbols)
    
    for result in results:
        all_data.extend(result)
    
    print(f"  - Total records fetched: {len(all_data)}")
    return all_data

def fetch_news_data(query: str, from_date: str = None, to_date: str = None, page_size: int = 100) -> list[dict]:
    """
    Fetches news articles for a given query with optional date range.
    
    Args:
        query: Search query string
        from_date: Start date in format YYYY-MM-DD (default: 30 days ago)
        to_date: End date in format YYYY-MM-DD (default: today)
        page_size: Number of articles to fetch (max 100 per request)
    """
    print(f"  - Fetching news data for query: '{query}'...")
    NEWS_API_KEY = os.getenv("NEWS_API_KEY")
    if not NEWS_API_KEY:
        print("    [ERROR] NEWS_API_KEY not found.")
        return []
    
    # Build URL with parameters
    params = {
        "q": query,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": min(page_size, 100),  # API max is 100
        "apiKey": NEWS_API_KEY
    }
    
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date
    
    url = "https://newsapi.org/v2/everything"
    
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        articles = data.get("articles", [])
        
        records = []
        seen_titles = set()  # For deduplication
        
        for article in articles:
            title = article.get("title", "").strip()
            
            # Skip if no title or duplicate
            if not title or title in seen_titles:
                continue
            
            seen_titles.add(title)
            
            records.append({
                "published_at": article.get("publishedAt"),
                "source_name": article.get("source", {}).get("name"),
                "title": title,
                "description": article.get("description", ""),
                "content": article.get("content", ""),
                "url": article.get("url", "")
            })
        
        print(f"    - Fetched {len(records)} unique articles for '{query}'.")
        return records
    except requests.exceptions.RequestException as e:
        print(f"    [ERROR] Request failed for '{query}': {e}")
        return []

def fetch_news_for_symbols(symbols: list[str], from_date: str = None, to_date: str = None) -> list[dict]:
    """
    Fetches news for multiple stock symbols and general market news.
    Deduplicates across all queries.
    """
    print(f"  - Fetching news for {len(symbols)} symbols plus general market news...")
    
    all_articles = []
    seen_titles = set()
    
    # Fetch symbol-specific news
    for symbol in symbols:
        query = f"{symbol} OR stock"
        articles = fetch_news_data(query, from_date, to_date, page_size=20)
        
        # Deduplicate
        for article in articles:
            title = article.get("title", "")
            if title and title not in seen_titles:
                seen_titles.add(title)
                all_articles.append(article)
    
    # Fetch general market/tech news
    general_query = "technology OR market OR economy OR stock market"
    general_articles = fetch_news_data(general_query, from_date, to_date, page_size=50)
    
    for article in general_articles:
        title = article.get("title", "")
        if title and title not in seen_titles:
            seen_titles.add(title)
            all_articles.append(article)
    
    print(f"  - Total unique articles fetched: {len(all_articles)}")
    return all_articles