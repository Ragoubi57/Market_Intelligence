import os
import requests
from dotenv import load_dotenv

load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

def test_news_api():
    print("--- Testing NewsAPI ---")
    # Search for recent news about ecommerce
    url = f"https://newsapi.org/v2/everything?q=ecommerce&apiKey={NEWS_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(f"Successfully fetched {len(data.get('articles', []))} articles.")
        # Print the title of the first article
        if data.get('articles'):
            print(f"First article title: {data['articles'][0]['title']}")
    else:
        print(f"Error fetching from NewsAPI: {response.status_code}")
        print(response.text)

def test_alpha_vantage():
    print("\n--- Testing Alpha Vantage ---")
    # Get daily stock data for Amazon (AMZN)
    symbol = "AMZN"
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={ALPHA_VANTAGE_API_KEY}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if "Time Series (Daily)" in data:
            print(f"Successfully fetched daily data for {symbol}.")
            # Get the most recent day's data
            latest_date = list(data["Time Series (Daily)"].keys())[0]
            latest_data = data["Time Series (Daily)"][latest_date]
            print(f"Latest data for {latest_date}: {latest_data}")
        else:
            print("Error in Alpha Vantage response:")
            print(data) # This will show you the error message, e.g., "Invalid API call"
    else:
        print(f"Error fetching from Alpha Vantage: {response.status_code}")

if __name__ == "__main__":
    test_news_api()
    test_alpha_vantage()