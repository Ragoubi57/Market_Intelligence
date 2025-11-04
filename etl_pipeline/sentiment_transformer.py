from typing import List, Dict
import hashlib
import os
import json

# Try to import transformers, fallback to TextBlob if not available
try:
    from transformers import pipeline
    sentiment_pipeline = pipeline("sentiment-analysis", model="yiyanghkust/finbert-tone")
    USE_FINBERT = True
    print("[INFO] FinBERT model loaded successfully.")
except ImportError:
    from textblob import TextBlob
    sentiment_pipeline = None
    USE_FINBERT = False
    print("[WARN] Transformers not available. Falling back to TextBlob for sentiment analysis.")

# Cache directory for storing results
CACHE_DIR = "cache/sentiment"
os.makedirs(CACHE_DIR, exist_ok=True)

def hash_text(text: str) -> str:
    """Generate a hash for the given text."""
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

def save_to_cache(text_hash: str, result: Dict):
    """Save the sentiment result to the cache."""
    cache_path = os.path.join(CACHE_DIR, f"{text_hash}.json")
    with open(cache_path, "w") as f:
        json.dump(result, f)

def load_from_cache(text_hash: str) -> Dict:
    """Load the sentiment result from the cache if it exists."""
    cache_path = os.path.join(CACHE_DIR, f"{text_hash}.json")
    if os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            return json.load(f)
    return None

def sentiment_transformer(texts: List[str], batch_size: int = 16) -> List[Dict]:
    """
    Perform sentiment analysis on a list of texts using FinBERT or TextBlob fallback.

    Args:
        texts (List[str]): List of input texts.
        batch_size (int): Number of texts to process in a batch.

    Returns:
        List[Dict]: List of sentiment results with label and score.
    """
    results = []

    for text in texts:
        if not text or not text.strip():
            # Handle empty or null text
            results.append({"label": "Neutral", "score": 0.0, "polarity": 0.0})
            continue

        # Check cache first
        text_hash = hash_text(text)
        cached_result = load_from_cache(text_hash)
        if cached_result:
            results.append(cached_result)
            continue

        # Perform sentiment analysis
        try:
            if USE_FINBERT and sentiment_pipeline:
                prediction = sentiment_pipeline(text[:512])[0]  # Truncate to 512 tokens
                label = prediction['label']
                score = prediction['score']

                # Map FinBERT labels to polarity
                polarity = 0.0
                if label == "Positive":
                    polarity = score
                elif label == "Negative":
                    polarity = -score

                result = {"label": label, "score": score, "polarity": polarity}
            else:
                # Fallback to TextBlob
                blob = TextBlob(text)
                polarity = blob.sentiment.polarity
                if polarity > 0.05:
                    label = "Positive"
                elif polarity < -0.05:
                    label = "Negative"
                else:
                    label = "Neutral"
                result = {"label": label, "score": abs(polarity), "polarity": polarity}
            
            results.append(result)

            # Save to cache
            save_to_cache(text_hash, result)
        except Exception as e:
            print(f"[ERROR] Sentiment analysis failed for text: {text[:50]}... Error: {e}")
            results.append({"label": "Neutral", "score": 0.0, "polarity": 0.0})

    return results