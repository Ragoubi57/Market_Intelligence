"""Quick test to verify FinBERT sentiment analysis is working."""

from etl_pipeline.sentiment_transformer import sentiment_transformer

# Test with sample financial news headlines
test_headlines = [
    "Stock market hits record high as tech companies surge",
    "Company reports massive losses in quarterly earnings",
    "Federal Reserve maintains interest rates, markets remain stable",
    "Major acquisition deal announced, investors react positively",
    "Recession fears grow as unemployment rises"
]

print("Testing FinBERT sentiment analysis...")
print("=" * 70)

results = sentiment_transformer(test_headlines)

for headline, result in zip(test_headlines, results):
    print(f"\nHeadline: {headline}")
    print(f"Label: {result['label']}")
    print(f"Score: {result['score']:.4f}")
    print(f"Polarity: {result['polarity']:.4f}")

print("\n" + "=" * 70)
print("Test complete! If you see varied sentiment labels and scores above,")
print("FinBERT is working correctly.")
