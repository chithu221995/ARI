import os
import requests
from dotenv import load_dotenv

load_dotenv()
NEWS_API_KEY = os.getenv("NEWS_API_KEY")

def fetch_news_for_ticker(ticker):
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": ticker,
        "apiKey": NEWS_API_KEY,
        "pageSize": 5,
        "sortBy": "publishedAt",
        "language": "en"
    }
    response = requests.get(url, params=params)
    articles = []
    if response.status_code == 200:
        data = response.json()
        for article in data.get("articles", []):
            articles.append({
                "title": article.get("title"),
                "url": article.get("url"),
                "published_at": article.get("publishedAt"),
                "source": article.get("source", {}).get("name")
            })
    return articles