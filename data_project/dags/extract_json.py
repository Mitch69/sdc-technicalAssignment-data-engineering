import requests
from datetime import date, timedelta
import json
from airflow.decorators import task
from config import API_KEY
 


# Definition to fetch articles from the NewsAPI within the specified date range and query
def news_articles(api_key: str, from_date: str, to_date: str, query: str = '*'):

    base_url = 'https://newsapi.org/v2/everything'
    params = {
        'q': query,
        'from': from_date,
        'to': to_date,
        'sortBy': 'publishedAt',
        'apiKey': api_key,
        'language': 'en',
        'pageSize': 100
    }

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()['articles']
    else:
        print(f"Failed to fetch articles. Status code: {response.status_code}")
        return []


# Fetches the last week's articles using the NewsAPI and return to a json
def fetch_articles(api_key: str):

    today = date.today()
    last_week = today - timedelta(days=7)

    articles = news_articles(api_key=api_key, from_date=str(last_week), to_date=str(today))

    for article in articles:
        article['source_name'] = article['source']['name'] if article.get('source') else None
        del article['source']

    articles_json = json.dumps(articles, ensure_ascii=False, indent=4)
    
    

    return articles_json
    
