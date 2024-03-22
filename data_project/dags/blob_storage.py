import pandas as pd
import json
from datetime import datetime
import os


def store_articles(articles_json):
    # articles_json is a JSON string containing the articles data

    # Load JSON string into a Python list of dictionaries
    articles_data = json.loads(articles_json)

    # Normalize JSON data to flatten it
    df = pd.json_normalize(articles_data)

    # Rename columns to more descriptive names
    df.rename(columns={
        'title': 'newstitle',
        'publishedAt': 'timestamp',
        'url': 'url_source',
        'content': 'content',
        'source_name': 'source',  
        'author': 'author',
        'urlToImage': 'urltoimage'
    }, inplace=True)

    # Convert 'timestamp' to datetime and split into separate date and time columns
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date'] = df['timestamp'].dt.date
    df['time'] = df['timestamp'].dt.time
    df.drop(columns=["timestamp"], inplace=True)

    # Generate a filename using the current date and time
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{formatted_datetime}.parquet"

    # Check if the 'blob_storage' directory exists, and create it if it does not
    blob_folder = "/home/michelle/Analytics/data_project/blob_storage"
    if not os.path.exists(blob_folder):
        os.makedirs(blob_folder)


    # Save the DataFrame to a Parquet file in the 'blob_storage' directory
    filepath = os.path.join(blob_folder, filename)
    try:
        df.to_parquet(filepath, engine='pyarrow')
        print(f"DataFrame saved as {filepath}")
    except Exception as e:
        print(f"Error saving DataFrame: {e}")


    return(filepath)


# from extract_json import fetch_articles
# from config import API_KEY
# articles_json = fetch_articles(API_KEY)
# store_articles(articles_json)