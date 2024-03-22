import pandas as pd
import os


def transform_data(filepath):
    df = pd.read_parquet(filepath)
    print(df)

    # Date Dimension table
    date_dim = df[['date']].drop_duplicates()
    date_dim['year'] = pd.to_datetime(date_dim['date']).dt.year
    date_dim['month'] = pd.to_datetime(date_dim['date']).dt.month
    date_dim['day'] = pd.to_datetime(date_dim['date']).dt.day


    # Author Dimension
    author_dim = df[['author']].drop_duplicates()
    author_dim['author_id'] = range(1, len(author_dim) + 1)
    #print(author_dim)


    # Source Dimension
    source_dim = df[['source']].drop_duplicates()
    source_dim['source_id'] = range(1, len(source_dim) + 1)
    #print(source_dim)


    # Fact Table
    fact_table = df.copy()
    # Add an 'article_id' as a unique identifier for each article
    fact_table['article_id'] = range(1, len(fact_table) + 1)


    # Create a dictionary of the tables
    news_articles_dict= {
        'date_dim': date_dim,
        'author_dim': author_dim,
        'source_dim': source_dim,
        'fact_table': fact_table
    }

    #print(news_articles_dict)
    
    return news_articles_dict



# from extract_json import fetch_articles
# from blob_storage import store_articles
# filepath = "/home/michelle/Analytics/data_project/blob_storage/2024-03-22_09-48-44.parquet"
# transform_data(filepath)