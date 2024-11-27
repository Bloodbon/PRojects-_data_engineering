import pandas as pd
import json
import requests
import datetime
from datetime import date
import uuid
import os
from google.cloud import storage

def upload_to_gcs(bucket_name, destination_blob_name, source_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def fetch_news_data():
    today = date.today()
    api_key = '3ef02d8a5fd14f1d9615bff33b1213e3'

    base_url = "https://newsapi.org/v2/everything?q={}&from={}&to={}&sortBy=popularity&apiKey={}&language=en"
    start_date_value = str(today - datetime.timedelta(days=1))
    end_date_value = str(today)

    df = pd.DataFrame(columns=['newsTitle', 'timestamp', 'url_source', 'content', 'source', 'author', 'urlToImage'])

    url_extractor = base_url.format("apple", start_date_value, end_date_value, api_key)
    response = requests.get(url_extractor)
    d = response.json()

    for i in d['articles']:
        newsTitle = i['title']
        timestamp = i['publishedAt']
        url_source = i['url']
        source = i['source']['name']
        author = i['author']
        urlToImage = i['urlToImage']
        partial_content = i['content'] if i['content'] is not None else ""
        
        if len(partial_content) >= 200:
            partial_content = partial_content[:199]
        if '.' in partial_content:
            trimmed_part = partial_content[:partial_content.rindex('.')]
        else:
            trimmed_part = partial_content

        new_row = pd.DataFrame({
            'newsTitle': [newsTitle],
            'timestamp': [timestamp],
            'url_source': [url_source],
            'content': [trimmed_part],
            'source': [source],
            'author': [author],
            'urlToImage': [urlToImage]
        })

        df = pd.concat([df, new_row], ignore_index=True)

    current_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f'run_{current_time}.parquet'
    print(df)
    
    # Check and print the current working directory
    print("Current Working Directory:", os.getcwd())

    # Write DataFrame to Parquet file
    df.to_parquet(filename)

    # Upload to GCS
    bucket_name = 'snowflake_projects'
    destination_blob_name = f'news_data_analysis/parquet_files/{filename}'
    upload_to_gcs(bucket_name, destination_blob_name, filename)

    # Remove local file after upload
    os.remove(filename)
