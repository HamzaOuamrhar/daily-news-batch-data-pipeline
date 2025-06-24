from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
import json
from dotenv import load_dotenv
from confluent_kafka import Producer

default_args = {
    'owner': 'hamza',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_news():
    env_path = 'config/news.env'
    print("fetch news started")
    if not os.path.exists(env_path):
        raise FileNotFoundError(f"{env_path} does not exist")
    load_dotenv(env_path)

    api_key = os.getenv('API_KEY')
    print(api_key)

    if not api_key:
        raise ValueError("API_KEY not set")

    url = "https://newsapi.org/v2/top-headlines"
    params = {
        'country': 'us',
        'apiKey': api_key,
        'pageSize': 100,
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    news_data = response.json()

    output_path = 'logs/news.json'

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(news_data, f, indent=4)
    print(f"Saved news data to {output_path}")



def produce_to_kafka():
    kafka_conf = {
        'bootstrap.servers': 'kafka:9092'
    }
    producer = Producer(kafka_conf)
    topic = 'news-topic'

    with open('logs/news.json') as f:
        data = json.load(f)

    for article in data.get("articles", []):
        producer.produce(topic, json.dumps(article).encode('utf-8'))
        producer.poll(0)
    producer.flush()

with DAG(
    'fetch-and-produce-news',
    default_args=default_args,
    description='Fetch news from NewsAPI and save to file then produce to kafka',
    schedule='@daily',
    start_date=datetime(2025, 6, 23),
    catchup=False,
) as dag:

    task_fetch_news = PythonOperator(
        task_id='fetch_news',
        python_callable=fetch_news
    )

    task_produce_news = PythonOperator(
        task_id='produce_to_kafka',
        python_callable=produce_to_kafka
    )

    task_fetch_news >> task_produce_news
