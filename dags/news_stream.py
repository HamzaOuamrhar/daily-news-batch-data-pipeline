from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import requests
import os
import json
from dotenv import load_dotenv
from confluent_kafka import Producer
from airflow.operators.bash import BashOperator
import pandas as pd
import psycopg2

default_args = {
    'owner': 'hamza',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_news():
    print("fetch news started")

    api_key = os.environ['API_KEY']
    print(api_key)

    if not api_key:
        raise ValueError("API_KEY not set")

    url = "https://newsapi.org/v2/top-headlines"
    params = {
        'country': 'us',
        'apiKey': api_key,
        'pageSize': 100,
        # 'category': 'business'
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    news_data = response.json()

    output_path = 'logs/news.json'

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(news_data, f, indent=4)
    print(f"Saved news data to {output_path}")


def create_dataset():
    pg_user = os.environ['PG_USER'] 
    pg_password = os.environ['PG_PASSWORD']
    pg_database = os.environ['PG_DATABASE']
    pg_url = "postgresql://" + pg_user + ":" + pg_password + "@" + "postgres:5432/" + pg_database
    
    output_csv = "/opt/airflow/logs/entity_daily_counts.csv"

    conn = psycopg2.connect(pg_url)
    query = r"""
        WITH entities AS (
            SELECT DISTINCT entity_text FROM trending_entities WHERE entity_type in ('EVENT', 'FAC', 'GPE', 'LOC', 'ORG', 'PERSON', 'PRODUCT', 'WORK_OF_ART')
        ),
        date_range AS (
            SELECT the_date::date AS published_date
            FROM dates
            WHERE the_date >= '2025-07-20'
        ),
        grid AS (
            SELECT e.entity_text, d.published_date
            FROM entities e CROSS JOIN date_range d
        ),
        counts AS (
            SELECT entity_text, DATE(published_date) AS published_date, COUNT(*) AS count_mentions
            FROM trending_entities
            WHERE published_date >= '2025-07-20'
            AND entity_type in ('EVENT', 'FAC', 'GPE', 'LOC', 'ORG', 'PERSON', 'PRODUCT', 'WORK_OF_ART')
            GROUP BY entity_text, DATE(published_date)
        )
        SELECT 
            g.entity_text,
            g.published_date,
            COALESCE(c.count_mentions, 0) AS count_mentions
        FROM grid g
        LEFT JOIN counts c
            ON g.entity_text = c.entity_text AND g.published_date = c.published_date
        ORDER BY g.entity_text, g.published_date
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    df.to_csv(output_csv, index=False)


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

    task_spark_batch = BashOperator(
        task_id='spark_kafka_to_postgres',
        bash_command='docker exec spark-master spark-submit --packages org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.5.1 /opt/spark-apps/kafka_batch_to_postgres.py'
    )

    task_create_dataset = PythonOperator(
        task_id='create_dataset',
        python_callable=create_dataset
    )

    task_fetch_news >> task_produce_news >> task_spark_batch >> task_create_dataset
