from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
import os


pg_url = os.environ['PG_URL']
pg_user = os.environ['PG_USER']
pg_password = os.environ['PG_PASSWORD']
pg_table = os.environ['PG_TABLE']

news_schema = (
    StructType()
    .add("title", StringType())
    .add("description", StringType())
    .add("url", StringType())
    .add("publishedAt", StringType())
    .add("content", StringType())
)

spark = SparkSession.builder \
    .appName("KafkaNewsBatchToPostgres") \
    .getOrCreate()

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "news-topic") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

news_df = df.selectExpr("CAST(value AS STRING) as json_str")

parsed_df = news_df.select(from_json(col("json_str"), news_schema).alias("data")).select("data.*")

parsed_df.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", pg_table) \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

spark.stop()
