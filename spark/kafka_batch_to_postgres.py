from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, udf, concat_ws, trim, lower, to_date
from pyspark.sql.types import StructType, StringType, ArrayType, StructType, StructField
import os
import spacy

nlp = spacy.load("en_core_web_sm")

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

tweets_schema = (
    StructType()
    .add("tweet_content", StringType())
    .add("tweet_date", StringType())
)

def extract_entities(text):
    if text is None:
        return []
    doc = nlp(text)
    return [(ent.text, ent.label_) for ent in doc.ents]

entity_schema = ArrayType(StructType([
    StructField("entity", StringType(), True),
    StructField("type", StringType(), True)
]))

extract_entities_udf = udf(extract_entities, entity_schema)

spark = SparkSession.builder \
    .appName("news") \
    .getOrCreate()

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "news-topic") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

df2 = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "tweets-topic") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

news_df = df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = news_df.select(from_json(col("json_str"), news_schema).alias("data")).select("data.*")

tweets_df = df2.selectExpr("CAST(value AS STRING) as json_str")
parsed_df2 = tweets_df.select(from_json(col("json_str"), tweets_schema).alias("data")).select("data.*")

parsed_df = parsed_df.withColumn("title", trim(col("title")))
parsed_df = parsed_df.withColumn("description", trim(col("description")))
parsed_df = parsed_df.withColumn("content", trim(col("content")))

parsed_df = parsed_df.withColumn("full_text", concat_ws(" ", col("title"), col("description"), col('content')))

entities_df = parsed_df.withColumn("entities", extract_entities_udf(col("full_text")))
entities_df2 = parsed_df2.withColumn("entities", extract_entities_udf(col("tweet_content")))

flattened_entities_df = entities_df.select(
    to_date(col("publishedAt")).alias("published_date"),
    explode(col("entities")).alias("entity_struct")
).select(
    lower(col("entity_struct.entity")).alias("entity_text"),
    col("entity_struct.type").alias("entity_type"),
    col("published_date")
)

flattened_entities_df2 = entities_df2.select(
    to_date(col("tweet_date")).alias("published_date"),
    explode(col("entities")).alias("entity_struct")
).select(
    lower(col("entity_struct.entity")).alias("entity_text"),
    col("entity_struct.type").alias("entity_type"),
    col("published_date")
)

flattened_entities_merged = flattened_entities_df.union(flattened_entities_df2)

flattened_entities_merged.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", pg_table) \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

dates_df = entities_df.select(to_date(col("publishedAt")).alias("the_date"))

dates_df2 = entities_df2.select(to_date(col("tweet_date")).alias("the_date"))

merged_dates = dates_df.union(dates_df2).distinct()

try:
    merged_dates.write \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", "dates") \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
except Exception as e:
    if "duplicate key" not in str(e).lower():
        raise e

spark.stop()
