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

news_df = df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = news_df.select(from_json(col("json_str"), news_schema).alias("data")).select("data.*")

parsed_df = parsed_df.withColumn("title", trim(col("title")))
parsed_df = parsed_df.withColumn("description", trim(col("description")))

parsed_df = parsed_df.withColumn("full_text", concat_ws(" ", col("title"), col("description")))

entities_df = parsed_df.withColumn("entities", extract_entities_udf(col("full_text")))

flattened_entities_df = entities_df.select(
    to_date(col("publishedAt")).alias("published_date"),
    explode(col("entities")).alias("entity_struct")
).select(
    lower(col("entity_struct.entity")).alias("entity_text"),
    col("entity_struct.type").alias("entity_type"),
    col("published_date")
)

flattened_entities_df.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", pg_table) \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

spark.stop()
