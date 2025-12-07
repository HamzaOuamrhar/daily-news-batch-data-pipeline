from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, udf, concat_ws, trim, lower, to_date
from pyspark.sql.types import StructType, StringType, ArrayType, StructType, StructField
import os
import spacy
import json
from pathlib import Path

nlp = spacy.load("en_core_web_sm")

OFFSET_FILE_PATH = "/opt/spark-apps/kafka_offsets.json"

def load_offsets():
    if Path(OFFSET_FILE_PATH).exists():
        try:
            with open(OFFSET_FILE_PATH, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            pass
    return {
        "news-topic": {"0": 0},
        "tweets-topic": {"0": 0}
    }

def save_offsets(offsets_dict):
    os.makedirs(os.path.dirname(OFFSET_FILE_PATH), exist_ok=True)
    with open(OFFSET_FILE_PATH, 'w') as f:
        json.dump(offsets_dict, f, indent=2)

def extract_final_offsets(df):
    try:
        offset_df = df.select("topic", "partition", "offset")
        offsets_collected = offset_df.collect()
        
        final_offsets = {}
        for row in offsets_collected:
            topic = row["topic"]
            partition = str(row["partition"])
            offset = row["offset"] + 1
            
            if topic not in final_offsets:
                final_offsets[topic] = {}
            final_offsets[topic][partition] = offset
        
        return final_offsets
    except Exception as e:
        print(f"Error extracting offsets: {e}")
        return None

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
    .master('spark://spark-master:7077') \
    .getOrCreate()

saved_offsets = load_offsets()

print(f"Loaded offsets: {saved_offsets}")

news_starting_offsets = json.dumps({"news-topic": saved_offsets.get("news-topic", {"0": 0})})
tweets_starting_offsets = json.dumps({"tweets-topic": saved_offsets.get("tweets-topic", {"0": 0})})

print(f"News topic starting from: {news_starting_offsets}")
print(f"Tweets topic starting from: {tweets_starting_offsets}")

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "news-topic") \
    .option("startingOffsets", news_starting_offsets) \
    .option("endingOffsets", "latest") \
    .load()

df2 = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "tweets-topic") \
    .option("startingOffsets", tweets_starting_offsets) \
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

final_offsets = {}

try:
    if df.count() > 0:
        news_offsets = extract_final_offsets(df)
        if news_offsets:
            final_offsets.update(news_offsets)
            print(f"Extracted news offsets: {news_offsets}")
    else:
        print("No new messages in news-topic")
except Exception as e:
    print(f"Error processing news topic offsets: {e}")

try:
    if df2.count() > 0:
        tweets_offsets = extract_final_offsets(df2)
        if tweets_offsets:
            final_offsets.update(tweets_offsets)
            print(f"Extracted tweets offsets: {tweets_offsets}")
    else:
        print("No new messages in tweets-topic")
except Exception as e:
    print(f"Error processing tweets topic offsets: {e}")

if final_offsets:
    try:
        current_offsets = load_offsets()
        current_offsets.update(final_offsets)
        save_offsets(current_offsets)
        print(f"Successfully saved offsets: {current_offsets}")
    except Exception as e:
        print(f"Error saving offsets: {e}")
else:
    print("No new offsets to save")

spark.stop()
