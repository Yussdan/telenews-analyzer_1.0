from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf, col, current_timestamp, from_json, length, avg, count, 
    collect_set, window, collect_list
)
from pyspark.sql.types import (
    ArrayType, StructType, StructField, StringType, FloatType, 
    TimestampType, IntegerType
)
import spacy
import logging
import json
import os
from pymongo import MongoClient

from sentiment_analysis import SentimentAnalyzer
from topic_modeling import TopicModeler
from db_connection import connect_to_elasticsearch

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ELASTICSEARCH_NEWS_INDEX = os.getenv("ELASTICSEARCH_NEWS_INDEX", "telegram_news")
ELASTICSEARCH_METRICS_INDEX = os.getenv("ELASTICSEARCH_METRICS_INDEX", "telegram_metrics")
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "telegram_news")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telegram_news")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "20"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "30"))
CONNECTION_TIMEOUT = int(os.getenv("CONNECTION_TIMEOUT", "60"))

# Определение схемы сообщения для парсинга JSON из Kafka
message_schema = StructType([
    StructField("id", StringType()),
    StructField("text", StringType()),
    StructField("channel_name", StringType()),
    StructField("date", TimestampType()),
    StructField("views", IntegerType()),
    StructField("forwards", IntegerType()),
    StructField("has_media", StringType())
])

# Определение схем для UDF
sentiment_schema = StructType([
    StructField("label", StringType()),
    StructField("score", FloatType())
])

class NewsProcessor:
    def __init__(self):
        logger.info("Initializing News Processor")
        try:
            self.nlp = spacy.load("ru_core_news_sm")
            self.sentiment_analyzer = SentimentAnalyzer()
            self.topic_modeler = TopicModeler(self.nlp)
            logger.info("NLP components initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing NLP components: {e}")
            raise
        try:
            self.es = connect_to_elasticsearch(
                ELASTICSEARCH_HOST, ELASTICSEARCH_PORT,
                max_retries=MAX_RETRIES,
                retry_delay=RETRY_DELAY,
                timeout=CONNECTION_TIMEOUT
            )
            logger.info("Connected to Elasticsearch successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {e}")
            raise

        self._setup_elasticsearch_indices()


    def _setup_elasticsearch_indices(self):
        """Create Elasticsearch indices if they don't exist"""
        try:
            if not self.es.indices.exists(index=ELASTICSEARCH_NEWS_INDEX):
                self.es.indices.create(
                    index=ELASTICSEARCH_NEWS_INDEX,
                    body={
                        "settings": {
                            "number_of_shards": 1,
                            "number_of_replicas": 0,
                            "analysis": {
                                "analyzer": {
                                    "russian_analyzer": {
                                        "type": "custom",
                                        "tokenizer": "standard",
                                        "filter": ["lowercase", "russian_stop", "russian_stemmer"]
                                    }
                                },
                                "filter": {
                                    "russian_stop": {
                                        "type": "stop",
                                        "stopwords": "_russian_"
                                    },
                                    "russian_stemmer": {
                                        "type": "stemmer",
                                        "language": "russian"
                                    }
                                }
                            }
                        },
                        "mappings": {
                            "properties": {
                                "message_id": {"type": "integer"},
                                "channel_id": {"type": "keyword"},
                                "channel_name": {
                                    "type": "text",
                                    "fields": {
                                        "keyword": {"type": "keyword"}
                                    }
                                },
                                "date": {"type": "date"},
                                "text": {
                                    "type": "text",
                                    "analyzer": "russian_analyzer"
                                },
                                "entities": {
                                    "type": "nested",
                                    "properties": {
                                        "entity": {"type": "keyword"},
                                        "type": {"type": "keyword"}
                                    }
                                },
                                "sentiment": {
                                    "properties": {
                                        "label": {"type": "keyword"},
                                        "score": {"type": "float"}
                                    }
                                },
                                "topics": {
                                    "type": "keyword"
                                },
                                "has_media": {"type": "boolean"},
                                "views": {"type": "integer"},
                                "forwards": {"type": "integer"}
                            }
                        }
                    }
                )
                logger.info(f"Created Elasticsearch index: {ELASTICSEARCH_NEWS_INDEX}")

            if not self.es.indices.exists(index=ELASTICSEARCH_METRICS_INDEX):
                self.es.indices.create(
                    index=ELASTICSEARCH_METRICS_INDEX,
                    body={
                        "settings": {
                            "number_of_shards": 1,
                            "number_of_replicas": 0
                        },
                        "mappings": {
                            "properties": {
                                "channel_name": {"type": "keyword"},
                                "window_start": {"type": "date"},
                                "window_end": {"type": "date"},
                                "hourly_sentiment": {"type": "float"},
                                "message_count": {"type": "integer"},
                                "hourly_topics": {"type": "keyword"}
                            }
                        }
                    }
                )
                logger.info(f"Created Elasticsearch index: {ELASTICSEARCH_METRICS_INDEX}")
                
        except Exception as e:
            logger.error(f"Failed to create Elasticsearch indices: {e}")
            raise
        
    def create_udfs(self):
        """Создание UDF функций для Spark"""
        logger.info("Creating UDFs for sentiment analysis and topic extraction")
        
        analyzer = self.sentiment_analyzer
        modeler = self.topic_modeler
        @udf(sentiment_schema)
        def analyze_sentiment(text):
            try:
                if text:
                    result = analyzer.analyze(text)
                    return (result["label"], float(result["score"]))
                return ("neutral", 0.5)
            except Exception as e:
                logger.error(f"Error in sentiment analysis: {e}")
                return ("error", 0.0)

        @udf(ArrayType(StringType()))
        def extract_topics(text):
            try:
                if text:
                    return modeler.get_topics(text)
                return []
            except Exception as e:
                logger.error(f"Error in topic extraction: {e}")
                return []

        @udf(StringType())
        def extract_main_entity(text):
            try:
                if text:
                    entity = modeler.extract_main_entity(text)
                    return str(entity) if entity else None
                return None
            except Exception as e:
                logger.error(f"Error in entity extraction: {e}")
                return None

        return analyze_sentiment, extract_topics, extract_main_entity


def read_from_kafka(spark):
    logger.info(f"Setting up Kafka stream reader for topic: {KAFKA_TOPIC}")
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()


def create_spark_session():
    logger.info("Creating Spark session")
    return SparkSession.builder \
        .appName("NewsFeedProcessor") \
        .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0," +
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0")\
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()


def write_to_elasticsearch_custom(df, index_name, id_field=None):
    logger.info(f"Setting up Elasticsearch stream writer for index: {index_name}")

    def process_batch(batch_df, batch_id):
        try:
            if not batch_df.isEmpty():
                options = {
                    "es.resource": index_name,
                    "es.nodes": ELASTICSEARCH_HOST,
                    "es.port": ELASTICSEARCH_PORT,
                    "es.nodes.wan.only": "true"
                }
                if id_field:
                    options["es.mapping.id"] = id_field
                head_pd = batch_df.select("id", "text", "topics").limit(5).toPandas()
                logger.info(f"Sample batch for ES (first 5):\n{head_pd}")
                batch_df.write \
                    .format("org.elasticsearch.spark.sql") \
                    .options(**options) \
                    .mode("append") \
                    .save()
                logger.info(f"Wrote batch {batch_id} to Elasticsearch index {index_name}")
        except Exception as e:
            logger.error(f"Error writing to Elasticsearch: {e}")

    return df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", f"/tmp/checkpoint/es_{index_name.replace('/', '_')}") \
        .start()

def write_to_mongodb_custom(df, collection_name):
    logger.info(f"Setting up MongoDB stream writer for collection: {collection_name}")
    def process_batch(batch_df, batch_id):
        try:
            if not batch_df.isEmpty():
                rows = batch_df.toJSON().collect()
                documents = [json.loads(row) for row in rows]
                client = MongoClient(MONGODB_URI)
                db = client[MONGODB_DB]
                collection = db[collection_name]
                if documents:
                    for doc in documents:
                        if "window" in doc and "channel_name" in doc:
                            filter_criteria = {
                                "channel_name": doc["channel_name"],
                                "window.start": doc["window"]["start"],
                                "window.end": doc["window"]["end"]
                            }
                            collection.update_one(filter_criteria, {"$set": doc}, upsert=True)
                        elif "id" in doc:
                            collection.update_one({"id": doc["id"]}, {"$set": doc}, upsert=True)
                        else:
                            collection.insert_one(doc)
                    logger.info(f"Processed {len(documents)} documents for MongoDB collection {collection_name}")
        except Exception as e:
            logger.error(f"Error writing to MongoDB: {e}")
    
    return df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", f"/tmp/checkpoint/mongo_{collection_name}") \
        .start()

def process_data(df, news_processor):
    """Расширенная обработка данных"""
    logger.info("Processing incoming data stream")
    analyze_sentiment, extract_topics, extract_main_entity = news_processor.create_udfs()
    processed_df = df.select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), message_schema).alias("data")
    ).select("data.*")
    enriched_df = processed_df \
       .withColumn("sentiment", analyze_sentiment(col("text"))) \
        .withColumn("topics", extract_topics(col("text"))) \
        .withColumn("main_entity", extract_main_entity(col("text"))) \
        .withColumn("processing_time", current_timestamp()) \
        .withColumn("text_length", length(col("text"))) \
        .withColumn("has_media_int", col("has_media").cast("integer"))
    return enriched_df


def calculate_channel_metrics(df):
    logger.info("Calculating channel metrics")
    return df \
        .groupBy("channel_name") \
        .agg(
            avg("sentiment.score").alias("avg_sentiment"),
            count("*").alias("message_count"),
            avg("views").alias("avg_views"),
            avg("forwards").alias("avg_forwards"),
            collect_set("topics").alias("channel_topics"),
            collect_set("main_entity").alias("entities_of_channel")
        )

def main():
    logger.info("Starting News Processor")
    spark = create_spark_session()
    news_processor = NewsProcessor()

    try:
        kafka_df = read_from_kafka(spark)
        processed_df = process_data(kafka_df, news_processor)
        news_query = write_to_elasticsearch_custom(
            processed_df,
            ELASTICSEARCH_NEWS_INDEX,
            id_field="id"
        )
        windowed_df = processed_df \
            .withWatermark("date", "1 hour") \
            .groupBy(
                window(col("date"), "1 hour"),
                col("channel_name")
            )

        hourly_metrics = windowed_df.agg(
            avg("sentiment.score").alias("hourly_sentiment"),
            count("*").alias("message_count"),
            collect_list("topics").alias("hourly_topics"),
            collect_list("main_entity").alias("main_entities")
        )
        hourly_metrics_flat = hourly_metrics \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
        metrics_query = write_to_elasticsearch_custom(
            hourly_metrics_flat,
            ELASTICSEARCH_METRICS_INDEX
        )
        mongodb_query = write_to_mongodb_custom(hourly_metrics, "hourly_analytics")

        def update_topic_model(batch_df, batch_id):
            try:
                if batch_id % 100 == 0 and not batch_df.isEmpty():
                    sample_size = 1000
                    sampled_data = batch_df.limit(sample_size)
                    text_samples = [row.text for row in sampled_data.select("text").collect() if row.text]
                    if text_samples:
                        news_processor.topic_modeler.update_model(text_samples)
                        logger.info(f"Updated topic model with {len(text_samples)} samples")
            except Exception as e:
                logger.error(f"Error updating topic model: {e}")

        update_query = processed_df.writeStream \
            .foreachBatch(update_topic_model) \
            .option("checkpointLocation", "/tmp/checkpoint/model_updates") \
            .start()

        logger.info("All streaming queries started, waiting for termination")
        news_query.awaitTermination()
        metrics_query.awaitTermination()
        mongodb_query.awaitTermination()
        update_query.awaitTermination()
    except Exception as e:
        logger.error(f"Error in main processing: {e}")
    finally:
        logger.info("Stopping Spark session")
        spark.stop()

if __name__ == "__main__":
    main()
