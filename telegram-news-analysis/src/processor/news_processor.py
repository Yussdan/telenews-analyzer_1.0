from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    udf, col, from_json, avg, count,
    collect_set, window,
    explode_outer, filter as spark_filter, size
)
from pyspark.sql.types import (
    ArrayType, StructType, StructField, StringType, FloatType,
    TimestampType, IntegerType, BooleanType
)
import spacy
import logging
import json
import os
from pymongo import MongoClient, errors as pymongo_errors
import time
import requests
import traceback # Import traceback for better error logging
from datetime import datetime # For mention timestamp
import pandas as pd

# Импортируем наш новый SentimentAnalyzer и TopicModeler
from sentiment_analysis import SentimentAnalyzer, EntityMentionAnalyzer
from topic_modeling import TopicModeler
from db_connection import connect_to_elasticsearch, connect_to_mongodb # Assuming connect_to_mongodb is also in db_connection

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables (consider using a config management library for larger projects)
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
LOCAL_LLM_URL = os.getenv("LOCAL_LLM_URL", "http://llm-service:8000/generate")
USE_LOCAL_LLM = os.getenv("USE_LOCAL_LLM", "true").lower() == "true"
POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://admin:password@postgres:5432/telenews")
DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")
EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "8g")
CHECKPOINT_BASE_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")
SENTIMENT_CACHE_LIMIT = int(os.getenv("SENTIMENT_CACHE_LIMIT", "50000"))
TOPIC_CACHE_LIMIT = int(os.getenv("TOPIC_CACHE_LIMIT", "10000"))
MODEL_UPDATE_FREQUENCY_BATCHES = int(os.getenv("MODEL_UPDATE_FREQUENCY_BATCHES", "60"))


# Определение схемы сообщения для парсинга JSON из Kafka
message_schema = StructType([
    StructField("id", StringType(), False),
    StructField("text", StringType(), True),
    StructField("channel_id", StringType(), True),
    StructField("channel_name", StringType(), True),
    StructField("date", TimestampType(), True),
    StructField("views", IntegerType(), True),
    StructField("forwards", IntegerType(), True),
    StructField("has_media", StringType(), True)
])

# Определение расширенной схемы для UDF с учетом локальной LLM
sentiment_schema = StructType([
    StructField("label", StringType(), True), # Allow nulls for error cases
    StructField("score", FloatType(), True),
    StructField("method", StringType(), True),
    StructField("explanation", StringType(), True)
])

# Расширенная схема для тем с учетом нового TopicModeler
topic_schema = StructType([
    StructField("label", StringType(), True),
    StructField("keywords", ArrayType(StringType(), True), True),
    StructField("entities", ArrayType(StringType(), True), True),
    StructField("similarity", FloatType(), True)
])

topics_array_schema = ArrayType(topic_schema, True)

# Схема для сущностей (главная сущность)
entity_schema = StructType([
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("method", StringType(), True)
])

# Схема для упоминаний сущностей (возвращаемая analysis function, используется в ES)
entity_mention_schema = StructType([
    StructField("entity_id", StringType(), True),
    StructField("entity_name", StringType(), True),
    StructField("sentiment", sentiment_schema, True) 
])

entity_mentions_array_schema = ArrayType(entity_mention_schema, True)


final_output_schema = StructType([
    StructField("id", StringType(), False),
    StructField("channel_id", StringType(), True),
    StructField("channel_name", StringType(), True),
    StructField("date", TimestampType(), True),
    StructField("text", StringType(), True),
    StructField("views", IntegerType(), True),
    StructField("forwards", IntegerType(), True),
    StructField("has_media", BooleanType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("processing_time", TimestampType(), False),
    StructField("text_length", IntegerType(), True),
    StructField("sentiment", sentiment_schema, True),
    StructField("topics", topics_array_schema, True),
    StructField("main_entity", entity_schema, True),
    StructField("entity_mentions", entity_mentions_array_schema, True)
])


intermediate_map_output_schema = StructType([
    StructField("id", StringType(), False),
    StructField("channel_id", StringType(), True),
    StructField("channel_name", StringType(), True),
    StructField("date", TimestampType(), True),
    StructField("text", StringType(), True),
    StructField("views", IntegerType(), True),
    StructField("forwards", IntegerType(), True),
    StructField("has_media", StringType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("processing_time", TimestampType(), False),
    StructField("text_length", IntegerType(), True),
    StructField("sentiment_json", StringType(), True),
    StructField("topics_json", StringType(), True),
    StructField("main_entity_json", StringType(), True),
    StructField("entity_mentions_json", StringType(), True)
])



nlp_model = None
sentiment_analyzer = None
topic_modeler = None
entity_analyzer = None
llm_available = False


def load_spacy_model(model_name="ru_core_news_sm"):
    """Loads or downloads the spaCy model."""
    global nlp_model
    if nlp_model is None:
        try:
            logger.info(f"Attempting to load spaCy model {model_name}...")
            nlp_model = spacy.load(model_name)
            logger.info(f"spaCy model {model_name} loaded.")
        except OSError:
            logger.warning(f"spaCy model {model_name} not found. Downloading...")
            try:
                spacy.cli.download(model_name)
                nlp_model = spacy.load(model_name)
                logger.info(f"spaCy model {model_name} downloaded and loaded.")
            except Exception as e:
                logger.error(f"Failed to download or load spaCy model {model_name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error loading spaCy model {model_name}: {e}")
    return nlp_model

def initialize_analysis_components(config):
    """Initializes analysis components, intended to run on workers."""
    global sentiment_analyzer, topic_modeler, entity_analyzer, llm_available, nlp_model

    logger.info("Worker: Initializing analysis components...")

    current_llm_available = config.get('llm_available', False) # Get availability from driver check
    use_local_llm_config = config.get('use_local_llm', False)

    nlp_instance = load_spacy_model()

    #Sentiment Analyzer
    try:
        if sentiment_analyzer is None:
            sentiment_analyzer = SentimentAnalyzer(
                use_local_llm=use_local_llm_config and current_llm_available,
                local_llm_url=config.get('local_llm_url')
            )
            logger.info(f"Worker: SentimentAnalyzer initialized. LLM active: {sentiment_analyzer.llm_available}")
    except Exception as e:
        logger.error(f"Worker: Error initializing SentimentAnalyzer: {e}")
        sentiment_analyzer = None # Ensure it's None on failure

    # Topic Modeler
    try:
        if topic_modeler is None:
            topic_modeler = TopicModeler(
                nlp=nlp_instance, # Use the loaded spaCy model
                mongodb_uri=config.get('mongodb_uri'),
                llm_url=config.get('local_llm_url') if use_local_llm_config else None,
                use_local_llm=True
            )
            model_info = topic_modeler.get_model_info()
            logger.info(f"Worker: TopicModeler initialized. LLM active: {model_info.get('llm_available', False)}")
    except Exception as e:
        logger.error(f"Worker: Error initializing TopicModeler: {e}")
        logger.error(traceback.format_exc())
        topic_modeler = None


    # Entity Mention Analyzer
    try:
        if entity_analyzer is None and config.get('postgres_uri'):
            import psycopg2 # Import locally as it might not be needed everywhere
            postgres_conn = None
            try:
                postgres_conn = psycopg2.connect(config['postgres_uri'])
                entity_analyzer = EntityMentionAnalyzer(
                    postgres_connection=postgres_conn, # Pass the connection
                    sentiment_analyzer=sentiment_analyzer, # Use initialized analyzer
                    use_local_llm=use_local_llm_config and current_llm_available,
                    local_llm_url=config.get('local_llm_url')
                )
                # Note: The connection should ideally be managed carefully.
                # Creating one per partition might be okay for low frequency,
                # but a pool might be better for high throughput.
                # For simplicity here, we create one. It will be closed when the worker process exits.
                logger.info("Worker: EntityMentionAnalyzer initialized with PostgreSQL connection.")
            except ImportError:
                 logger.warning("Worker: psycopg2 not installed. Cannot initialize EntityMentionAnalyzer.")
                 entity_analyzer = None
            except Exception as e:
                logger.warning(f"Worker: Failed to initialize EntityMentionAnalyzer with PostgreSQL: {e}")
                if postgres_conn:
                    postgres_conn.close() # Close connection if init failed
                entity_analyzer = None
        elif not config.get('postgres_uri'):
            logger.info("Worker: POSTGRES_URI not set, skipping EntityMentionAnalyzer initialization.")
            entity_analyzer = None

    except Exception as e:
        logger.error(f"Worker: Error during EntityMentionAnalyzer setup: {e}")
        entity_analyzer = None

    logger.info("Worker: Analysis components initialization complete.")


def check_llm_availability(url):
    """Checks local LLM availability with retries."""
    if not url:
        logger.warning("LOCAL_LLM_URL is not set, cannot check.")
        return False
    
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            logger.info(f"Checking Local LLM availability at {url} (attempt {attempt+1}/{max_attempts})")
            
            # Сначала проверяем /health
            health_response = requests.get(f"{url.split('/generate')[0]}/health", timeout=10)
            if health_response.status_code != 200:
                logger.warning(f"LLM health check failed with status {health_response.status_code}")
                time.sleep(5)
                continue
                
            health_data = health_response.json()
            if not health_data.get("model_ready", False):
                logger.info("LLM service is running but model is not ready yet")
                time.sleep(10)
                continue
                
            # Затем делаем тестовый запрос
            response = requests.post(
                url, 
                json={"prompt": "Привет", "max_tokens": 5},
                timeout=240
            )
            
            if response.status_code == 200:
                result = response.json()
                if "error" in result and result["error"]:
                    logger.warning(f"LLM test returned error: {result['error']}")
                else:
                    logger.info("LLM test request successful!")
                    return True
            else:
                logger.warning(f"LLM test returned status code {response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"LLM connection attempt {attempt+1} failed: {e}")
        
        # Не ждем перед последней попыткой
        if attempt < max_attempts - 1:
            logger.info(f"Waiting 15s before next LLM availability check...")
            time.sleep(15)
    
    logger.error("All LLM connection attempts failed")
    return False




# --- Main Processing Function for mapInPandas ---
import json # Add json import at the top

def process_partition_pandas(iterator: iter, config_broadcast) -> iter:
    """
    Processes partitions using Pandas UDF (mapInPandas).
    Initializes models once per worker process handling partitions.
    Applies sentiment, topic, entity extraction.
    RETURNS COMPLEX FIELDS AS JSON STRINGS.

    Args:
        iterator: An iterator of Pandas DataFrames.
        config_broadcast: A Spark broadcast variable holding configuration.

    Yields:
        An iterator of processed Pandas DataFrames matching intermediate_map_output_schema.
    """
    config = config_broadcast.value
    initialize_analysis_components(config) # Keep this as is

    sa = sentiment_analyzer
    tm = topic_modeler
    ea = entity_analyzer
    current_llm_available = config.get('llm_available', False)
    use_local_llm_config = config.get('use_local_llm', False)

    # --- Analysis functions now return Python dicts/lists ---
    def safe_analyze_sentiment(text):
        if not sa or not text or not isinstance(text, str):
            return None # Return None for easier JSON handling
        try:
            use_llm_for_this = use_local_llm_config and current_llm_available and (hash(text) % 5 == 0)
            result = sa.analyze(text, use_llm=use_llm_for_this)
            return {
                "label": result.get("label", "neutral"),
                "score": float(result.get("score", 0.5)),
                "method": result.get("method", "unknown"),
                "explanation": result.get("explanation", "")[:500]
            }
        except Exception as e:
            logger.error(f"Error in sentiment analysis for text '{str(text)[:50]}...': {e}")
            # Return None or an error dict that can be serialized
            return {"label": "error", "score": 0.0, "method": "udf_exception", "explanation": str(e)[:100]}

    def safe_get_topics(text):
        # ... (previous logic, returning list of dicts) ...
        if not tm or not text or not isinstance(text, str) or len(text) < 50:
            return None # Return None for easier JSON handling
        try:
            topics = tm.get_topics(text)
            return [{
                "label": t.get("label", ""),
                "keywords": t.get("keywords", []),
                "entities": t.get("entities", []),
                "similarity": float(t.get("similarity", 0.0))
            } for t in topics[:5]]
        except Exception as e:
            logger.error(f"Error in topic extraction for text '{str(text)[:50]}...': {e}")
            return None # Return None on error

    def safe_extract_main_entity(text):
        # ... (previous logic, returning dict or None) ...
        if not tm or not text or not isinstance(text, str) or len(text) < 30:
            return None
        try:
            entity_dict = tm.extract_main_entity(text)
            if entity_dict:
                return {
                    "name": entity_dict.get("name", ""),
                    "type": entity_dict.get("type", ""),
                    "description": entity_dict.get("description", "")[:500],
                    "method": entity_dict.get("method", "")
                }
            return None
        except Exception as e:
            logger.error(f"Error in main entity extraction for text '{str(text)[:50]}...': {e}")
            return None # Return None on error

    def safe_find_entity_mentions(row_dict):
        # ... (previous logic, returning list of dicts) ...
        if not ea or not row_dict:
            return None
        try:
            mentions = ea.find_entity_mentions(row_dict)
            return [{
                "entity_id": m.get("entity_id", ""),
                "entity_name": m.get("entity_name", ""),
                "sentiment": {
                    "label": m.get("sentiment", {}).get("label", "neutral"),
                    "score": float(m.get("sentiment", {}).get("score", 0.5)),
                    "method": m.get("sentiment", {}).get("method", "unknown"),
                    "explanation": m.get("sentiment", {}).get("explanation", "")[:500]
                }
            } for m in mentions[:10]]
        except Exception as e:
            logger.error(f"Error finding entity mentions: {e}")
            return None


    # Function to safely serialize Python objects to JSON strings
    def safe_json_dumps(obj):
        if obj is None:
            return None
        try:
            return json.dumps(obj, ensure_ascii=False) # ensure_ascii=False for non-Latin chars
        except Exception as e:
            logger.warning(f"Could not serialize object to JSON: {e}. Object: {str(obj)[:100]}")
            return None

    # Process each Pandas DataFrame chunk
    for pdf in iterator:
        # Apply analysis functions (they return Python dicts/lists/None)
        sentiment_results = pdf['text'].apply(safe_analyze_sentiment)
        topics_results = pdf['text'].apply(safe_get_topics)
        main_entity_results = pdf['text'].apply(safe_extract_main_entity)

        # Apply entity mentions row-wise
        entity_mentions_results = []
        if ea:
             for index, row in pdf.iterrows():
                 message_context_dict = {
                     "id": row.get("id"), "text": row.get("text"),
                     "channel_id": row.get("channel_id"), "timestamp": str(row.get("date"))
                 }
                 entity_mentions_results.append(safe_find_entity_mentions(message_context_dict))
        else:
             entity_mentions_results = [None for _ in range(len(pdf))]

        # --- Serialize results to JSON strings in new columns ---
        pdf['sentiment_json'] = sentiment_results.apply(safe_json_dumps)
        pdf['topics_json'] = topics_results.apply(safe_json_dumps)
        pdf['main_entity_json'] = main_entity_results.apply(safe_json_dumps)
        pdf['entity_mentions_json'] = pd.Series(entity_mentions_results).apply(safe_json_dumps) # Convert list to Series

        # --- Add other columns ---
        pdf['text_length'] = pdf['text'].apply(lambda x: len(x) if isinstance(x, str) else 0)
        pdf['processing_time'] = pd.Timestamp.utcnow()

        # --- Select columns matching intermediate_map_output_schema ---
        # Ensure base columns are present (they should be from the input iterator)
        # Add the new string and basic columns
        required_cols = [f.name for f in intermediate_map_output_schema]
        # Create default values for any potentially missing output columns
        for col_name in required_cols:
             if col_name not in pdf.columns:
                 pdf[col_name] = None # Or appropriate default

        # Important: Ensure Timestamps are compatible with Arrow if they are returned
        pdf['date'] = pd.to_datetime(pdf['date'])
        pdf['kafka_timestamp'] = pd.to_datetime(pdf['kafka_timestamp'])
        pdf['processing_time'] = pd.to_datetime(pdf['processing_time'])

        # Yield the DataFrame with JSON string columns
        yield pdf[required_cols]


# Class NewsProcessor remains primarily for Initialization & Utility on Driver
class NewsProcessor:
    def __init__(self, spark_context):
        logger.info("Initializing News Processor on Driver")
        self.spark_context = spark_context

        # Store config to be broadcasted
        self.config = {
            "use_local_llm": USE_LOCAL_LLM,
            "local_llm_url": LOCAL_LLM_URL,
            "mongodb_uri": MONGODB_URI,
            "mongodb_db": MONGODB_DB,
            "postgres_uri": POSTGRES_URI,
            "elasticsearch_host": ELASTICSEARCH_HOST,
            "elasticsearch_port": ELASTICSEARCH_PORT,
            "sentiment_cache_limit": SENTIMENT_CACHE_LIMIT,
            "topic_cache_limit": TOPIC_CACHE_LIMIT
        }

        # Check LLM availability on Driver (will be passed in config)
        self.config['llm_available'] = check_llm_availability(LOCAL_LLM_URL) if USE_LOCAL_LLM else False
        logger.info(f"Driver: LLM Availability Check: {self.config['llm_available']}")

        # Broadcast the configuration dictionary
        self.config_broadcast = self.spark_context.broadcast(self.config)
        logger.info("Configuration broadcasted to workers.")

        # Initialize components needed *only* on the Driver (e.g., ES/Mongo clients for specific actions)
        self.es = connect_to_elasticsearch(
            ELASTICSEARCH_HOST, ELASTICSEARCH_PORT, MAX_RETRIES, RETRY_DELAY, CONNECTION_TIMEOUT
        )
        self._setup_elasticsearch_indices()

        self.mongodb_client, self.mongodb_db = None, None
        if MONGODB_URI and MONGODB_DB:
            try:
                self.mongodb_client, self.mongodb_db = connect_to_mongodb(MONGODB_URI, MONGODB_DB)
                logger.info("Driver: Connected to MongoDB for metrics/updates.")
            except Exception as e:
                logger.error(f"Driver: Failed to connect to MongoDB: {e}")
        else:
            logger.warning("Driver: MongoDB URI/DB not set, metrics/updates disabled.")

        # We don't initialize the heavy NLP components here anymore.
        # They are initialized on workers via initialize_analysis_components.

        logger.info("NewsProcessor initialized on Driver.")


    def _setup_elasticsearch_indices(self):
        """Create Elasticsearch indices if they don't exist (runs on Driver)."""
        if not self.es:
            logger.error("Elasticsearch connection not available on Driver. Cannot setup indices.")
            return
        # --- Mapping for News Index ---
        # Use the final_output_schema (+ ES specific settings) to define mapping
        # (Simplified version - Adapt based on actual analysis needs)
        news_index_mapping = {
            "properties": {
                "id": {"type": "keyword"},
                "channel_id": {"type": "keyword"},
                "channel_name": {
                    "type": "text", "analyzer": "russian_analyzer",
                    "fields": {"keyword": {"type": "keyword"}}
                },
                "date": {"type": "date"},
                "text": {"type": "text", "analyzer": "russian_analyzer"},
                "has_media": {"type": "boolean"},
                "views": {"type": "integer"},
                "forwards": {"type": "integer"},
                "kafka_timestamp": {"type": "date"},
                "processing_time": {"type": "date"},
                "text_length": {"type": "integer"},
                "sentiment": { # Object based on sentiment_schema
                    "properties": {
                        "label": {"type": "keyword"},
                        "score": {"type": "float"},
                        "method": {"type": "keyword"},
                        "explanation": {"type": "text", "index": False}
                    }
                },
                "topics": { # Nested based on topic_schema
                    "type": "nested",
                    "properties": {
                        "label": {"type": "keyword"},
                        "keywords": {"type": "keyword"},
                        "entities": {"type": "keyword"},
                        "similarity": {"type": "float"}
                    }
                },
                "main_entity": { # Object based on entity_schema
                    "properties": {
                        "name": {"type": "keyword"},
                        "type": {"type": "keyword"},
                        "description": {"type": "text", "index": False},
                        "method": {"type": "keyword"}
                    }
                },
                "entity_mentions": { # Nested based on entity_mention_schema
                    "type": "nested",
                    "properties": {
                        "entity_id": {"type": "keyword"},
                        "entity_name": {"type": "keyword"},
                        "sentiment": { # Reusing sentiment properties
                            "properties": {
                                "label": {"type": "keyword"},
                                "score": {"type": "float"},
                                "method": {"type": "keyword"},
                                "explanation": {"type": "text", "index": False}
                            }
                        }
                    }
                }
            }
        }
        # Simplified Settings (reuse from original code)
        news_index_settings = {
            "number_of_shards": 1, "number_of_replicas": 0,
            "analysis": {
                 "analyzer": {"russian_analyzer": {"type": "custom", "tokenizer": "standard", "filter": ["lowercase", "russian_stop", "russian_stemmer"]}},
                 "filter": {"russian_stop": {"type": "stop", "stopwords": "_russian_"}, "russian_stemmer": {"type": "stemmer", "language": "russian"}}
             }
        }
        # --- Mapping for Metrics Index --- (Reuse from original code)
        metrics_index_mapping = {
            "properties": {
                "channel_name": {"type": "keyword"},
                "window_start": {"type": "date"},
                "window_end": {"type": "date"},
                "hourly_sentiment": {"type": "float"},
                "message_count": {"type": "integer"},
                "hourly_topics": {"type": "keyword"},
                "main_entities": {"type": "keyword"}
            }
        }
        metrics_index_settings = {"number_of_shards": 1, "number_of_replicas": 0}

        # --- Create Indices --- (Reuse logic from original code)
        indices_to_create = {
            ELASTICSEARCH_NEWS_INDEX: (news_index_settings, news_index_mapping),
            ELASTICSEARCH_METRICS_INDEX: (metrics_index_settings, metrics_index_mapping)
        }
        for index_name, (settings, mapping) in indices_to_create.items():
            try:
                if not self.es.indices.exists(index=index_name):
                    self.es.indices.create(index=index_name, settings=settings, mappings=mapping)
                    logger.info(f"Driver: Created Elasticsearch index: {index_name}")
                else:
                    logger.info(f"Driver: Elasticsearch index {index_name} already exists.")
                    # Optional: Add mapping update logic here if needed (be cautious)
            except Exception as e:
                logger.error(f"Driver: Failed to create/check Elasticsearch index {index_name}: {e}")
                # Decide if this is fatal

    def _log_analysis_metrics(self, batch_df, batch_id):
        """Логирование метрик в MongoDB (выполняется на драйвере в foreachBatch)"""
        if self.mongodb_db is None:
            logger.debug(f"Metrics Batch {batch_id}: MongoDB not available, skipping metrics logging.")
            return

        try:
            start_time = time.time()
            # --- Collect stats from the batch ---
            # Note: Collecting can be expensive. Consider sampling or approximate counts.
            message_count = batch_df.count()
            if message_count == 0:
                logger.debug(f"Metrics Batch {batch_id}: Empty batch, skipping metrics.")
                return

            sentiment_counts = batch_df.groupBy("sentiment.label").count().collect()
            sentiment_distribution = {row["label"]: row["count"] for row in sentiment_counts if row["label"]}

            # Topic distribution (requires exploding potentially nested data)
            topic_dist_df = batch_df.select(explode_outer(col("topics")).alias("topic_info")) \
                                     .groupBy("topic_info.label") \
                                     .agg(count("*").alias("count")) \
                                     .filter(col("label").isNotNull())
            topic_distribution = {row["label"]: row["count"] for row in topic_dist_df.collect()}

            # --- Prepare metrics documents ---
            # Note: Getting real-time stats (cache hits, LLM calls) from workers is complex.
            # These metrics reflect только distribution within the batch.
            # We can log the config used.
            llm_used_in_config = self.config.get('use_local_llm') and self.config.get('llm_available')

            processing_time = time.time() - start_time

            metrics_doc = {
                 "batch_id": batch_id,
                 "timestamp": datetime.utcnow(),
                 "message_count": message_count,
                 "sentiment_distribution": sentiment_distribution,
                 "topic_distribution_batch": topic_distribution,
                 "llm_available_in_config": self.config.get('llm_available'),
                 "llm_used_in_config": llm_used_in_config,
                 "metrics_collection_time_sec": processing_time
             }

            self.mongodb_db.analysis_metrics.insert_one(metrics_doc)
            logger.info(f"Metrics Batch {batch_id}: Logged analysis metrics ({message_count} msgs)")

            # --- Cache Clearing Logic (Needs adaptation) ---
            # Clearing caches based on stats collected *remotely* in workers is hard.
            # Simplification: Could clear based on driver-side counts or time intervals if needed,
            # but it wouldn't directly correspond to worker cache sizes.
            # Or, implement cache size check *within* worker functions (process_partition_pandas)
            # and trigger clearing there based on local worker state (complex).
            # Let's skip complex cache clearing for now.

        except Exception as e:
            logger.error(f"Metrics Batch {batch_id}: Error logging analysis metrics: {e}")
            logger.error(traceback.format_exc())


# --- Helper: Create Spark Session ---
def create_spark_session():
    logger.info("Creating Spark session")
    mongo_pkg = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1" # Check Spark/Scala compatibility
    # Or try latest compatible: "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1" for Spark 3.4+

    return SparkSession.builder \
        .appName("NewsFeedProcessor") \
        .config("spark.jars.packages",
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1," # Use Spark 3.4.1 packages if running Spark 3.4.1
            f"org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0," # Keep ES connector compatible
            f"{mongo_pkg}") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.driver.memory", DRIVER_MEMORY) \
        .config("spark.executor.memory", EXECUTOR_MEMORY) \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE_DIR) \
        .config("spark.sql.shuffle.partitions", os.cpu_count() * 2) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "1024m") \
        .getOrCreate()


# --- Helper: Read from Kafka ---
def read_from_kafka(spark):
    logger.info(f"Setting up Kafka stream reader for topic: {KAFKA_TOPIC}")
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("kafka.group.id", "telegram-news-processor-group") \
            .load()
        logger.info("Kafka stream reader configured.")
        return df
    except Exception as e:
        logger.error(f"Failed to configure Kafka stream reader: {e}")
        raise

# --- Helper: Write to Elasticsearch (using foreachBatch - reusable) ---
def write_to_elasticsearch_custom(df, index_name, id_field="id", checkpoint_suffix=""):
    """Writes DataFrame to Elasticsearch using foreachBatch."""
    es_options = {
        "es.resource": index_name,
        "es.nodes": ELASTICSEARCH_HOST,
        "es.port": ELASTICSEARCH_PORT,
        "es.nodes.wan.only": "true",
        "es.write.operation": "upsert" if id_field else "index",
        "es.mapping.id": id_field if id_field else None,
        "es.batch.size.bytes": "5mb", # Tune batch size
        "es.batch.size.entries": "1000"
    }
    if not es_options["es.mapping.id"]: del es_options["es.mapping.id"]

    checkpoint_path = f"{CHECKPOINT_BASE_DIR}/es_{index_name.replace('/', '_')}_{checkpoint_suffix}"
    logger.info(f"Setting up ES writer: index={index_name}, checkpoint={checkpoint_path}, id_field={id_field}")

    def process_batch(batch_df, batch_id):
        start_time = time.time()
        try:
            # Coalesce small batches to avoid too many small writes
            # batch_df = batch_df.repartition(1) # Careful: adds a shuffle
            count = batch_df.count() # Action to trigger computation and get count
            if count > 0:
                logger.info(f"ES Batch {batch_id}: Writing {count} records to index {index_name}")
                # Ensure schema matches ES mapping before writing; Spark should handle basic types
                batch_df.write \
                    .format("org.elasticsearch.spark.sql") \
                    .options(**es_options) \
                    .mode("append") \
                    .save()
                elapsed = time.time() - start_time
                logger.info(f"ES Batch {batch_id}: Wrote {count} records to {index_name} in {elapsed:.2f}s")
            else:
                logger.debug(f"ES Batch {batch_id}: Empty batch for {index_name}, skipping.")
        except Exception as e:
            logger.error(f"ES Batch {batch_id}: Error writing to ES index {index_name}: {e}")
            logger.error(traceback.format_exc())
            # Consider adding failed batch to DLQ

    return df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", checkpoint_path) \
        .start()


# --- Helper: Write to MongoDB (using foreachBatch - reusable) ---
def write_to_mongodb_custom(df, collection_name, checkpoint_suffix="", upsert_key=None):
    """Writes DataFrame to MongoDB using foreachBatch and pymongo."""
    checkpoint_path = f"{CHECKPOINT_BASE_DIR}/mongo_{collection_name}_{checkpoint_suffix}"
    logger.info(f"Setting up MongoDB writer: coll={collection_name}, checkpoint={checkpoint_path}, upsert_key={upsert_key}")

    def process_batch(batch_df, batch_id):
        start_time = time.time()
        if not MONGODB_URI or not MONGODB_DB:
            logger.warning(f"Mongo Batch {batch_id}: MongoDB not configured, skipping write to {collection_name}.")
            return

        try:
            # Persist the batch to avoid recomputation if count fails
            batch_df = batch_df.persist()
            count = batch_df.count()
            if count > 0:
                logger.info(f"Mongo Batch {batch_id}: Processing {count} records for collection {collection_name}")
                # Collect data to driver - potentially dangerous for large batches!
                # Consider mapPartitions with pymongo if batches are too large for driver.
                rows_json = batch_df.toJSON().collect()
                documents = [json.loads(row) for row in rows_json]

                client = None
                try:
                    # Establish connection within batch - pooling is recommended for prod
                    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=15000)
                    db = client[MONGODB_DB]
                    collection = db[collection_name]

                    from pymongo import UpdateOne # Import pymongo operation

                    operations = []
                    for doc in documents:
                        # Convert date strings back to datetime if needed by Mongo
                        # (Spark's toJSON might stringify them)
                        # Example: if 'window_start' in doc and isinstance(doc['window_start'], str):
                        #             doc['window_start'] = datetime.fromisoformat(doc['window_start'].replace('Z', '+00:00'))

                        if upsert_key and upsert_key in doc and doc[upsert_key] is not None:
                            filter_criteria = {upsert_key: doc[upsert_key]}
                            operations.append(UpdateOne(filter_criteria, {"$set": doc}, upsert=True))
                        elif "id" in doc and doc["id"] is not None: # Default upsert on "id"
                            operations.append(UpdateOne({"id": doc["id"]}, {"$set": doc}, upsert=True))
                        else: # Fallback to insert if no key
                             operations.append(UpdateOne({"_id": doc.get("id", None)}, {"$set": doc}, upsert=True)) # Try upserting on _id=id

                    if operations:
                        result = collection.bulk_write(operations, ordered=False)
                        elapsed = time.time() - start_time
                        logger.info(f"Mongo Batch {batch_id}: Wrote {result.upserted_count + result.modified_count}/{len(documents)} docs "
                                    f"to {collection_name} in {elapsed:.2f}s "
                                    f"(Upserted: {result.upserted_count}, Modified: {result.modified_count})")
                    else:
                         logger.info(f"Mongo Batch {batch_id}: No valid documents/operations for {collection_name}.")

                except pymongo_errors.ConnectionFailure as ce:
                    logger.error(f"Mongo Batch {batch_id}: Connection failure to MongoDB ({MONGODB_URI}): {ce}")
                except pymongo_errors.BulkWriteError as bwe:
                    logger.error(f"Mongo Batch {batch_id}: Bulk write error to MongoDB collection {collection_name}: {bwe.details}")
                except Exception as e:
                    logger.error(f"Mongo Batch {batch_id}: Error writing to MongoDB collection {collection_name}: {e}")
                    logger.error(traceback.format_exc())
                finally:
                    if client: client.close()
            else:
                logger.debug(f"Mongo Batch {batch_id}: Empty batch for {collection_name}, skipping.")
        except Exception as e:
            logger.error(f"Mongo Batch {batch_id}: Error processing batch for MongoDB: {e}")
            logger.error(traceback.format_exc())
        finally:
             batch_df.unpersist() # Release persisted RDD


    return df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", checkpoint_path) \
        .start()


# --- Specific Writer for Processed News with Mention Storage (using foreachBatch) ---
def write_processed_data_with_mentions(df, news_processor, mongo_msg_collection="messages", mongo_mention_collection="entity_mentions", es_index=ELASTICSEARCH_NEWS_INDEX):
    """Writes processed data to ES and MongoDB messages, and extracts/writes mentions separately."""
    checkpoint_path = f"{CHECKPOINT_BASE_DIR}/processed_news_combined"
    logger.info(f"Setting up combined writer: ES={es_index}, MongoMsg={mongo_msg_collection}, MongoMention={mongo_mention_collection}, checkpoint={checkpoint_path}")

    # Driver-side connections needed for logging/updates inside foreachBatch
    es_host = news_processor.config['elasticsearch_host']
    es_port = news_processor.config['elasticsearch_port']
    mongo_available = news_processor.mongodb_db is not None

    es_options = {
        "es.resource": es_index, "es.nodes": es_host, "es.port": es_port,
        "es.nodes.wan.only": "true", "es.write.operation": "upsert", "es.mapping.id": "id",
        "es.batch.size.bytes": "5mb", "es.batch.size.entries": "1000"
    }

    def process_batch(batch_df, batch_id):
        start_time = time.time()
        if not news_processor:
            logger.error(f"Combined Batch {batch_id}: NewsProcessor instance unavailable.")
            return

        try:
            # Persist to avoid recomputing DataFrame multiple times
            batch_df = batch_df.persist()
            count = batch_df.count()

            if count > 0:
                logger.info(f"Combined Batch {batch_id}: Processing {count} records for ES/{es_index}, Mongo/{mongo_msg_collection}, Mentions/{mongo_mention_collection}")

                # --- 1. Log Analysis Metrics (on Driver) ---
                news_processor._log_analysis_metrics(batch_df, batch_id)

                # --- 2. Write to Elasticsearch ---
                try:
                    # Select columns matching ES mapping (should be covered by final_output_schema)
                    batch_df.select([f.name for f in final_output_schema]).write \
                        .format("org.elasticsearch.spark.sql") \
                        .options(**es_options) \
                        .mode("append") \
                        .save()
                    logger.info(f"Combined Batch {batch_id}: Wrote {count} records to Elasticsearch index {es_index}")
                except Exception as es_e:
                    logger.error(f"Combined Batch {batch_id}: Error writing to Elasticsearch: {es_e}")
                    # Log only, continue processing

                # --- 3. Write Main Messages to MongoDB ---
                if mongo_available:
                    client = None
                    try:
                        # Select necessary columns for the main message document
                        # Convert to JSON strings, then parse back to dicts
                        rows_json = batch_df.select([f.name for f in final_output_schema]).toJSON().collect()
                        documents = [json.loads(row) for row in rows_json]

                        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=15000)
                        db = client[MONGODB_DB]
                        collection = db[mongo_msg_collection]
                        from pymongo import UpdateOne

                        operations = []
                        for doc in documents:
                            if "id" in doc and doc["id"]:
                                # Optionally clean up dates if needed
                                # if 'date' in doc and isinstance(doc['date'], str): doc['date'] = datetime.fromisoformat(...)
                                operations.append(UpdateOne({"id": doc["id"]}, {"$set": doc}, upsert=True))

                        if operations:
                            result = collection.bulk_write(operations, ordered=False)
                            logger.info(f"Combined Batch {batch_id}: Wrote {result.upserted_count + result.modified_count}/{len(documents)} messages to MongoDB {mongo_msg_collection}")
                        else:
                            logger.info(f"Combined Batch {batch_id}: No valid messages found for MongoDB {mongo_msg_collection}")

                    except Exception as mongo_e:
                        logger.error(f"Combined Batch {batch_id}: Error writing main messages to MongoDB {mongo_msg_collection}: {mongo_e}")
                    finally:
                        if client: client.close()
                else:
                    logger.debug(f"Combined Batch {batch_id}: MongoDB unavailable, skipping write to {mongo_msg_collection}")

                # --- 4. Extract and Write Entity Mentions ---
                if mongo_available:
                    client = None
                    mention_docs_to_insert = []
                    try:
                        # Select only message ID and the already extracted entity mentions array
                        # entity_mentions column should already contain list of dicts matching schema
                        mention_data = batch_df.select("id", "entity_mentions") \
                                              .filter(col("entity_mentions").isNotNull() & (size(col("entity_mentions")) > 0)) \
                                              .collect() # Collect to driver

                        if mention_data:
                             processing_ts = datetime.utcnow()
                             for row in mention_data:
                                 message_id = row.id
                                 mentions = row.entity_mentions # This is Array[Struct] -> List[Row] -> List[Dict] hopefully
                                 for mention_dict in mentions: # Iterate through the list of mention dicts
                                     if isinstance(mention_dict, Row): mention_dict = mention_dict.asDict(recursive=True) # Convert Row to dict if needed

                                     # Basic validation
                                     if not mention_dict or not mention_dict.get('entity_id'): continue

                                     mention_doc = {
                                         "_id": f"{message_id}_{mention_dict.get('entity_id')}_{mention_dict.get('entity_name', '')[:10]}", # Create a unique ID
                                         "message_id": message_id,
                                         "entity_id": mention_dict.get("entity_id"),
                                         "entity_name": mention_dict.get("entity_name"),
                                         "sentiment_label": mention_dict.get("sentiment", {}).get("label"),
                                         "sentiment_score": mention_dict.get("sentiment", {}).get("score"),
                                         "sentiment_method": mention_dict.get("sentiment", {}).get("method"),
                                         # "sentiment_explanation": mention_dict.get("sentiment", {}).get("explanation"), # Keep explanation?
                                         "processing_timestamp": processing_ts
                                     }
                                     mention_docs_to_insert.append(UpdateOne(
                                        {"_id": mention_doc["_id"]},
                                        {"$set": mention_doc},
                                        upsert=True
                                     ))


                        if mention_docs_to_insert:
                            client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=15000)
                            db = client[MONGODB_DB]
                            mention_collection = db[mongo_mention_collection]
                            result = mention_collection.bulk_write(mention_docs_to_insert, ordered=False)
                            logger.info(f"Combined Batch {batch_id}: Upserted {result.upserted_count + result.modified_count}/{len(mention_docs_to_insert)} entity mentions to MongoDB {mongo_mention_collection}")
                        else:
                            logger.info(f"Combined Batch {batch_id}: No entity mentions found in this batch.")

                    except Exception as mention_e:
                        logger.error(f"Combined Batch {batch_id}: Error writing entity mentions to MongoDB: {mention_e}")
                        logger.error(traceback.format_exc())
                    finally:
                        if client: client.close()
                else:
                     logger.debug(f"Combined Batch {batch_id}: MongoDB unavailable, skipping mention storage.")

                elapsed = time.time() - start_time
                logger.info(f"Combined Batch {batch_id}: Finished processing {count} records in {elapsed:.2f}s.")

            else:
                logger.debug(f"Combined Batch {batch_id}: Empty batch, skipping.")

        except Exception as e:
            logger.error(f"Combined Batch {batch_id}: Error processing combined batch: {e}")
            logger.error(traceback.format_exc())
        finally:
            batch_df.unpersist() # Release memory

    return df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", checkpoint_path) \
        .start()


# --- Main Data Processing Logic ---
def process_data(df, config_broadcast):
    """Parses Kafka messages, applies enrichments using mapInPandas (returning JSON strings),
       and then parses JSON strings back to complex Spark types."""
    logger.info("Applying processing steps using mapInPandas and JSON serialization")

    # --- 1. Parse JSON from Kafka ---
    parsed_df = df.select(
        col("key").cast("string"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), message_schema).alias("data")
    ).select("key", "kafka_timestamp", "data.*")

    parsed_df = parsed_df.filter(col("id").isNotNull() & (col("text").isNotNull()))

    # Define input columns for mapInPandas (ensure all needed fields are selected)
    input_df_for_map = parsed_df.select(
        "id", "channel_id", "channel_name", "date", "text",
        "views", "forwards", "has_media", "kafka_timestamp"
    )


    # --- 2. Apply Enrichments using mapInPandas (returns JSON strings) ---
    intermediate_df = input_df_for_map.mapInPandas(
        lambda iterator: process_partition_pandas(iterator, config_broadcast),
        schema=intermediate_map_output_schema
    )

    final_df = intermediate_df \
        .withColumn("sentiment", from_json(col("sentiment_json"), sentiment_schema)) \
        .withColumn("topics", from_json(col("topics_json"), topics_array_schema)) \
        .withColumn("main_entity", from_json(col("main_entity_json"), entity_schema)) \
        .withColumn("entity_mentions", from_json(col("entity_mentions_json"), entity_mentions_array_schema)) \
        .withColumn("has_media", col("has_media").cast(BooleanType())) # Cast boolean here

    final_processed_df = final_df.select(
         "id", "channel_id", "channel_name", "date", "text", "text_length",
         "sentiment", "topics", "main_entity", "entity_mentions",
         "views", "forwards", "has_media",
         "processing_time", "kafka_timestamp"
    )


    logger.info("Data processing steps configured (mapInPandas + from_json).")
    return final_processed_df



# --- Calculate Channel Metrics --- (Reuse logic from original code, minor adjustments)
def calculate_hourly_channel_metrics(processed_df):
    """Calculates hourly metrics per channel using Spark SQL aggregations."""
    logger.info("Configuring hourly channel metrics calculation")

    # 1. Explode the topics.label array to get individual topic labels
    exploded_df = processed_df.select(
        "*",
        explode_outer(col("topics.label")).alias("topic_label")
    )

    windowed_df = exploded_df \
        .withWatermark("date", "1 hour") \
        .groupBy(
            window(col("date"), "1 hour", "1 hour"),
            col("channel_name")
        )

    hourly_metrics = windowed_df.agg(
        avg(col("sentiment.score")).alias("hourly_sentiment"),
        count("*").alias("message_count"),
        collect_set(col("topic_label")).alias("distinct_topic_labels"),
        collect_set(col("main_entity.name")).alias("distinct_entity_names")
    )

    # 4. Clean up and finalize the DataFrame
    hourly_metrics_flat = hourly_metrics.select(
        col("channel_name"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("hourly_sentiment"),
        col("message_count"),
        col("distinct_topic_labels"),  # Already filtered for non-nulls
        col("distinct_entity_names")   # Same here
    ).filter(col("channel_name").isNotNull())

    logger.info("Hourly channel metrics calculation configured.")
    return hourly_metrics_flat



def main():
    logger.info("---- Starting News Feed Processor Application ----")
    spark = None
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created.")
        logger.info(f"Spark Config: {spark.sparkContext.getConf().getAll()}")

        news_processor = NewsProcessor(spark.sparkContext)
        logger.info("NewsProcessor (Driver part) initialized successfully.")


        kafka_df = read_from_kafka(spark)

        processed_df = process_data(kafka_df, news_processor.config_broadcast)

        news_write_query = write_processed_data_with_mentions(
            processed_df,
            news_processor,
            mongo_msg_collection="messages",
            mongo_mention_collection="entity_mentions",
            es_index=ELASTICSEARCH_NEWS_INDEX
        )
        logger.info("Started stream writing processed data (ES, Mongo Msg, Mongo Mention).")

        hourly_metrics_df = calculate_hourly_channel_metrics(processed_df)

        write_to_elasticsearch_custom(
            hourly_metrics_df,
            ELASTICSEARCH_METRICS_INDEX,
            id_field=None,
            checkpoint_suffix="metrics_es"
        )
        logger.info("Started stream writing hourly metrics to Elasticsearch.")
        logger.info("All streaming queries started. Waiting for termination...")
        news_write_query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Stopping streams...")
    except Exception as e:
        logger.error(f"An critical error occurred in the main processing loop: {e}")
        logger.error(traceback.format_exc())
    finally:
        logger.info("---- Stopping Spark Session ----")
        if spark:
            active_streams = spark.streams.active
            if active_streams:
                 logger.info(f"Stopping {len(active_streams)} active streaming queries...")
                 for query in active_streams:
                    try:
                        logger.info(f"Stopping query: {query.name} (ID: {query.id})")
                        query.stop()
                        query.awaitTermination(timeout=60)
                        logger.info(f"Query {query.id} stopped.")
                    except Exception as stop_e:
                        logger.error(f"Error stopping query {query.id}: {stop_e}")
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
