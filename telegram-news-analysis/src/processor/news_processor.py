
import os
import json
import logging
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, length
from pyspark.sql.types import (
    ArrayType, StructType, StructField, StringType, FloatType,
    TimestampType, IntegerType
)
import hashlib
from pymongo import MongoClient, UpdateOne, ASCENDING
import asyncio
from functools import partial
import nest_asyncio
from typing import Dict, List, Tuple, Optional
from elasticsearch import Elasticsearch
import aiohttp
from datetime import datetime
import pandas as pd
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---- Конфиги ---- #
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ELASTICSEARCH_NEWS_INDEX = os.getenv("ELASTICSEARCH_NEWS_INDEX", "telegram_news")
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "telegram_news")
LLM_SENTIMENT_URL = os.getenv("LLM_SENTIMENT_URL", "http://llm-light-1:8000/generate")
LLM_TOPICS_URL = os.getenv("LLM_TOPICS_URL", "http://llm-light-2:8000/generate")
LLM_ENTITY_URL = os.getenv("LLM_ENTITY_URL", "http://llm-light-3:8000/generate")
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/checkpoints_realtime")
DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")
EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "8g")
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
ELASTICSEARCH_METRICS_INDEX = os.getenv("ELASTICSEARCH_METRICS_INDEX", "telegram_metrics")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telegram_news")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "1"))
CONNECTION_TIMEOUT = int(os.getenv("CONNECTION_TIMEOUT", "600"))
POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://admin:password@postgres:5432/telenews")
USE_LIGHTWEIGHT_LLM = os.getenv("USE_LIGHTWEIGHT_LLM", "true").lower() == "true"

JARS_DIR = "/app/jars"
JARS_AVAILABLE = os.path.exists(JARS_DIR) and len(os.listdir(JARS_DIR)) > 0

# Схемы данных
message_schema = StructType([
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("channel_id", StringType(), True),
    StructField("channel_name", StringType(), True),
    StructField("date", TimestampType(), True),
    StructField("views", IntegerType(), True),
    StructField("forwards", IntegerType(), True),
    StructField("has_media", StringType(), True),
])

sentiment_schema = StructType([
    StructField("label", StringType(), True),
    StructField("score", FloatType(), True),
    StructField("explanation", StringType(), True),
])

topics_schema = ArrayType(
    StructType([
        StructField("label", StringType(), True),
        StructField("keywords", ArrayType(StringType()), True),
        StructField("similarity", FloatType(), True)
    ])
)

entity_schema = StructType([
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("description", StringType(), True)
])

# Кэш для LLM ответов с ограничением размера
from collections import OrderedDict
class LRUCache(OrderedDict):
    def __init__(self, maxsize=1000):
        super().__init__()
        self.maxsize = maxsize
        
    def __getitem__(self, key):
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value
        
    def __setitem__(self, key, value):
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        if len(self) > self.maxsize:
            self.popitem(last=False)

LLM_CACHE = LRUCache(maxsize=10000)

def get_elasticsearch_client() -> Elasticsearch:
    """Создает и возвращает клиент Elasticsearch"""
    try:
        return Elasticsearch([{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT, "scheme": "http"}])
    except Exception:
        return Elasticsearch(f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}")

def ensure_es_index() -> None:
    """Создает индекс в Elasticsearch если он не существует"""
    es = get_elasticsearch_client()
    
    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "text": {"type": "text"},
                "channel_id": {"type": "keyword"},
                "channel_name": {"type": "keyword"},
                "date": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss||strict_date_optional_time||epoch_millis"},
                "views": {"type": "integer"},
                "forwards": {"type": "integer"},
                "has_media": {"type": "keyword"},
                "sentiment": {
                    "properties": {
                        "label": {"type": "keyword"},
                        "score": {"type": "float"},
                        "explanation": {"type": "text"}
                    }
                },
                "topics": {
                    "type": "nested",
                    "properties": {
                        "label": {"type": "keyword"},
                        "keywords": {"type": "keyword"},
                        "similarity": {"type": "float"}
                    }
                },
                "main_entity": {
                    "properties": {
                        "name": {"type": "keyword"},
                        "type": {"type": "keyword"},
                        "description": {"type": "text"}
                    }
                },
                "entity_mentions": {
                    "type": "nested",
                    "properties": {
                        "entity_name": {"type": "keyword"},
                        "offset": {"type": "integer"}
                    }
                }
            }
        },
        "settings": {
            "index": {
                "refresh_interval": "1s", # Более частое обновление индекса
                "number_of_shards": 3,    # Увеличено количество шардов
                "number_of_replicas": 1
            }
        }
    }

    if not es.indices.exists(index=ELASTICSEARCH_NEWS_INDEX):
        es.indices.create(index=ELASTICSEARCH_NEWS_INDEX, body=mapping)
        logger.info(f"Создан индекс {ELASTICSEARCH_NEWS_INDEX}")
    else:
        logger.info(f"Индекс {ELASTICSEARCH_NEWS_INDEX} уже существует")

def safe_first_json_obj(text: str) -> dict:
    """Извлекает первый JSON объект из текста"""
    match = re.search(r'\{[\s\S]*?\}', text)
    if match:
        try:
            return json.loads(match.group(0))
        except Exception as e:
            logger.warning(f"Error parsing JSON from LLM: {e}. Raw={match.group(0)[:100]}")
    return {}

def safe_first_json_array(text: str) -> List[dict]:
    """Извлекает JSON массив из текста"""
    match = re.search(r'\[\s*\{[\s\S]*?\}\s*\]', text)
    if match:
        try:
            arr = json.loads(match.group(0))
            if isinstance(arr, list):
                return [x for x in arr if isinstance(x, dict)]
        except Exception as e:
            logger.warning(f"Error parsing JSON array from LLM: {e}. Raw={match.group(0)[:100]}")
    return []

def run_async_in_thread(async_func, *args, **kwargs):
    """Запускает асинхронную функцию в отдельном потоке"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    nest_asyncio.apply(loop)
    try:
        return loop.run_until_complete(async_func(*args, **kwargs))
    finally:
        loop.close()

async def async_llm_request(url: str, payload: dict, timeout: int = CONNECTION_TIMEOUT) -> Optional[str]:
    """Асинхронный запрос к LLM сервису"""
    connector = aiohttp.TCPConnector(limit=100) # Увеличен лимит соединений
    for attempt in range(MAX_RETRIES):
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(url, json=payload, timeout=timeout) as response:
                    if response.status == 200:
                        resp_data = await response.json()
                        return resp_data.get("generated_text", resp_data.get("text", ""))
                    logger.warning(f"Ошибка API, статус: {response.status}, URL: {url}")
        except Exception as e:
            logger.warning(f"Ошибка запроса к {url}: {str(e)}")
        
        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(RETRY_DELAY)
    
    return None

async def parallel_llm_requests(text: str) -> Tuple[str, str, str]:
    """Параллельные запросы к LLM сервисам"""
    if not USE_LIGHTWEIGHT_LLM or not text or len(text) < 20:
        return (
            json.dumps({"label": "neutral", "score": 0.5, "explanation": "LLM отключен/короткий текст"}, ensure_ascii=False),
            json.dumps([{"label": "Общество", "keywords": [], "similarity": 0.7}], ensure_ascii=False),
            json.dumps({"name": "", "type": "", "description": ""}, ensure_ascii=False)
        )

    cache_key = hashlib.md5(text.encode("utf-8")).hexdigest()
    if cache_key in LLM_CACHE:
        return LLM_CACHE[cache_key]

    prompts = {
        "sentiment": f"""
            You are a helpful assistant for Russian news analysis.
            Analyze the sentiment of this news and reply with a SINGLE valid JSON.
            
            Example response:
            {{
            "label": "positive",
            "score": 0.9,
            "explanation": "good result"
            }}

            News: "{text[:400]}"
        """,
        "topics": f"""
            You are an intelligent news classifier.
            Classify the MAIN TOPIC of this news.
            
            Reply ONLY with a JSON array containing ONE object.

            Valid topics:
            - Политика
            - Экономика
            - Происшествия
            - Общество
            - Технологии
            - Спорт
            - Культура
            - Здоровье
            - Наука
            - Военные
            - Знаменитости

            Example:
            [
            {{
                "label": "Общество",
                "keywords": ["животные", "пес"],
                "similarity": 0.9
            }}
            ]

            News:
            "{text[:600]}"
        """,
        "entity": f"""
            You are a precise information extractor.
            Given a news fragment, extract the MAIN entity (person, organization, location, or event) it discusses.

            Reply ONLY with ONE valid JSON object and NOTHING else.

            Example:
            {{
            "name": "ООН",
            "type": "ОРГАНИЗАЦИЯ",
            "description": "Международная организация"
            }}

            News:
            "{text}"
        """
    }

    payloads = {k: {"prompt": v, "max_tokens": 120, "temperature": 0.3} for k, v in prompts.items()}
    
    urls = {
        "sentiment": LLM_SENTIMENT_URL,
        "topics": LLM_TOPICS_URL,
        "entity": LLM_ENTITY_URL
    }

    tasks = [async_llm_request(urls[k], payloads[k]) for k in ["sentiment", "topics", "entity"]]
    responses = await asyncio.gather(*tasks)

    sentiment = process_sentiment_response(responses[0] or "", text)
    topics = process_topics_response(responses[1] or "", text)
    entity = process_entity_response(responses[2] or "", text)

    result = (
        json.dumps(sentiment, ensure_ascii=False),
        json.dumps(topics, ensure_ascii=False),
        json.dumps(entity, ensure_ascii=False)
    )
    
    LLM_CACHE[cache_key] = result
    return result

def process_sentiment_response(response_text: str, original_text: str) -> dict:
    """Обработка ответа для sentiment"""
    sentiment = safe_first_json_obj(response_text)
    if not isinstance(sentiment, dict) or "label" not in sentiment:
        sentiment = {"label": "neutral", "score": 0.5, "explanation": "Failed to extract sentiment"}

    label = sentiment.get("label", "").lower()
    score = sentiment.get("score")
    explanation = sentiment.get("explanation", "")

    valid_labels = ["positive", "negative", "neutral"]
    if label not in valid_labels:
        lower_text = original_text.lower()
        negative_words = ["ужас", "катастроф", "трагед", "авари", "гибел", "умер", "убит", "разруш", "обрушил", "напад", "конфликт", "дтп", "ранен", "пострадавш", "погиб"]
        positive_words = ["успех", "радост", "побед", "достиж", "хорош", "прекрасн", "удач", "счаст", "любов"]
        
        neg_count = sum(1 for word in negative_words if word in lower_text)
        pos_count = sum(1 for word in positive_words if word in lower_text)
        
        label = "negative" if neg_count > pos_count else "positive" if pos_count > neg_count else "neutral"

    if not isinstance(score, (float, int)):
        score = {"positive": 0.75, "negative": 0.25, "neutral": 0.5}[label]
    else:
        score = float(score)
        if label == "positive" and score < 0.55:
            score = 0.7
        elif label == "negative" and score > 0.45:
            score = 0.3
        elif label == "neutral" and (score < 0.4 or score > 0.6):
            score = 0.5

    return {
        "label": label,
        "score": round(score, 3),
        "explanation": (explanation or "Автоматическое определение тональности")[:120]
    }

def process_topics_response(response_text: str, original_text: str) -> List[dict]:
    """Обработка ответа для topics"""
    valid_labels = {
        "Политика", "Экономика", "Происшествия", "Общество",
        "Технологии", "Спорт", "Культура", "Здоровье",
        "Наука", "Военные", "Знаменитости"
    }
    
    topics = safe_first_json_array(response_text)
    corrected_topics = []
    lower_text = original_text.lower()
    
    for topic in (topics if isinstance(topics, list) else []):
        if not isinstance(topic, dict):
            continue

        label = topic.get("label", "")
        if not label or label not in valid_labels:
            found = next((v for v in valid_labels if v.lower() in lower_text), "Общество")
            label = found

        sim = topic.get("similarity", 0.7)
        try:
            sim = float(sim)
            if sim <= 0 or sim > 1:
                sim = 0.7
        except Exception:
            sim = 0.7

        keywords = topic.get("keywords", [])
        if not isinstance(keywords, list):
            keywords = []
            
        corrected_topics.append({"label": label, "keywords": keywords, "similarity": sim})

    return corrected_topics or [{"label": "Общество", "keywords": [], "similarity": 0.7}]

def process_entity_response(response_text: str, original_text: str) -> dict:
    """Обработка ответа для entity"""
    entity = safe_first_json_obj(response_text)
    if not isinstance(entity, dict):
        entity = {}

    name = entity.get("name", "")
    if name:
        name = re.sub(r'[^\w\s\-.,а-яА-ЯёЁ]', '', name).strip()
    if not name:
        words = re.findall(r'[А-Я][а-яА-Я\-]+', original_text)
        name = words[0] if words else ""
    
    valid_types = {"ЧЕЛОВЕК", "ОРГАНИЗАЦИЯ", "МЕСТО", "СОБЫТИЕ"}
    eng_to_rus = {
        "PERSON": "ЧЕЛОВЕК",
        "ORGANIZATION": "ОРГАНИЗАЦИЯ", 
        "LOCATION": "МЕСТО",
        "EVENT": "СОБЫТИЕ"
    }
    
    etype = entity.get("type", "").upper()
    etype = eng_to_rus.get(etype, etype)
    
    if etype not in valid_types:
        if name:
            if any(name.lower().endswith(x) for x in ["виль", "град", "бург", "ск", "ово", "ино"]):
                etype = "МЕСТО"
            elif name.isupper():
                etype = "ОРГАНИЗАЦИЯ"
            else:
                etype = "ЧЕЛОВЕК"
        else:
            etype = ""
    
    description = entity.get("description", "")
    if not description and name:
        description = "Упоминается в новости"
    
    return {
        "name": name,
        "type": etype,
        "description": description[:100] if description else ""
    }

def llm_sentiment(text: str) -> str:
    """UDF для получения sentiment из LLM"""
    return run_async_in_thread(partial(parallel_llm_requests, text))[0]

def llm_topics(text: str) -> str:
    """UDF для получения topics из LLM"""
    return run_async_in_thread(partial(parallel_llm_requests, text))[1]

def llm_entity(text: str) -> str:
    """UDF для получения entity из LLM"""
    return run_async_in_thread(partial(parallel_llm_requests, text))[2]

async def check_service(session: aiohttp.ClientSession, service: dict, health_url: str) -> bool:
    """Проверка доступности сервиса"""
    try:
        async with session.get(health_url, timeout=3) as response:
            if response.status == 200:
                logger.info(f"Сервис {service['name']} доступен по URL {service['url']}")
                return True
            logger.error(f"Сервис {service['name']} недоступен, статус: {response.status}")
            return False
    except Exception as e:
        logger.error(f"Ошибка при проверке сервиса {service['name']}: {str(e)}")
        return False

async def async_check_llm_services() -> bool:
    """Асинхронная проверка доступности LLM сервисов"""
    services = [
        {"name": "Sentiment LLM", "url": LLM_SENTIMENT_URL},
        {"name": "Topics LLM", "url": LLM_TOPICS_URL},
        {"name": "Entity LLM", "url": LLM_ENTITY_URL}
    ]
    
    async with aiohttp.ClientSession() as session:
        tasks = [check_service(session, service, service["url"].replace("/generate", "/health")) 
                for service in services]
        results = await asyncio.gather(*tasks)
        
    all_available = all(results)
    if not all_available:
        logger.warning("Некоторые LLM-сервисы недоступны. Проверьте конфигурацию.")
    
    return all_available

def check_llm_services() -> bool:
    """Проверка доступности сервисов"""
    return run_async_in_thread(async_check_llm_services)

def write_to_mongo(batch_df, _) -> None:
    """Запись пакета данных в MongoDB"""
    try:
        rows = batch_df.collect()
        pdf = pd.DataFrame([row.asDict() for row in rows])

        def convert_datetime(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, dict):
                return {k: convert_datetime(v) for k, v in obj.items()}
            elif isinstance(obj, (list, tuple)):
                return [convert_datetime(x) for x in obj]
            return obj

        for col in pdf.columns:
            if pd.api.types.is_datetime64_any_dtype(pdf[col]):
                pdf[col] = pdf[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

        records = []
        for record in pdf.to_dict('records'):
            try:
                record = convert_datetime(record)
                record = {k: v for k, v in record.items() if v is not None}
                records.append(record)
            except Exception as e:
                logger.warning(f"Ошибка обработки записи: {str(e)[:200]}")

        if not records:
            logger.info("Нет записей для записи в MongoDB")
            return

        mongo = MongoClient(MONGODB_URI, 
                          maxPoolSize=100,  # Увеличен размер пула соединений
                          connectTimeoutMS=2000,  # Уменьшен timeout
                          socketTimeoutMS=5000)
        db = mongo[MONGODB_DB]
        collection = db.messages

        try:
            collection.create_index(
                [("id", ASCENDING), ("channel_id", ASCENDING)],
                unique=True,
                name="message_id_channel_id_idx"
            )
        except Exception as e:
            logger.debug(f"Индекс уже существует или ошибка создания: {str(e)}")

        batch_size = 200  # Увеличен размер батча
        total_processed = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            operations = []
            
            for record in batch:
                filter_doc = {
                    "message_id": record.get('id'),
                    "channel_id": record.get('channel_id')
                } if record.get('id') and record.get('channel_id') else {
                    "text_hash": record.get('text_hash', 
                        hashlib.md5(str(record.get('text', '')).encode()).hexdigest()),
                    "channel_id": record.get('channel_id', ''),
                    "date": record.get('date', '')
                }
                
                operations.append(UpdateOne(
                    filter_doc,
                    {"$set": record},
                    upsert=True
                ))

            if operations:
                collection.bulk_write(operations, ordered=False)
                total_processed += len(operations)
                logger.debug(f"Обработано {len(operations)} записей (всего: {total_processed})")

        logger.info(f"Успешно записано {total_processed} записей в MongoDB")

    except Exception as e:
        logger.error(f"Критическая ошибка при записи в MongoDB: {str(e)}")

def main() -> None:
    """Основная функция обработки новостей"""
    check_llm_services()
    
    sentiment_udf = udf(llm_sentiment, StringType())
    topics_udf = udf(llm_topics, StringType())
    entity_udf = udf(llm_entity, StringType())

    logger.info("Инициализация Spark для обработки новостей Telegram")
    
    spark = SparkSession.builder \
        .appName("NewsFeedProcessor") \
        .config("spark.jars.packages", 
                "" if JARS_AVAILABLE else 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1," 
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0," 
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.jars", 
                ",".join([os.path.join(JARS_DIR, f) for f in os.listdir(JARS_DIR)]) 
                if JARS_AVAILABLE else "") \
        .config("spark.jars.excludes", "javax.ws.rs:javax.ws.rs-api") \
        .config("spark.driver.memory", DRIVER_MEMORY) \
        .config("spark.executor.memory", EXECUTOR_MEMORY) \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.sql.shuffle.partitions", max(os.cpu_count(), 4)) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "128m") \
        .config("spark.streaming.kafka.consumer.poll.ms", "500") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "5000") \
        .config("spark.streaming.concurrentJobs", "4") \
        .config("spark.default.parallelism", max(os.cpu_count() * 2, 8)) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .getOrCreate()

    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Настройка чтения из Kafka топика {KAFKA_TOPIC}")

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 10000) \
        .option("fetchOffset.numRetries", 8) \
        .option("kafkaConsumer.pollTimeoutMs", 1000) \
        .load()

    parsed = df.select(
        from_json(col("value").cast("string"), message_schema).alias("data")
    ).select("data.*")

    logger.info("Настройка обогащения данных тональностью, темами и сущностями")
    
    enriched = parsed.filter(col("id").isNotNull() & (length(col("text")) > 5)) \
        .withColumn("sentiment_json", sentiment_udf(col("text"))) \
        .withColumn("topics_json", topics_udf(col("text"))) \
        .withColumn("entity_json", entity_udf(col("text")))

    enriched = enriched \
        .withColumn("sentiment", from_json(col("sentiment_json"), sentiment_schema)) \
        .withColumn("topics", from_json(col("topics_json"), topics_schema)) \
        .withColumn("main_entity", from_json(col("entity_json"), entity_schema))

    logger.info(f"Настройка записи в Elasticsearch: {ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}/{ELASTICSEARCH_NEWS_INDEX}")
    
    es_query = enriched.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", f"{ELASTICSEARCH_NEWS_INDEX}/_doc") \
        .option("es.nodes", ELASTICSEARCH_HOST) \
        .option("es.port", ELASTICSEARCH_PORT) \
        .option("es.batch.size.entries", "2000") \
        .option("es.batch.write.retry.count", "3") \
        .option("es.batch.write.retry.wait", "5") \
        .option("es.nodes.wan.only", "true") \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/es") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()

    logger.info(f"Настройка записи в MongoDB: {MONGODB_URI}/{MONGODB_DB}")

    spark.conf.set("spark.sql.execution.arrow.enabled", "false")

    mongo_query = enriched.writeStream \
        .foreachBatch(write_to_mongo) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/mongo") \
        .trigger(processingTime="5 seconds") \
        .start()

    logger.info("Запуск процессора. Ожидание данных...")
    
    es_query.awaitTermination()
    mongo_query.awaitTermination()

if __name__ == "__main__":
    try:
        ensure_es_index()
        main()
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        raise
