import os
import time
import logging
import sys
from datetime import datetime
import spacy
import pymongo

from db_connection import connect_to_elasticsearch, connect_to_mongodb
from topic_modeling import TopicModeler
from sentiment_analysis import SentimentAnalyzer

# MongoDB settings
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
#MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "telegram_news")
MESSAGES_COLLECTION = "messages"

# Elasticsearch settings
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
# ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "localhost")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ELASTICSEARCH_INDEX = "telegram_news"
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "20"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "30"))
CONNECTION_TIMEOUT = int(os.getenv("CONNECTION_TIMEOUT", "60"))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class NewsProcessor:
    def __init__(self):
        # Подключение к MongoDB с улучшенной логикой повторных попыток
        self.mongo_client, self.db = connect_to_mongodb(MONGODB_URI, MONGODB_DB)
        self.messages_collection = self.db[MESSAGES_COLLECTION]
        
        # Подключение к Elasticsearch с улучшенной логикой повторных попыток
        self.es = connect_to_elasticsearch(
            ELASTICSEARCH_HOST, 
            ELASTICSEARCH_PORT,
            max_retries=MAX_RETRIES,
            retry_delay=RETRY_DELAY,
            timeout=CONNECTION_TIMEOUT
        )
        
        # Создание индекса Elasticsearch, если не существует
        self._setup_elasticsearch_index()
        
        logger.info("Loading NLP models...")
        self.nlp = spacy.load("ru_core_news_md", disable=["parser", "ner"])
        self.topic_modeler = TopicModeler(self.nlp)
        self.sentiment_analyzer = SentimentAnalyzer()
        logger.info("NLP models loaded")

    def _setup_elasticsearch_index(self):
        """Create Elasticsearch index if it doesn't exist"""
        try:
            if not self.es.indices.exists(index=ELASTICSEARCH_INDEX):
                # Your existing index creation code
                self.es.indices.create(
                    index=ELASTICSEARCH_INDEX,
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

                logger.info(f"Created Elasticsearch index: {ELASTICSEARCH_INDEX}")
        except Exception as e:
            logger.error(f"Failed to create Elasticsearch index: {e}")
            raise
        
    def process_messages(self, batch_size=30):
        """Обработка непроанализированных сообщений из MongoDB"""
        # Получение необработанных сообщений
        unprocessed_messages = self.messages_collection.find(
            {"processed": False}
        ).sort("date", pymongo.ASCENDING).limit(batch_size)
        
        count = 0
        for message in unprocessed_messages:
            try:
                # Обработка текста сообщения
                text = message.get("text", "")
                if not text or len(text) < 10:  # Игнорирование слишком коротких сообщений
                    self.messages_collection.update_one(
                        {"_id": message["_id"]},
                        {"$set": {"processed": True, "skipped": True}}
                    )
                    continue
                
                # Анализ текста с помощью spaCy
                doc = self.nlp(text)
                
                # Выделение именованных сущностей
                entities = []
                for ent in doc.ents:
                    entities.append({
                        "entity": ent.text,
                        "type": ent.label_
                    })
                
                # Определение тональности текста
                sentiment = self.sentiment_analyzer.analyze(text)
                
                # Определение тем
                topics = self.topic_modeler.get_topics(text)
                
                # Создание индексируемого документа
                doc_id = f"{message['channel_id']}_{message['message_id']}"
                es_doc = {
                    "message_id": message["message_id"],
                    "channel_id": message["channel_id"],
                    "channel_name": message.get("channel_name", ""),
                    "date": message["date"],
                    "text": text,
                    "entities": entities,
                    "sentiment": sentiment,
                    "topics": topics,
                    "has_media": message.get("has_media", False),
                    "views": message.get("views", 0),
                    "forwards": message.get("forwards", 0)
                }
                
                # Try to index with retries
                max_es_retries = 5
                for retry in range(max_es_retries):
                    try:
                        # Индексация в Elasticsearch
                        self.es.index(
                            index=ELASTICSEARCH_INDEX,
                            id=doc_id,
                            body=es_doc,
                            request_timeout=30  # Add explicit timeout
                        )
                        break  # Success - exit retry loop
                    except Exception as es_error:
                        if retry < max_es_retries - 1:
                            logger.warning(f"Elasticsearch indexing failed (attempt {retry+1}/{max_es_retries}): {es_error}")
                            time.sleep(2)  # Short delay before retry
                        else:
                            # Final retry failed
                            logger.error(f"Failed to index document {doc_id} after {max_es_retries} attempts")
                            raise  # Re-raise to be caught by outer exception handler
                
                # Обновление статуса в MongoDB
                self.messages_collection.update_one(
                    {"_id": message["_id"]},
                    {"$set": {
                        "processed": True,
                        "entities": entities,
                        "sentiment": sentiment,
                        "topics": topics,
                        "processed_at": datetime.now()
                    }}
                )
                
                count += 1
                if count % 10 == 0:
                    logger.info(f"Processed {count} messages")
                
            except Exception as e:
                logger.error(f"Error processing message {message.get('_id')}: {e}")
                # Mark as error but don't set as processed so we can try again later
                try:
                    self.messages_collection.update_one(
                        {"_id": message["_id"]},
                        {"$set": {"error": str(e), "error_time": datetime.now()}}
                    )
                except Exception as update_error:
                    logger.error(f"Failed to update error status: {update_error}")
        
        return count

    
    def run(self, interval=30):
        """Запуск циклической обработки сообщений"""
        logger.info("Starting News Processor")
        try:
            while True:
                start_time = time.time()
                processed_count = self.process_messages()
                
                if processed_count > 0:
                    logger.info(f"Processed {processed_count} messages")
                else:
                    logger.info("No new messages to process")
                
                # Обновление модели тем каждый час
                if datetime.now().minute == 0:
                    logger.info("Updating topic model...")
                    self.topic_modeler.update_model(self.messages_collection)
                
                elapsed = time.time() - start_time
                sleep_time = max(0, interval - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Process interrupted by user")
        except Exception as e:
            logger.error(f"Error in processor: {e}")
        finally:
            self.mongo_client.close()
            logger.info("News Processor stopped")


def main():
    logger.info("Starting main execution")
    try:
        processor = NewsProcessor()
        processor.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()