import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
import os
import json
import traceback
import signal

from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest
from motor.motor_asyncio import AsyncIOMotorClient
import asyncpg
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import pymongo

# Настройка путей и API
SESSION_PATH = '/app/sessions/telegram_news_session.session'
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telegram_news")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Улучшенная конфигурация с учетом новых компонентов системы
class Config:
    # Telegram API
    TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
    TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
    TELEGRAM_PHONE_NUMBER = os.getenv("TELEGRAM_PHONE_NUMBER", "")
    
    # MongoDB
    MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
    MONGODB_DB = os.getenv("MONGODB_DB", "telegram_news")
    MESSAGES_COLLECTION = os.getenv("MESSAGES_COLLECTION", "messages")
    CHANNELS_COLLECTION = os.getenv("CHANNELS_COLLECTION", "channels")
    CHANNEL_STATS_COLLECTION = "channel_stats"
    ENTITY_MENTIONS_COLLECTION = "entity_mentions"
    
    # PostgreSQL
    POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "telenews")
    
    # Kafka
    KAFKA_BROKERS = KAFKA_BROKERS
    KAFKA_TOPIC = KAFKA_TOPIC
    KAFKA_BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", "1000"))
    
    # Application settings
    CHANNEL_UPDATE_INTERVAL = int(os.getenv("CHANNEL_UPDATE_INTERVAL", "10"))
    MESSAGE_BATCH_SIZE = int(os.getenv("MESSAGE_BATCH_SIZE", "100"))
    HEALTHCHECK_INTERVAL = int(os.getenv("HEALTHCHECK_INTERVAL", "180"))
    
    # New tables and collections
    TRACKED_ENTITIES_TABLE = "tracked_entities"
    
    @classmethod
    def get_postgres_uri(cls):
        return (f"postgresql://{cls.POSTGRES_USER}:{cls.POSTGRES_PASSWORD}@"
                f"{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}")

config = Config()

class TelegramNewsCollector:
    def __init__(self):
        self._initialize_logging()
        self._setup_clients()
        self.active_channels = set()
        self.channels_last_checked = datetime.min
        self.client = None
        self.message_handler = None
        self.running = False
        self.total_messages_collected = 0
        self.last_stats_report = datetime.now()
        self.tracked_entities = {}
        self.last_entity_check = datetime.min

    def _initialize_logging(self):
        """Инициализация системы логирования"""
        logging.basicConfig(
            level=getattr(logging, LOG_LEVEL),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Logger initialized")

    def _setup_clients(self):
        """Инициализация клиентов для внешних сервисов"""
        try:
            self._setup_kafka()
            self._setup_mongodb()
            self._setup_postgres()
            self.logger.info("All clients initialized successfully")
        except Exception as e:
            self.logger.error(f"Error setting up clients: {e}")
            raise

    def _setup_kafka(self):
        """Настройка Kafka producer и создание топика при необходимости"""
        try:
            self.kafka_producer = Producer({
                'bootstrap.servers': config.KAFKA_BROKERS,
                'message.max.bytes': 10_000_000,
                'queue.buffering.max.messages': 100_000,
                'queue.buffering.max.kbytes': 1_000_000,
                'batch.num.messages': config.KAFKA_BATCH_SIZE,
                'compression.type': 'lz4',
                'retries': 5,
                'retry.backoff.ms': 500,
                'acks': 'all'
            })
            self.kafka_topic = config.KAFKA_TOPIC

            admin_client = AdminClient({'bootstrap.servers': config.KAFKA_BROKERS})
            topics = admin_client.list_topics(timeout=10).topics
            if self.kafka_topic not in topics:
                new_topic = NewTopic(self.kafka_topic, num_partitions=3, replication_factor=1)
                fs = admin_client.create_topics([new_topic])
                for topic, f in fs.items():
                    try:
                        f.result()
                        self.logger.info(f"Created Kafka topic: {topic}")
                    except Exception as e:
                        self.logger.warning(f"Failed to create topic {topic}: {e}")
            else:
                self.logger.info(f"Kafka topic '{self.kafka_topic}' already exists")

            self.logger.info(f"Kafka producer configured for topic: {self.kafka_topic}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    def _setup_mongodb(self):
        """Настройка подключения к MongoDB"""
        try:
            self.mongo_client = AsyncIOMotorClient(
                config.MONGODB_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000
            )
            self.db = self.mongo_client[config.MONGODB_DB]
            
            # Коллекции в MongoDB
            self.messages_collection = self.db[config.MESSAGES_COLLECTION]
            self.channels_collection = self.db[config.CHANNELS_COLLECTION]
            self.channel_stats_collection = self.db[config.CHANNEL_STATS_COLLECTION]
            self.entity_mentions_collection = self.db[config.ENTITY_MENTIONS_COLLECTION]
            
            self.logger.info(f"MongoDB client connected to {config.MONGODB_URI}")
        except Exception as e:
            self.logger.error(f"Failed to initialize MongoDB client: {e}")
            raise

    def _setup_postgres(self):
        """Настройка подключения к PostgreSQL"""
        try:
            self.postgres_uri = config.get_postgres_uri()
            self.postgres_pool = None
            self.logger.info(f"PostgreSQL URI configured")
        except Exception as e:
            self.logger.error(f"Failed to configure PostgreSQL URI: {e}")
            raise

    async def initialize_db(self):
        """Создание необходимых индексов в MongoDB и структур в PostgreSQL"""
        try:
            self.logger.info("Initializing MongoDB indexes...")
            await asyncio.gather(
                # Индексы для сообщений
                self.messages_collection.create_index([("message_id", 1), ("channel_id", 1)], unique=True),
                self.messages_collection.create_index([("date", -1)]),
                self.messages_collection.create_index([("text", "text")]),
                self.messages_collection.create_index([("channel_name", 1)]),
                self.messages_collection.create_index([("processed", 1)]),
                
                # Индексы для каналов
                self.channels_collection.create_index([("channel_id", 1)], unique=True),
                self.channels_collection.create_index([("channel_name", 1)]),
                self.channels_collection.create_index([("is_active", 1)]),
                
                # Индексы для статистики каналов
                self.channel_stats_collection.create_index([("channel_id", 1), ("date", 1)], unique=True),
                self.channel_stats_collection.create_index([("date", -1)]),
                
                # Индексы для упоминаний сущностей
                self.entity_mentions_collection.create_index([("entity_id", 1), ("message_id", 1)], unique=True),
                self.entity_mentions_collection.create_index([("entity_id", 1), ("timestamp", -1)]),
                self.entity_mentions_collection.create_index([("entity_name", 1)])
            )
            self.logger.info("MongoDB indexes created successfully")
            
            # Инициализация структур в PostgreSQL
            await self._init_postgres_tables()
            
        except Exception as e:
            self.logger.error(f"Failed to initialize databases: {e}")
            raise

    async def _init_postgres_tables(self):
        """Создание необходимых таблиц в PostgreSQL, если их нет"""
        try:
            if not self.postgres_pool:
                self.postgres_pool = await asyncpg.create_pool(self.postgres_uri)
            
            async with self.postgres_pool.acquire() as conn:
                # Проверяем существование таблицы отслеживаемых сущностей
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = $1
                    )
                """, config.TRACKED_ENTITIES_TABLE)
                
                if not table_exists:
                    self.logger.info(f"Creating {config.TRACKED_ENTITIES_TABLE} table in PostgreSQL")
                    await conn.execute(f"""
                        CREATE TABLE IF NOT EXISTS {config.TRACKED_ENTITIES_TABLE} (
                            id SERIAL PRIMARY KEY,
                            name VARCHAR(100) NOT NULL,
                            synonyms TEXT[],
                            description TEXT,
                            category VARCHAR(50),
                            created_at TIMESTAMP DEFAULT NOW(),
                            updated_at TIMESTAMP DEFAULT NOW(),
                            user_id INTEGER,
                            active BOOLEAN DEFAULT TRUE
                        )
                    """)
                    
                    # Индекс для быстрого поиска активных сущностей
                    await conn.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_{config.TRACKED_ENTITIES_TABLE}_active
                        ON {config.TRACKED_ENTITIES_TABLE} (active)
                    """)
                    
                    self.logger.info(f"Table {config.TRACKED_ENTITIES_TABLE} created")
                
                # Проверяем существование таблицы упоминаний сущностей
                entity_mentions_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'entity_mentions'
                    )
                """)
                
                if not entity_mentions_exists:
                    self.logger.info("Creating entity_mentions table in PostgreSQL")
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS entity_mentions (
                            id SERIAL PRIMARY KEY,
                            entity_id INTEGER,
                            message_id VARCHAR(50),
                            channel_id VARCHAR(50),
                            mention_text TEXT,
                            sentiment JSONB,
                            context TEXT,
                            timestamp TIMESTAMP,
                            relevance_score FLOAT
                        )
                    """)
                    
                    # Индексы для упоминаний сущностей
                    await conn.execute("""
                        CREATE INDEX IF NOT EXISTS idx_entity_mentions_entity_id
                        ON entity_mentions (entity_id);
                        
                        CREATE INDEX IF NOT EXISTS idx_entity_mentions_timestamp
                        ON entity_mentions (timestamp);
                    """)
                    
                    self.logger.info("Table entity_mentions created")
                
        except Exception as e:
            self.logger.error(f"Error initializing PostgreSQL tables: {e}")
            raise


    async def get_active_channels_from_postgres(self):
        """Получение активных каналов из PostgreSQL"""
        try:
            self.logger.debug("Fetching active channels from PostgreSQL")
            if not self.postgres_pool:
                self.postgres_pool = await asyncpg.create_pool(self.postgres_uri)
                
            async with self.postgres_pool.acquire() as conn:
                query = """
                    SELECT DISTINCT channel_name 
                    FROM user_channels uc
                    JOIN users u ON uc.user_id = u.id
                    WHERE u.is_active = TRUE AND channel_name IS NOT NULL AND channel_name != ''
                """
                channels = await conn.fetch(query)
                channel_set = {channel['channel_name'] for channel in channels if channel['channel_name']}
                self.logger.info(f"Retrieved {len(channel_set)} active channels from PostgreSQL")
                return channel_set
        except Exception as e:
            self.logger.error(f"Error fetching channels from PostgreSQL: {e}")
            return self.active_channels or set()

    async def update_tracked_entities(self):
        """Обновление списка отслеживаемых сущностей из PostgreSQL"""
        if datetime.now() - self.last_entity_check < timedelta(minutes=5):
            return
            
        try:
            self.logger.info("Updating tracked entities from PostgreSQL")
            self.last_entity_check = datetime.now()
            
            if not self.postgres_pool:
                self.postgres_pool = await asyncpg.create_pool(self.postgres_uri)
                
            async with self.postgres_pool.acquire() as conn:
                query = f"""
                    SELECT id, name, synonyms, category 
                    FROM {config.TRACKED_ENTITIES_TABLE}
                    WHERE active = TRUE
                """
                entities = await conn.fetch(query)
                
                # Обновляем словарь отслеживаемых сущностей
                new_entities = {}
                for entity in entities:
                    entity_id = entity['id']
                    new_entities[entity_id] = {
                        "id": entity_id,
                        "name": entity['name'],
                        "synonyms": entity['synonyms'] if entity['synonyms'] else [],
                        "category": entity['category']
                    }
                
                # Логируем изменения
                added = set(new_entities.keys()) - set(self.tracked_entities.keys())
                removed = set(self.tracked_entities.keys()) - set(new_entities.keys())
                
                if added or removed:
                    self.logger.info(f"Tracked entities updated: +{len(added)}, -{len(removed)}")
                    
                self.tracked_entities = new_entities
                self.logger.info(f"Now tracking {len(self.tracked_entities)} entities")
                
        except Exception as e:
            self.logger.error(f"Error updating tracked entities: {e}")

    async def update_channels(self, immediate=False):
        """Обновление списка каналов и подписка на новые"""
        try:
            if not immediate and datetime.now() - self.channels_last_checked < timedelta(seconds=config.CHANNEL_UPDATE_INTERVAL):
                return

            self.logger.info("Updating channel list")
            self.channels_last_checked = datetime.now()
            
            new_channels = await self.get_active_channels_from_postgres()
            
            added_channels = new_channels - self.active_channels
            removed_channels = self.active_channels - new_channels
            
            if not (added_channels or removed_channels):
                self.logger.debug("No channel changes detected")
                return

            self.logger.info(f"Channel changes: +{len(added_channels)}, -{len(removed_channels)}")
            
            # Join new channels
            join_tasks = []
            for channel in added_channels:
                join_tasks.append(self._join_channel(channel))
            
            if join_tasks:
                await asyncio.gather(*join_tasks, return_exceptions=True)
            
            self.active_channels = new_channels
            
            # Update message handler with new channel list
            if self.client and self.client.is_connected():
                self._update_message_handler()
                self.logger.info("Message handler updated with new channel list")
            else:
                self.logger.warning("Client not connected, couldn't update message handler")
                
        except Exception as e:
            self.logger.error(f"Error updating channels: {e}")
            self.logger.debug(traceback.format_exc())

    async def _join_channel(self, channel_name):
        """Присоединение к каналу и сохранение информации о нем"""
        if not channel_name or not channel_name.strip():
            self.logger.warning("Attempted to join empty channel name, skipping")
            return
            
        try:
            self.logger.info(f"Joining channel: {channel_name}")
            channel_entity = await self.client.get_entity(channel_name)
            await self.client(JoinChannelRequest(channel_entity))
            
            channel_info = {
                "channel_id": channel_entity.id,
                "channel_name": channel_name,
                "title": getattr(channel_entity, "title", channel_name),
                "joined_at": datetime.now(),
                "is_active": True,
                "subscribers": getattr(channel_entity, "participants_count", None),
                "description": getattr(channel_entity, "about", None),
                "last_updated": datetime.now()
            }
            
            await self.channels_collection.update_one(
                {"channel_id": channel_entity.id},
                {"$set": channel_info},
                upsert=True
            )
            
            self.logger.info(f"Successfully joined channel: {channel_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to join channel {channel_name}: {e}")
            # If this is a critical error that we should track for later
            await self.channels_collection.update_one(
                {"channel_name": channel_name},
                {"$set": {
                    "channel_name": channel_name,
                    "is_active": False,
                    "error": str(e),
                    "last_error_at": datetime.now()
                }},
                upsert=True
            )
            return False

    def _update_message_handler(self):
        """Обновление обработчика сообщений для текущих каналов"""
        if not self.client:
            self.logger.error("Cannot update message handler: client not initialized")
            return
            
        if self.message_handler:
            self.client.remove_event_handler(self.message_handler)
            self.logger.debug("Removed previous message handler")
        
        channel_list = list(self.active_channels)
        if not channel_list:
            self.logger.warning("No active channels to monitor")
            return
            
        @self.client.on(events.NewMessage(chats=channel_list))
        async def message_handler(event):
            await self.process_message(event)
        
        self.message_handler = message_handler
        self.logger.info(f"Set up message handler for {len(channel_list)} channels")

    async def process_message(self, event):
        """Обработка и сохранение сообщения"""
        try:
            # Get message data
            message_data = await self._prepare_message_data(event)
            
            # Skip empty messages
            if not message_data.get("text") and not message_data.get("has_media"):
                self.logger.debug(f"Skipping empty message {message_data['message_id']}")
                return
                
            self.logger.debug(f"Processing message {message_data['message_id']} from {message_data['channel_name']}")

            # Send to Kafka and save to MongoDB concurrently
            await asyncio.gather(
                self._send_to_kafka(message_data),
                self._save_to_mongodb(message_data)
            )
            
            # Check for entity mentions
            if message_data.get("text") and self.tracked_entities:
                await self._detect_entity_mentions(message_data)
            
            # Update statistics
            self.total_messages_collected += 1
            if self.total_messages_collected % 100 == 0 or datetime.now() - self.last_stats_report > timedelta(minutes=10):
                self.logger.info(f"Total messages collected: {self.total_messages_collected}")
                self.last_stats_report = datetime.now()
            
        except Exception as e:
            chat_id = getattr(event, "chat_id", "unknown")
            msg_id = getattr(event.message, "id", "unknown") if hasattr(event, "message") else "unknown"
            self.logger.error(f"Error processing message {msg_id} from {chat_id}: {e}")
            self.logger.debug(traceback.format_exc())

    async def _prepare_message_data(self, event):
        """Подготовка данных сообщения"""
        try:
            chat = await event.get_chat()
            message = event.message
            
            message_date = message.date
            if message_date:
                if message_date.tzinfo is None:
                    message_date = message_date.replace(tzinfo=timezone.utc)
                
            # Extract more information if available
            media_type = None
            if message.media:
                media_type = type(message.media).__name__
            
            # Basic structure alignment with Elasticsearch mapping
            return {
                "id": str(message.id),  # Make sure it's a string for consistency
                "message_id": message.id,
                "channel_id": event.chat_id,
                "channel_name": getattr(chat, "username", str(event.chat_id)),
                "date": message_date.isoformat() if message_date else datetime.now(timezone.utc).isoformat(),
                "text": message.text or "",
                "has_media": message.media is not None,
                "media_type": media_type,
                "views": getattr(message, "views", 0),
                "forwards": getattr(message, "forwards", 0),
                "replies": getattr(message.replies, "replies", 0) if getattr(message, "replies", None) else 0,
                "processed": False,
                "created_at": datetime.now(timezone.utc).isoformat(),
                
                # Добавляем поля для совместимости с новым индексом Elasticsearch
                "entities": [],
                "sentiment": {"label": "unknown", "score": 0.5, "method": "pending"},
                "topics": []
            }
        except Exception as e:
            self.logger.error(f"Error preparing message data: {e}")
            self.logger.debug(traceback.format_exc())
            return {
                "id": str(getattr(event.message, "id", "unknown")),
                "message_id": getattr(event.message, "id", 0),
                "channel_id": getattr(event, "chat_id", 0),
                "channel_name": "unknown",
                "date": datetime.now(datetime.timezone.utc).isoformat(),
                "text": getattr(event.message, "text", "") or "",
                "has_media": False,
                "views": 0,
                "forwards": 0,
                "processed": False,
                "created_at": datetime.now(datetime.timezone.utc).isoformat(),
                "error": str(e),
                
                # Добавляем поля для совместимости с новым индексом Elasticsearch
                "entities": [],
                "sentiment": {"label": "error", "score": 0.0, "method": "error"},
                "topics": []
            }

    async def _send_to_kafka(self, message_data):
        """Отправка сообщения в Kafka"""
        try:
            # Make a copy of the message data to ensure we don't modify the original
            kafka_data = message_data.copy()
            
            # Convert to JSON and produce to Kafka
            message_json = json.dumps(kafka_data, ensure_ascii=False)
            self.kafka_producer.produce(
                self.kafka_topic,
                key=str(kafka_data["message_id"]),
                value=message_json.encode('utf-8'),
                callback=self._delivery_report
            )
            self.kafka_producer.poll(0)
        except Exception as e:
            self.logger.error(f"Error sending to Kafka: {e}")
            self.logger.debug(traceback.format_exc())

    async def _save_to_mongodb(self, message_data):
        """Сохранение сообщения в MongoDB"""
        try:
            # Make a copy for MongoDB
            mongo_data = message_data.copy()
            
            # Преобразование времени
            if "date" in mongo_data and isinstance(mongo_data["date"], str):
                try:
                    mongo_data["date"] = datetime.fromisoformat(mongo_data["date"].replace('Z', '+00:00'))
                except ValueError:
                    pass
                    
            if "created_at" in mongo_data and isinstance(mongo_data["created_at"], str):
                try:
                    mongo_data["created_at"] = datetime.fromisoformat(mongo_data["created_at"].replace('Z', '+00:00'))
                except ValueError:
                    pass
            
            await self.messages_collection.update_one(
                {"message_id": mongo_data["message_id"], "channel_id": mongo_data["channel_id"]},
                {"$set": mongo_data},
                upsert=True
            )
            
            # Update channel last message timestamp
            await self.channels_collection.update_one(
                {"channel_id": mongo_data["channel_id"]},
                {"$set": {"last_message_at": datetime.now()}},
                upsert=False
            )
            
            # Update daily channel statistics
            await self._update_channel_stats(mongo_data)
            
        except pymongo.errors.DuplicateKeyError:
            self.logger.debug(f"Message {message_data['message_id']} already exists")
        except Exception as e:
            self.logger.error(f"Error saving to MongoDB: {e}")
            self.logger.debug(traceback.format_exc())

    async def _update_channel_stats(self, message_data):
        """Обновление статистики по каналу"""
        try:
            channel_id = message_data.get("channel_id")
            channel_name = message_data.get("channel_name")
            date = message_data.get("date")
            
            if not channel_id or not date:
                return
                
            # Если дата строка, преобразуем в datetime
            if isinstance(date, str):
                try:
                    date = datetime.fromisoformat(date.replace('Z', '+00:00'))
                except ValueError:
                    date = datetime.now(timezone.utc)
            
            # Используем только дату (без времени)
            date_key = date.date().isoformat()
            
            await self.channel_stats_collection.update_one(
                {"channel_id": channel_id, "date": date_key},
                {
                    "$set": {
                        "channel_name": channel_name,
                        "last_updated": datetime.now()
                    },
                    "$inc": {
                        "message_count": 1,
                        "media_count": 1 if message_data.get("has_media") else 0
                    }
                },
                upsert=True
            )
        except Exception as e:
            self.logger.error(f"Error updating channel stats: {e}")

    async def _detect_entity_mentions(self, message_data):
        """Обнаружение упоминаний отслеживаемых сущностей в сообщении"""
        if not self.tracked_entities or not message_data.get("text"):
            return
            
        try:
            text = message_data["text"].lower()
            message_id = message_data["message_id"]
            channel_id = message_data["channel_id"]
            channel_name = message_data["channel_name"]
            date = message_data.get("date")
            
            # Преобразуем строковую дату в datetime при необходимости
            if isinstance(date, str):
                try:
                    date = datetime.fromisoformat(date.replace('Z', '+00:00'))
                except ValueError:
                    date = datetime.now(timezone.utc)
            
            # Ищем упоминания сущностей
            mentions = []
            for entity_id, entity in self.tracked_entities.items():
                # Проверяем основное название и синонимы
                variants = [entity["name"]] + entity.get("synonyms", [])
                
                for variant in variants:
                    if variant.lower() in text:
                        # Определяем контекст упоминания (100 символов до и после)
                        position = text.find(variant.lower())
                        start = max(0, position - 100)
                        end = min(len(text), position + len(variant) + 100)
                        mention_text = text[start:end]
                        
                        mention = {
                            "entity_id": entity_id,
                            "entity_name": entity["name"],
                            "message_id": message_id,
                            "channel_id": channel_id,
                            "channel_name": channel_name,
                            "mention_text": mention_text,
                            "context": text,
                            "timestamp": date,
                            # Базовый сентимент - будет заменен в процессоре
                            "sentiment": {
                                "label": "pending",
                                "score": 0.5
                            },
                            "relevance_score": 1.0
                        }
                        mentions.append(mention)
                        
                        # После первого совпадения переходим к следующей сущности
                        break
            
            # Сохраняем найденные упоминания
            if mentions:
                # Сохраняем в MongoDB для дальнейшей обработки
                await self.entity_mentions_collection.insert_many(mentions)
                self.logger.debug(f"Found {len(mentions)} entity mentions in message {message_id}")
                
                # Добавляем информацию об упоминаниях в сообщение
                entity_names = [mention["entity_name"] for mention in mentions]
                await self.messages_collection.update_one(
                    {"message_id": message_id, "channel_id": channel_id},
                    {"$set": {"detected_entities": entity_names}}
                )
                
        except Exception as e:
            self.logger.error(f"Error detecting entity mentions: {e}")

    def _delivery_report(self, err, msg):
        """Обработчик доставки сообщений в Kafka"""
        if err:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    async def start(self):
        """Запуск сбора данных из Telegram"""
        if self.running:
            self.logger.warning("Collector is already running")
            return
            
        self.logger.info("Starting Telegram News Collector")
        self.running = True
        
        try:
            # Initialize PostgreSQL connection pool
            if not self.postgres_pool:
                self.postgres_pool = await asyncpg.create_pool(
                    self.postgres_uri,
                    min_size=2,
                    max_size=10
                )
                self.logger.info("PostgreSQL connection pool created")
            
            # Initialize database
            await self.initialize_db()
            
            # Create and connect Telegram client
            self.client = TelegramClient(
                SESSION_PATH,
                config.TELEGRAM_API_ID,
                config.TELEGRAM_API_HASH,
                device_model="Telegram News Collector",
                system_version="1.0",
                app_version="1.0"
            )
            
            await self.client.connect()
            
            # Check authorization and handle if needed
            if not await self.client.is_user_authorized():
                self.logger.info("Client not authorized, starting authentication")
                phone_number = config.TELEGRAM_PHONE_NUMBER
                if not phone_number:
                    raise ValueError("TELEGRAM_PHONE_NUMBER environment variable not set")
                    
                await self.client.start(phone=phone_number)
                
            self.logger.info("Telegram client started and authenticated")
            
            # Get and join channels
            self.active_channels = await self.get_active_channels_from_postgres()
            if self.active_channels:
                await self._join_channels(self.active_channels)
                self._update_message_handler()
                self.logger.info(f"Monitoring {len(self.active_channels)} channels")
            else:
                self.logger.warning("No active channels found, collector will check periodically")
            
            # Load tracked entities
            await self.update_tracked_entities()
            
            # Set up scheduled tasks
            self.update_channels_task = asyncio.create_task(self._periodic_channel_update())
            self.update_entities_task = asyncio.create_task(self._periodic_entity_update())
            self.health_check_task = asyncio.create_task(self._periodic_health_check())
            
            self.logger.info("Telegram News Collector started successfully")
            
        except Exception as e:
            self.logger.error(f"Critical error in Telegram client: {e}")
            self.logger.debug(traceback.format_exc())
            self.running = False
            raise

    async def _periodic_channel_update(self):
        """Периодическое обновление списка каналов"""
        while self.running:
            try:
                await self.update_channels()
                await asyncio.sleep(config.CHANNEL_UPDATE_INTERVAL)
            except asyncio.CancelledError:
                self.logger.info("Channel update task cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in channel update task: {e}")
                await asyncio.sleep(30)

    async def _periodic_entity_update(self):
        """Периодическое обновление списка отслеживаемых сущностей"""
        while self.running:
            try:
                await self.update_tracked_entities()
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                self.logger.info("Entity update task cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in entity update task: {e}")
                await asyncio.sleep(60)

    async def _periodic_health_check(self):
        """Периодическая проверка состояния сервисов"""
        while self.running:
            try:
                await self.check_connections()
                await asyncio.sleep(config.HEALTHCHECK_INTERVAL)
            except asyncio.CancelledError:
                self.logger.info("Health check task cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in health check task: {e}")
                await asyncio.sleep(60)

    async def _join_channels(self, channels):
        """Присоединение к списку каналов"""
        if not channels:
            self.logger.warning("No active channels found to join")
            return
        
        self.logger.info(f"Joining {len(channels)} channels")    
        join_tasks = []
        for channel in channels:
            if channel and channel.strip():  # Ensure valid channel name
                join_tasks.append(self._join_channel(channel))
        
        if join_tasks:
            results = await asyncio.gather(*join_tasks, return_exceptions=True)
            success_count = sum(1 for r in results if r is True)
            self.logger.info(f"Successfully joined {success_count} out of {len(join_tasks)} channels")

    async def check_connections(self):
        """Проверка всех соединений"""
        self.logger.info("Checking all connections")
        results = await asyncio.gather(
            self._check_mongodb(),
            self._check_postgres(),
            self._check_telegram(),
            return_exceptions=True
        )
        
        success = all(not isinstance(res, Exception) and res for res in results)
        if success:
            self.logger.info("All connections are healthy")
        else:
            self.logger.warning("One or more connections are unhealthy")
            
        return success

    async def _check_mongodb(self):
        """Проверка соединения с MongoDB"""
        try:
            # Simple ping command to check MongoDB connection
            await self.db.command('ping')
            return True
        except Exception as e:
            self.logger.error(f"MongoDB connection check failed: {e}")
            return False

    async def _check_postgres(self):
        """Проверка соединения с PostgreSQL"""
        try:
            if not self.postgres_pool:
                return False
                
            async with self.postgres_pool.acquire() as conn:
                await conn.execute("SELECT 1")  # Execute a simple query
            return True
        except Exception as e:
            self.logger.error(f"PostgreSQL connection check failed: {e}")
            return False

    async def _check_telegram(self):
        """Проверка соединения с Telegram"""
        if not self.client:
            self.logger.error("Telegram client not initialized")
            return False
            
        try:
            # Метод is_connected() не асинхронный, используем без await
            if not self.client.is_connected():
                self.logger.error("Telegram client is not connected")
                # Attempt to reconnect
                await self.client.connect()
                if not self.client.is_connected():  # Также без await здесь
                    return False
            
            # Try to actually use the connection
            me = await self.client.get_me()
            return me is not None
        except Exception as e:
            self.logger.error(f"Telegram connection check failed: {e}")
            # Try to reconnect on failure
            try:
                await self.client.disconnect()
                await asyncio.sleep(1)
                await self.client.connect()
                self.logger.info("Telegram client reconnected")
            except Exception:
                pass
            return False


    async def close_connections(self):
        """Безопасное закрытие всех соединений"""
        self.logger.info("Closing all connections")
        
        # Cancel periodic tasks
        for task_name in ["update_channels_task", "update_entities_task", "health_check_task"]:
            task = getattr(self, task_name, None)
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        # Flush Kafka messages
        try:
            if hasattr(self, 'kafka_producer'):
                self.logger.info("Flushing Kafka producer...")
                self.kafka_producer.flush(timeout=30)
                self.logger.info("Kafka producer flushed")
        except Exception as e:
            self.logger.error(f"Error flushing Kafka producer: {e}")
            
        # Close other connections
        await asyncio.gather(
            self._close_mongodb(),
            self._close_postgres(),
            self._close_telegram(),
            return_exceptions=True
        )
        
        self.logger.info("All connections closed")

    async def _close_mongodb(self):
        """Закрытие соединения с MongoDB"""
        if hasattr(self, 'mongo_client') and self.mongo_client:
            try:
                self.mongo_client.close()
                self.mongo_client = None
                self.db = None
                self.messages_collection = None
                self.channels_collection = None
                self.channel_stats_collection = None
                self.entity_mentions_collection = None
                self.logger.info("MongoDB connection closed")
            except Exception as e:
                self.logger.error(f"Error closing MongoDB connection: {e}")

    async def _close_postgres(self):
        """Закрытие соединения с PostgreSQL"""
        if hasattr(self, 'postgres_pool') and self.postgres_pool:
            try:
                await self.postgres_pool.close()
                self.postgres_pool = None
                self.logger.info("PostgreSQL connection pool closed")
            except Exception as e:
                self.logger.error(f"Error closing PostgreSQL connection: {e}")

    async def _close_telegram(self):
        """Закрытие соединения с Telegram"""
        if hasattr(self, 'client') and self.client:
            try:
                if self.client.is_connected():  # Без await
                    await self.client.disconnect()
                    self.logger.info("Telegram client disconnected")
                self.client = None
                self.message_handler = None
            except Exception as e:
                self.logger.error(f"Error disconnecting Telegram client: {e}")

    async def stop(self):
        """Грациозная остановка сервиса"""
        if not self.running:
            self.logger.info("Collector is not running")
            return
            
        try:
            self.logger.info("Shutting down Telegram News Collector...")
            self.running = False
            await self.close_connections()
            self.logger.info("Service stopped gracefully")
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
            self.logger.debug(traceback.format_exc())

    async def run(self):
        """Основной цикл работы с обработкой ошибок"""
        try:
            await self.start()

            while self.running:
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            self.logger.critical(f"Fatal error: {e}")
            self.logger.debug(traceback.format_exc())
        finally:
            await self.stop()

    async def generate_statistics(self):
        """Генерация обобщенной статистики для мониторинга"""
        try:
            if not self.mongodb_available:
                return {"error": "MongoDB not available"}
                
            # Количество отслеживаемых каналов
            channels_count = await self.channels_collection.count_documents({"is_active": True})
            
            # Количество сообщений за последние 24 часа
            yesterday = datetime.now() - timedelta(days=1)
            recent_messages = await self.messages_collection.count_documents({"date": {"$gte": yesterday}})
            
            # Количество отслеживаемых сущностей
            entity_count = len(self.tracked_entities)
            
            # Количество упоминаний сущностей за последние 24 часа
            recent_mentions = await self.entity_mentions_collection.count_documents({"timestamp": {"$gte": yesterday}})
            
            # Статистика по каналам (топ-5 по количеству сообщений)
            pipeline = [
                {"$match": {"date": {"$gte": yesterday}}},
                {"$group": {"_id": "$channel_name", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 5}
            ]
            top_channels = await self.messages_collection.aggregate(pipeline).to_list(length=5)
            
            return {
                "timestamp": datetime.now().isoformat(),
                "total_messages_collected": self.total_messages_collected,
                "active_channels": channels_count,
                "tracked_entities": entity_count,
                "messages_last_24h": recent_messages,
                "entity_mentions_last_24h": recent_mentions,
                "top_channels": [{"channel": c["_id"], "messages": c["count"]} for c in top_channels],
                "telegram_connected": self.client and self.client.is_connected(),
                "kafka_enabled": hasattr(self, "kafka_producer"),
                "running_since": self.start_time.isoformat() if hasattr(self, "start_time") else None
            }
        except Exception as e:
            self.logger.error(f"Error generating statistics: {e}")
            return {"error": str(e)}


async def main():
    """Точка входа в приложение"""
    collector = TelegramNewsCollector()

    loop = asyncio.get_running_loop()
    for signal_name in ('SIGINT', 'SIGTERM'):
        try:
            loop.add_signal_handler(
                getattr(signal, signal_name),
                lambda: asyncio.create_task(collector.stop())
            )
        except (NotImplementedError, ImportError):
            pass
    
    try:
        await collector.run()
    except KeyboardInterrupt:
        collector.logger.info("Application terminated by user")
    except Exception as e:
        collector.logger.critical(f"Unhandled exception: {e}")
        collector.logger.debug(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    try:
        import signal
    except ImportError:
        pass
        
    asyncio.run(main())
