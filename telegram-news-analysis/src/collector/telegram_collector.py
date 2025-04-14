import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
import os
import json
import traceback

from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest
from motor.motor_asyncio import AsyncIOMotorClient
import asyncpg
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import pymongo

import config

# Настройка путей и API
SESSION_PATH = '/app/sessions/telegram_news_session.session'
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telegram_news")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")  # Ensure "kafka" resolves correctly

class TelegramNewsCollector:
    def __init__(self):
        self._initialize_logging()
        self._setup_clients()
        self.active_channels = set()
        self.channels_last_checked = datetime.min
        self.client = None
        self.message_handler = None
        self.running = False

    def _initialize_logging(self):
        """Инициализация системы логирования"""
        logging.basicConfig(
            level=getattr(logging, config.LOG_LEVEL),
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
                'bootstrap.servers': KAFKA_BROKERS,
                'message.max.bytes': 10_000_000,
                'queue.buffering.max.messages': 100_000,
                'queue.buffering.max.kbytes': 1_000_000,
                'batch.num.messages': 1_000,
                'compression.type': 'lz4',
                'retries': 5,
                'retry.backoff.ms': 500,
                'acks': 'all'
            })
            self.kafka_topic = KAFKA_TOPIC

            admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKERS})
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
            self.messages_collection = self.db[config.MESSAGES_COLLECTION]
            self.channels_collection = self.db[config.CHANNELS_COLLECTION]
            self.logger.info(f"MongoDB client connected to {config.MONGODB_URI}")
        except Exception as e:
            self.logger.error(f"Failed to initialize MongoDB client: {e}")
            raise

    def _setup_postgres(self):
        """Настройка подключения к PostgreSQL"""
        try:
            self.postgres_uri = (
                f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
                f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
            )
            self.logger.info(f"PostgreSQL URI configured")
        except Exception as e:
            self.logger.error(f"Failed to configure PostgreSQL URI: {e}")
            raise

    async def initialize_db(self):
        """Создание необходимых индексов в MongoDB"""
        try:
            self.logger.info("Initializing MongoDB indexes...")
            await asyncio.gather(
                self.messages_collection.create_index([("message_id", 1), ("channel_id", 1)], unique=True),
                self.messages_collection.create_index([("date", -1)]),
                self.messages_collection.create_index([("text", "text")]),
                self.channels_collection.create_index([("channel_id", 1)], unique=True),
                self.channels_collection.create_index([("channel_name", 1)])
            )
            self.logger.info("MongoDB indexes created successfully")
        except Exception as e:
            self.logger.error(f"Failed to create MongoDB indexes: {e}")
            raise

    async def get_active_channels_from_postgres(self):
        """Получение активных каналов из PostgreSQL"""
        try:
            self.logger.debug("Fetching active channels from PostgreSQL")
            conn = await asyncpg.connect(self.postgres_uri)
            try:
                query = """
                    SELECT DISTINCT channel_name 
                    FROM user_channels uc
                    JOIN users u ON uc.user_id = u.id
                    WHERE u.is_active = TRUE
                """
                channels = await conn.fetch(query)
                channel_set = {channel['channel_name'] for channel in channels if channel['channel_name']}
                self.logger.info(f"Retrieved {len(channel_set)} active channels from PostgreSQL")
                return channel_set
            finally:
                await conn.close()
        except Exception as e:
            self.logger.error(f"Error fetching channels from PostgreSQL: {e}")
            return self.active_channels or set()

    async def update_channels(self, immediate=False):
        """Обновление списка каналов и подписка на новые"""
        try:
            if not immediate and datetime.now() - self.channels_last_checked < timedelta(minutes=1):
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
                "created_at": datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error preparing message data: {e}")
            self.logger.debug(traceback.format_exc())
            # Return a minimal valid message to avoid further errors
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
                "error": str(e)
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
            
        except pymongo.errors.DuplicateKeyError:
            self.logger.debug(f"Message {message_data['message_id']} already exists")
        except Exception as e:
            self.logger.error(f"Error saving to MongoDB: {e}")
            self.logger.debug(traceback.format_exc())

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
                phone_number = os.getenv("TELEGRAM_PHONE_NUMBER")
                if not phone_number:
                    raise ValueError("TELEGRAM_PHONE_NUMBER environment variable not set")
                    
                await self.client.start(phone=phone_number)
                
            self.logger.info("Telegram client started and authenticated")
            
            # Get and join channels
            self.active_channels = await self.get_active_channels_from_postgres()
            if self.active_channels:
                await self._join_channels(self.active_channels)
                self._setup_message_handler()
                self.logger.info(f"Monitoring {len(self.active_channels)} channels")
            else:
                self.logger.warning("No active channels found, collector will check periodically")
            
            # Main loop
            while self.running:
                try:
                    await self.update_channels()
                    
                    # Healthcheck MongoDB and reconnect if needed
                    if not await self._check_mongodb():
                        self.logger.warning("MongoDB connection lost, reconnecting...")
                        self._setup_mongodb()
                    
                    # Sleep for the update interval
                    await asyncio.sleep(60)
                    
                except asyncio.CancelledError:
                    self.logger.info("Received cancellation signal")
                    break
                except Exception as e:
                    self.logger.error(f"Error in main loop: {e}")
                    self.logger.debug(traceback.format_exc())
                    await asyncio.sleep(30)  # Shorter sleep on error
                    
        except Exception as e:
            self.logger.error(f"Critical error in Telegram client: {e}")
            self.logger.debug(traceback.format_exc())
            self.running = False
            raise
        finally:
            self.running = False

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

    def _setup_message_handler(self):
        """Настройка обработчика сообщений"""
        if not self.client:
            self.logger.error("Cannot set up message handler: client not initialized")
            return
            
        if not self.active_channels:
            self.logger.warning("No active channels to monitor")
            return
            
        @self.client.on(events.NewMessage(chats=list(self.active_channels)))
        async def message_handler(event):
            await self.process_message(event)
        
        self.message_handler = message_handler
        self.logger.info(f"Message handler set up for {len(self.active_channels)} channels")

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
            conn = await asyncpg.connect(self.postgres_uri)
            await conn.execute("SELECT 1")  # Execute a simple query
            await conn.close()
            return True
        except Exception as e:
            self.logger.error(f"PostgreSQL connection check failed: {e}")
            return False

    async def _check_telegram(self):
        """Проверка соединения с Telegram"""
        if not self.client:
            self.logger.error("Telegram client not initialized")
            return False
            
        if not await self.client.is_connected():
            self.logger.error("Telegram client is not connected")
            return False
            
        try:
            # Try to actually use the connection
            me = await self.client.get_me()
            return me is not None
        except Exception as e:
            self.logger.error(f"Telegram connection check failed: {e}")
            return False

    async def close_connections(self):
        """Безопасное закрытие всех соединений"""
        self.logger.info("Closing all connections")
        
        try:
            # Kafka doesn't need async closing
            if hasattr(self, 'kafka_producer'):
                try:
                    self.kafka_producer.flush(10)  # Wait up to 10 seconds
                    self.logger.info("Kafka producer flushed")
                except Exception as e:
                    self.logger.error(f"Error flushing Kafka producer: {e}")
        except Exception as e:
            self.logger.error(f"Error closing Kafka: {e}")
            
        # Close other connections
        await asyncio.gather(
            self._close_mongodb(),
            self._close_telegram(),
            return_exceptions=True
        )

    async def _close_mongodb(self):
        """Закрытие соединения с MongoDB"""
        if hasattr(self, 'mongo_client') and self.mongo_client:
            try:
                await self.mongo_client.close()
                self.mongo_client = None
                self.db = None
                self.messages_collection = None
                self.channels_collection = None
                self.logger.info("MongoDB connection closed")
            except Exception as e:
                self.logger.error(f"Error closing MongoDB connection: {e}")

    async def _close_telegram(self):
        """Закрытие соединения с Telegram"""
        if hasattr(self, 'client') and self.client:
            try:
                if await self.client.is_connected():
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
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            self.logger.critical(f"Fatal error: {e}")
            self.logger.debug(traceback.format_exc())
        finally:
            await self.stop()

async def main():
    """Точка входа в приложение"""
    collector = TelegramNewsCollector()
    
    # Handle shutdown signals properly
    loop = asyncio.get_running_loop()
    for signal_name in ('SIGINT', 'SIGTERM'):
        try:
            loop.add_signal_handler(
                getattr(signal, signal_name),
                lambda: asyncio.create_task(collector.stop())
            )
        except (NotImplementedError, ImportError):
            # Signals not supported on this platform
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
