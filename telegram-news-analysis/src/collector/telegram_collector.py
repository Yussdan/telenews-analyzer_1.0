import asyncio
import logging
import sys
from datetime import datetime, timedelta
import os
import json

from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest
from motor.motor_asyncio import AsyncIOMotorClient
import asyncpg
from confluent_kafka import Producer
import pymongo  # Добавлено для обработки DuplicateKeyError

import config  # Локальный конфиг должен быть в конце импортов

# Настройка путей и API
SESSION_PATH = '/app/sessions/telegram_news_session.session'

class TelegramNewsCollector:
    def __init__(self):
        self._initialize_logging()
        self._setup_clients()
        self.active_channels = set()
        self.channels_last_checked = datetime.min

    def _initialize_logging(self):
        """Инициализация системы логирования"""
        logging.basicConfig(
            level=getattr(logging, config.LOG_LEVEL),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger(__name__)

    def _setup_clients(self):
        """Инициализация клиентов для внешних сервисов"""
        self._setup_kafka()
        self._setup_mongodb()
        self._setup_postgres()

    def _setup_kafka(self):
        """Настройка Kafka producer"""
        self.kafka_producer = Producer({
            'bootstrap.servers': config.KAFKA_BROKER,
            'message.max.bytes': 10_000_000,  # 10MB
            'queue.buffering.max.messages': 100_000,
            'queue.buffering.max.kbytes': 1_000_000,
            'batch.num.messages': 1_000,
            'compression.type': 'lz4'
        })
        self.kafka_topic = config.KAFKA_TOPIC

    def _setup_mongodb(self):
        """Настройка подключения к MongoDB"""
        self.mongo_client = AsyncIOMotorClient(config.MONGODB_URI)
        self.db = self.mongo_client[config.MONGODB_DB]
        self.messages_collection = self.db[config.MESSAGES_COLLECTION]
        self.channels_collection = self.db[config.CHANNELS_COLLECTION]

    def _setup_postgres(self):
        """Настройка подключения к PostgreSQL"""
        self.postgres_uri = (
            f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
            f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        )

    async def initialize_db(self):
        """Создание необходимых индексов в MongoDB"""
        await asyncio.gather(
            self.messages_collection.create_index([("message_id", 1), ("channel_id", 1)], unique=True),
            self.messages_collection.create_index([("date", -1)]),
            self.messages_collection.create_index([("text", "text")])
        )

    async def get_active_channels_from_postgres(self):
        """Получение активных каналов из PostgreSQL"""
        try:
            async with asyncpg.connect(self.postgres_uri) as conn:
                query = """
                    SELECT DISTINCT channel_name 
                    FROM user_channels uc
                    JOIN users u ON uc.user_id = u.id
                    WHERE u.is_active = TRUE
                """
                channels = await conn.fetch(query)
                return {channel['channel_name'] for channel in channels}
        except Exception as e:
            self.logger.error(f"Error fetching channels from PostgreSQL: {e}")
            return set()

    async def update_channels(self):
        """Обновление списка каналов и подписка на новые"""
        if datetime.now() - self.channels_last_checked < timedelta(minutes=1):
            return

        self.channels_last_checked = datetime.now()
        new_channels = await self.get_active_channels_from_postgres()
        
        added_channels = new_channels - self.active_channels
        removed_channels = self.active_channels - new_channels
        
        if not (added_channels or removed_channels):
            return

        self.logger.info(f"Updating channels: +{len(added_channels)}, -{len(removed_channels)}")
        
        for channel in added_channels:
            await self._join_channel(channel)
        
        self.active_channels = new_channels
        self._update_message_handler()

    async def _join_channel(self, channel_name):
        """Присоединение к каналу и сохранение информации о нем"""
        try:
            self.logger.info(f"Joining new channel: {channel_name}")
            channel_entity = await self.client.get_entity(channel_name)
            await self.client(JoinChannelRequest(channel_entity))
            
            channel_info = {
                "channel_id": channel_entity.id,
                "channel_name": channel_name,
                "title": getattr(channel_entity, "title", channel_name),
                "joined_at": datetime.now(),
                "is_active": True
            }
            
            await self.channels_collection.update_one(
                {"channel_id": channel_entity.id},
                {"$set": channel_info},
                upsert=True
            )
            
            self.logger.info(f"Successfully joined channel: {channel_name}")
        except Exception as e:
            self.logger.error(f"Failed to join channel {channel_name}: {e}")

    def _update_message_handler(self):
        """Обновление обработчика сообщений для текущих каналов"""
        self.client.remove_event_handler(self.message_handler)
        
        @self.client.on(events.NewMessage(chats=list(self.active_channels)))
        async def message_handler(event):
            await self.process_message(event)
        
        self.message_handler = message_handler

    async def process_message(self, event):
        """Обработка и сохранение сообщения"""
        try:
            message_data = await self._prepare_message_data(event)
            self.logger.debug(f"Processing message {message_data['message_id']} from {message_data['channel_name']}")

            await asyncio.gather(
                self._send_to_kafka(message_data),
                self._save_to_mongodb(message_data)
            )
            
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

    async def _prepare_message_data(self, event):
        """Подготовка данных сообщения"""
        chat = await event.get_chat()
        message = event.message
        
        return {
            "message_id": message.id,
            "channel_id": event.chat_id,
            "channel_name": getattr(chat, "username", str(event.chat_id)),
            "date": message.date,
            "text": message.text or "",
            "has_media": message.media is not None,
            "views": getattr(message, "views", None),
            "forwards": getattr(message, "forwards", None),
            "processed": False,
            "created_at": datetime.now()
        }

    async def _send_to_kafka(self, message_data):
        """Отправка сообщения в Kafka"""
        try:
            self.kafka_producer.produce(
                self.kafka_topic,
                key=str(message_data["message_id"]),
                value=json.dumps(message_data).encode('utf-8'),
                callback=self._delivery_report
            )
            self.kafka_producer.poll(0)
        except Exception as e:
            self.logger.error(f"Error sending to Kafka: {e}")

    async def _save_to_mongodb(self, message_data):
        """Сохранение сообщения в MongoDB"""
        try:
            await self.messages_collection.update_one(
                {"message_id": message_data["message_id"], "channel_id": message_data["channel_id"]},
                {"$set": message_data},
                upsert=True
            )
        except pymongo.errors.DuplicateKeyError:
            self.logger.debug(f"Message {message_data['message_id']} already exists")
        except Exception as e:
            self.logger.error(f"Error saving to MongoDB: {e}")

    def _delivery_report(self, err, msg):
        """Обработчик доставки сообщений в Kafka"""
        if err:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    async def start(self):
        """Запуск сбора данных из Telegram"""
        self.logger.info("Starting Telegram News Collector")
        
        try:
            await self.initialize_db()
            
            self.client = TelegramClient(
                SESSION_PATH,
                config.TELEGRAM_API_ID,
                config.TELEGRAM_API_HASH
            )
            
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                await self.client.start(phone=os.getenv("TELEGRAM_PHONE_NUMBER"))
                
            self.logger.info("Telegram client started and authenticated")
            
            self.active_channels = await self.get_active_channels_from_postgres()
            await self._join_channels(self.active_channels)
            self._setup_message_handler()
            
            self.logger.info(f"Collector initialized, monitoring {len(self.active_channels)} channels")
            
            while True:
                try:
                    await self.update_channels()
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    self.logger.info("Received cancellation signal")
                    break
                except Exception as e:
                    self.logger.error(f"Error in main loop: {e}")
                    await asyncio.sleep(60)
                    
        except Exception as e:
            self.logger.error(f"Error in Telegram client: {e}")
            raise

    async def _join_channels(self, channels):
        """Присоединение к списку каналов"""
        if not channels:
            self.logger.warning("No active channels found")
            return
            
        for channel in channels:
            if channel:  # Проверка на пустое значение
                await self._join_channel(channel)

    def _setup_message_handler(self):
        """Настройка обработчика сообщений"""
        @self.client.on(events.NewMessage(chats=list(self.active_channels)))
        async def message_handler(event):
            await self.process_message(event)
        
        self.message_handler = message_handler

    async def check_connections(self):
        """Проверка всех соединений"""
        results = await asyncio.gather(
            self._check_mongodb(),
            self._check_postgres(),
            self._check_telegram(),
            return_exceptions=True
        )
        
        return all(not isinstance(res, Exception) for res in results)

    async def _check_mongodb(self):
        """Проверка соединения с MongoDB"""
        try:
            await self.db.command('ping')
            return True
        except Exception as e:
            self.logger.error(f"MongoDB connection check failed: {e}")
            return False

    async def _check_postgres(self):
        """Проверка соединения с PostgreSQL"""
        try:
            conn = await asyncpg.connect(self.postgres_uri)
            await conn.close()
            return True
        except Exception as e:
            self.logger.error(f"PostgreSQL connection check failed: {e}")
            return False

    async def _check_telegram(self):
        """Проверка соединения с Telegram"""
        if not self.client or not await self.client.is_connected():
            self.logger.error("Telegram client is not connected")
            return False
        return True

    async def close_connections(self):
        """Безопасное закрытие всех соединений"""
        await asyncio.gather(
            self._close_mongodb(),
            self._close_telegram(),
            return_exceptions=True
        )

    async def _close_mongodb(self):
        """Закрытие соединения с MongoDB"""
        if self.mongo_client:
            await self.mongo_client.close()
            self.mongo_client = None
            self.logger.info("MongoDB connection closed")

    async def _close_telegram(self):
        """Закрытие соединения с Telegram"""
        if self.client and await self.client.is_connected():
            await self.client.disconnect()
            self.logger.info("Telegram client disconnected")

    async def stop(self):
        """Грациозная остановка сервиса"""
        try:
            self.logger.info("Shutting down Telegram News Collector...")
            await self.close_connections()
            self.logger.info("Service stopped gracefully")
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
            raise

    async def run(self):
        """Основной цикл работы с обработкой ошибок"""
        try:
            await self.start()
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            self.logger.critical(f"Fatal error: {e}")
        finally:
            await self.stop()

async def main():
    """Точка входа в приложение"""
    collector = TelegramNewsCollector()
    try:
        await collector.run()
    except KeyboardInterrupt:
        collector.logger.info("Application terminated by user")
    except Exception as e:
        collector.logger.critical(f"Unhandled exception: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())

