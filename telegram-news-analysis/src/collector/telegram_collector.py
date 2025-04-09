import asyncio
import logging
import sys
from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest
from datetime import datetime, timedelta
import config
import os
from telethon.sync import TelegramClient
from motor.motor_asyncio import AsyncIOMotorClient
import asyncpg

session_path = '/app/sessions/telegram_news_session.session'
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class TelegramNewsCollector:
    def __init__(self):
        self.client = None
        self.active_channels = set()
        self.channels_last_checked = datetime.min
        
        # Подключение к MongoDB
        self.mongo_client = AsyncIOMotorClient(config.MONGODB_URI)
        self.db = self.mongo_client[config.MONGODB_DB]
        self.messages_collection = self.db[config.MESSAGES_COLLECTION]
        self.channels_collection = self.db[config.CHANNELS_COLLECTION]

        # Подключение к PostgreSQL
        self.postgres_uri = (
            f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
            f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        )


    async def initialize_db(self):
        """Create required indexes in MongoDB"""
        await self.messages_collection.create_index([("message_id", 1), ("channel_id", 1)], unique=True)
        await self.messages_collection.create_index([("date", -1)])
        await self.messages_collection.create_index([("text", "text")])
            
    async def get_active_channels_from_postgres(self):
        """Получение активных каналов из PostgreSQL"""
        try:
            conn = await asyncpg.connect(self.postgres_uri)
            query = """
                SELECT DISTINCT channel_name 
                FROM user_channels uc
                INNER JOIN users u ON uc.user_id = u.id
                WHERE u.is_active = TRUE
            """
            channels = await conn.fetch(query)
            await conn.close()

            return {channel['channel_name'] for channel in channels}
        except Exception as e:
            logger.error(f"Error fetching channels from PostgreSQL: {e}")
            return set()
        
    async def update_channels(self):
        """Обновление списка каналов и подписка на новые"""
        try:
            # Проверяем не чаще чем раз в 5 минут
            if datetime.now() - self.channels_last_checked < timedelta(minutes=1):
                return
                
            self.channels_last_checked = datetime.now()
            
            new_channels = await self.get_active_channels_from_postgres()
            added_channels = new_channels - self.active_channels
            removed_channels = self.active_channels - new_channels
            
            if added_channels or removed_channels:
                logger.info(f"Updating channels: +{len(added_channels)}, -{len(removed_channels)}")
                
                # Обработка новых каналов
                for channel in added_channels:
                    try:
                        logger.info(f"Joining new channel: {channel}")
                        channel_entity = await self.client.get_entity(channel)
                        await self.client(JoinChannelRequest(channel_entity))
                        
                        # Сохраняем информацию о канале
                        channel_info = {
                            "channel_id": channel_entity.id,
                            "channel_name": channel,
                            "title": getattr(channel_entity, "title", channel),
                            "joined_at": datetime.now(),
                            "is_active": True
                        }
                        
                        await self.channels_collection.update_one(
                            {"channel_id": channel_entity.id},
                            {"$set": channel_info},
                            upsert=True
                        )
                        
                        logger.info(f"Successfully joined new channel: {channel}")
                    except Exception as e:
                        logger.error(f"Failed to join new channel {channel}: {e}")
                
                # Обновляем список активных каналов
                self.active_channels = new_channels
                
                # Пересоздаем обработчик событий с новым списком каналов
                self.client.remove_event_handler(self.message_handler)
                
                @self.client.on(events.NewMessage(chats=list(self.active_channels)))
                async def message_handler(event):
                    await self.process_message(event)
                
                self.message_handler = message_handler
                
        except Exception as e:
            logger.error(f"Error updating channels: {e}")
    
    async def process_message(self, event):
        """Обработка и сохранение сообщения"""
        try:
            
            # Получение данных о чате
            chat = await event.get_chat()
            channel_id = event.chat_id
            channel_name = getattr(chat, "username", str(channel_id))
            
            # Сбор данных о сообщении
            message = event.message
            message_data = {
                "message_id": message.id,
                "channel_id": channel_id,
                "channel_name": channel_name,
                "date": message.date,
                "text": message.text or "",
                "has_media": message.media is not None,
                "views": getattr(message, "views", None),
                "forwards": getattr(message, "forwards", None),
                "processed": False,
                "created_at": datetime.now()
            }
            logger.debug(f"Processing message {message.id} from {channel_name}")
            
            # Сохранение в MongoDB
            try:
                await self.messages_collection.update_one(
                    {"message_id": message.id, "channel_id": channel_id},
                    {"$set": message_data},
                    upsert=True
                )
                logger.debug(f"Saved message {message.id} from {channel_name}")
            except pymongo.errors.DuplicateKeyError:
                logger.debug(f"Message {message.id} from {channel_name} already exists")
            except Exception as e:
                logger.error(f"Error saving message to MongoDB: {e}")
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    async def start(self):
        """Запуск сбора данных из Telegram"""
        logger.info("Starting Telegram News Collector")
        
        try:
            # Initialize databases
            await self.initialize_db()
            
            self.client = TelegramClient(
                session_path,
                config.TELEGRAM_API_ID,
                config.TELEGRAM_API_HASH
            )
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                await self.client.start(phone=os.getenv("TELEGRAM_PHONE_NUMBER"))
                
            logger.info("Telegram client started and authenticated")
            
            # Initialize channels
            self.active_channels = await self.get_active_channels_from_postgres()
            await self.join_channels(self.active_channels)
            
            @self.client.on(events.NewMessage(chats=list(self.active_channels)))
            async def message_handler(event):
                await self.process_message(event)
            
            self.message_handler = message_handler
            
            logger.info(f"Collector initialized, monitoring {len(self.active_channels)} channels")
            
            while True:
                try:
                    await self.update_channels()
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    logger.info("Received cancellation signal")
                    break
                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    await asyncio.sleep(60)
                    
        except Exception as e:
            logger.error(f"Error in Telegram client: {e}")
            raise
    
    async def join_channels(self, channels):
        """Присоединиться к указанным каналам"""
        if not channels:
            logger.warning("No active channels found in PostgreSQL")
            return
            
        for channel in channels:
            if not channel:
                continue
                
            try:
                logger.info(f"Joining channel: {channel}")
                channel_entity = await self.client.get_entity(channel)
                await self.client(JoinChannelRequest(channel_entity))
                
                # Сохраняем информацию о канале
                channel_info = {
                    "channel_id": channel_entity.id,
                    "channel_name": channel,
                    "title": getattr(channel_entity, "title", channel),
                    "joined_at": datetime.now(),
                    "is_active": True,
                    "last_checked": datetime.now()
                }
                
                await self.channels_collection.update_one(
                    {"channel_id": channel_entity.id},
                    {"$set": channel_info},
                    upsert=True
                )
                
                logger.info(f"Successfully joined channel: {channel}")
            except Exception as e:
                logger.error(f"Failed to join channel {channel}: {e}")
                # Сохраняем информацию о неудачной попытке
                await self.channels_collection.update_one(
                    {"channel_name": channel},
                    {"$set": {
                        "last_error": str(e),
                        "last_attempt": datetime.now(),
                        "is_active": False
                    }},
                    upsert=True
                )

    async def check_connection(self):
        """Проверка соединений с базами данных"""
        try:
            # Проверка MongoDB
            await self.db.command('ping')
            
            # Проверка PostgreSQL
            conn = await asyncpg.connect(self.postgres_uri)
            await conn.close()
            
            # Проверка Telegram
            if not self.client or not await self.client.is_connected():
                raise ConnectionError("Telegram client is not connected")
                
            return True
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return False

    async def close_mongo_client(self):
        """Safely close the MongoDB client."""
        if self.mongo_client:
            await self.mongo_client.close()
            self.mongo_client = None

    async def stop(self):
        """Graceful shutdown of components."""
        try:
            if self.client and await self.client.is_connected():
                await self.client.disconnect()
            await self.close_mongo_client()
            logger.info("Telegram News Collector stopped gracefully")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            raise

    async def run(self):
        """Run the Telegram News Collector with error management."""
        try:
            await self.start()
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            logger.critical(f"Fatal error: {e}")
        finally:
            await self.stop()

async def main():
    collector = TelegramNewsCollector()
    try:
        await collector.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
    finally:
        await collector.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application terminated by user")
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}")
        sys.exit(1)