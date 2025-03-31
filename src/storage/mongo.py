import logging
import motor.motor_asyncio
from typing import List
from datetime import datetime, timedelta

from ..config.settings import settings
from ..models.message import MessageModel

logger = logging.getLogger(__name__)

class MongoStorage:
    def __init__(self):
        self.client = None
        self.db = None
        self.messages = None
    
    async def connect(self):
        """Connect to MongoDB database"""
        try:
            # Construct connection string
            conn_str = f"mongodb://"
            if settings.MONGO_USER and settings.MONGO_PASSWORD:
                conn_str += f"{settings.MONGO_USER}:{settings.MONGO_PASSWORD}@"
            conn_str += f"{settings.MONGO_HOST}:{settings.MONGO_PORT}"
            
            self.client = motor.motor_asyncio.AsyncIOMotorClient(conn_str)
            self.db = self.client[settings.MONGO_DB]
            self.messages = self.db.messages
            
            # Create indexes
            await self.messages.create_index([("telegram_id", 1), ("channel_id", 1)], unique=True)
            await self.messages.create_index([("date", -1)])
            await self.messages.create_index([("processed", 1)])
            
            logger.info(f"Connected to MongoDB at {settings.MONGO_HOST}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            return False
    
    async def close(self):
        """Close the database connection"""
        if self.client:
            self.client.close()
            logger.info("Closed MongoDB connection")
    
    async def save_message(self, message: MessageModel) -> bool:
        """Save a message to the database, update if exists"""
        if not self.messages:
            logger.error("Database not connected")
            return False
        
        try:
            # Convert message to dict
            message_dict = message.dict()
            
            # Set up filter for upsert
            filter_dict = {
                "telegram_id": message.telegram_id,
                "channel_id": message.channel_id
            }
            
            result = await self.messages.update_one(
                filter_dict,
                {"$set": message_dict},
                upsert=True
            )
            
            success = result.modified_count > 0 or result.upserted_id is not None
            return success
            
        except Exception as e:
            logger.error(f"Error saving message to MongoDB: {str(e)}")
            return False
    
    async def get_unprocessed_messages(self, limit: int = 1000) -> List[MessageModel]:
        """Get unprocessed messages from the database"""
        if not self.messages:
            logger.error("Database not connected")
            return []
        
        try:
            cursor = self.messages.find({"processed": False}).limit(limit)
            
            messages = []
            async for doc in cursor:
                # Convert MongoDB _id to string if present
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                
                messages.append(MessageModel(**doc))
            
            return messages
            
        except Exception as e:
            logger.error(f"Error getting unprocessed messages: {str(e)}")
            return []
    
    async def get_recent_messages(self, hours: int = 24, limit: int = 10000) -> List[MessageModel]:
        """Get messages from the last X hours"""
        if not self.messages:
            logger.error("Database not connected")
            return []
        
        try:
            # Calculate the time threshold
            threshold = datetime.utcnow() - timedelta(hours=hours)
            
            cursor = self.messages.find(
                {"date": {"$gte": threshold}}
            ).sort("date", -1).limit(limit)
            
            messages = []
            async for doc in cursor:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                
                messages.append(MessageModel(**doc))
            
            return messages
            
        except Exception as e:
            logger.error(f"Error getting recent messages: {str(e)}")
            return []
