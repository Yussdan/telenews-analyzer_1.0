import json
import logging
import asyncio
from typing import Callable
from datetime import datetime
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from ..config.settings import settings
from ..models.message import MessageModel

logger = logging.getLogger(__name__)

class KafkaMessageConsumer:
    def __init__(self):
        self.consumer = None
        self.running = False
        self.processing_task = None
    
    async def start(self, message_handler: Callable[[MessageModel], None]):
        """Start consuming messages from Kafka"""
        try:
            self.consumer = AIOKafkaConsumer(
                settings.KAFKA_TOPIC_MESSAGES,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                group_id=settings.KAFKA_CONSUMER_GROUP,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            await self.consumer.start()
            logger.info(f"Started Kafka consumer for topic {settings.KAFKA_TOPIC_MESSAGES}")
            
            self.running = True
            self.processing_task = asyncio.create_task(
                self._process_messages(message_handler)
            )
            
        except Exception as e:
            logger.error(f"Failed to start Kafka consumer: {str(e)}")
            raise
    
    async def stop(self):
        """Stop the Kafka consumer"""
        self.running = False
        
        if self.processing_task:
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass
        
        if self.consumer:
            await self.consumer.stop()
            logger.info("Kafka consumer stopped")
    
    async def _process_messages(self, message_handler: Callable[[MessageModel], None]):
        """Process incoming messages from Kafka"""
        try:
            async for message in self.consumer:
                if not self.running:
                    break
                
                try:
                    # Convert dictionary to MessageModel
                    data = message.value
                    
                    # Parse datetime string to datetime object
                    if 'date' in data and isinstance(data['date'], str):
                        data['date'] = datetime.fromisoformat(data['date'].replace('Z', '+00:00'))
                    
                    msg = MessageModel(**data)
                    
                    # Process the message
                    await message_handler(msg)
                    
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                
        except KafkaError as e:
            logger.error(f"Kafka error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error in message processing: {str(e)}")
