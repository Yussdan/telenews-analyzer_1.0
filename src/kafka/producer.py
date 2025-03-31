import json
import logging
from datetime import datetime
from aiokafka import AIOKafkaProducer

from ..config.settings import settings
from ..models.trend import TrendModel

logger = logging.getLogger(__name__)

class KafkaTrendProducer:
    def __init__(self):
        self.producer = None
        self.connected = False
        
    async def start(self):
        """Initialize and start Kafka producer"""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=self._json_serializer).encode('utf-8')
            )
            await self.producer.start()
            self.connected = True
            logger.info(f"Kafka producer connected to {settings.KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            self.connected = False
    
    async def stop(self):
        """Stop Kafka producer"""
        if self.producer:
            await self.producer.stop()
            self.connected = False
            logger.info("Kafka producer stopped")
    
    async def send_trend(self, trend: TrendModel):
        """Send a trend to Kafka"""
        if not self.connected:
            logger.error("Kafka producer is not connected")
            return False
        
        try:
            # Convert to dict for serialization
            trend_dict = trend.dict()
            
            await self.producer.send_and_wait(
                settings.KAFKA_TOPIC_TRENDS, 
                trend_dict
            )
            return True
        except Exception as e:
            logger.error(f"Failed to send trend to Kafka: {str(e)}")
            return False
    
    def _json_serializer(self, obj):
        """Custom JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")
