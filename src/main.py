import asyncio
import logging
import uvicorn
from fastapi import FastAPI

from src.config.settings import settings
from src.kafka.consumer import KafkaMessageConsumer
from src.kafka.producer import KafkaTrendProducer
from src.storage.postgres import PostgresStorage
from src.storage.mongo import MongoStorage
from src.processor.text_preprocessor import TextPreprocessor
from src.processor.clustering import TextClusterer
from src.processor.trend_detector import TrendDetector
from src.api import routes

# Configure logging
logging.basicConfig(
    level=settings.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

app = FastAPI(title="TelTrends Processor API")

# Add routes
app.include_router(routes.router, prefix="/api/v1")

# Initialize components
kafka_consumer = KafkaMessageConsumer()
kafka_producer = KafkaTrendProducer()
postgres_storage = PostgresStorage()
mongo_storage = MongoStorage()
text_preprocessor = TextPreprocessor()
text_clusterer = TextClusterer()
trend_detector = TrendDetector(text_clusterer)

# Set storage for routes
routes.postgres = postgres_storage

# Processing control
processing_task = None
trend_detection_task = None
is_running = False

async def process_message(message):
    """Process a single message from Kafka"""
    try:
        # Preprocess the text
        processed_message = text_preprocessor.preprocess(message)
        
        # Save to MongoDB
        await mongo_storage.save_message(processed_message)
        
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")

async def detect_trends_periodically():
    """Periodically detect trends from processed messages"""
    while is_running:
        try:
            # Get recent messages for trend detection
            messages = await mongo_storage.get_recent_messages(
                hours=settings.TREND_TIME_WINDOW_HOURS,
                limit=10000
            )
            
            if messages:
                logger.info(f"Detecting trends from {len(messages)} messages")
                
                # Detect trends
                trends = trend_detector.detect_trends(messages)
                
                # Save and publish trends
                for trend in trends:
                    # Save to PostgreSQL
                    trend_id = await postgres_storage.save_trend(trend)
                    if trend_id:
                        trend.id = trend_id
                        # Publish to Kafka
                        await kafka_producer.send_trend(trend)
                
                logger.info(f"Detected and published {len(trends)} trends")
            else:
                logger.info("No messages available for trend detection")
            
        except Exception as e:
            logger.error(f"Error in trend detection: {str(e)}")
        
        # Wait before next detection cycle
        await asyncio.sleep(60 * 15)  # Run every 15 minutes

@app.on_event("startup")
async def startup_event():
    """Initialize components on startup"""
    global processing_task, trend_detection_task, is_running
    
    try:
        # Connect to databases
        pg_success = await postgres_storage.connect()
        mongo_success = await mongo_storage.connect()
        
        if not pg_success or not mongo_success:
            logger.error("Failed to connect to databases, stopping startup")
            return
        
        # Start Kafka producer
        await kafka_producer.start()
        
        # Start Kafka consumer
        await kafka_consumer.start(process_message)
        
        # Start background tasks
        is_running = True
        trend_detection_task = asyncio.create_task(detect_trends_periodically())
        
        logger.info("All components initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize components: {str(e)}")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown"""
    global processing_task, trend_detection_task, is_running
    
    # Stop background tasks
    is_running = False
    
    if trend_detection_task:
        trend_detection_task.cancel()
        try:
            await trend_detection_task
        except asyncio.CancelledError:
            pass
    
    # Stop Kafka consumer and producer
    await kafka_consumer.stop()
    await kafka_producer.stop()
    
    # Close database connections
    await postgres_storage.close()
    await mongo_storage.close()
    
    logger.info("All components shut down successfully")

def main():
    """Main entry point for the application"""
    uvicorn.run(
        "src.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=True
    )

if __name__ == "__main__":
    main()
