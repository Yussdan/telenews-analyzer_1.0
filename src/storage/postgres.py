import logging
import asyncpg
from typing import List, Optional
import json

from ..config.settings import settings
from ..models.trend import TrendModel

logger = logging.getLogger(__name__)

class PostgresStorage:
    def __init__(self):
        self.pool = None
    
    async def connect(self):
        """Connect to PostgreSQL database"""
        try:
            self.pool = await asyncpg.create_pool(
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                database=settings.POSTGRES_DB,
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT
            )
            await self._init_db()
            logger.info(f"Connected to PostgreSQL at {settings.POSTGRES_HOST}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            return False
    
    async def close(self):
        """Close the database connection"""
        if self.pool:
            await self.pool.close()
            logger.info("Closed PostgreSQL connection")
    
    async def _init_db(self):
        """Initialize database tables if they don't exist"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS trends (
                    id SERIAL PRIMARY KEY,
                    cluster_id INTEGER NOT NULL,
                    keywords JSONB NOT NULL,
                    top_messages JSONB NOT NULL,
                    channels JSONB NOT NULL,
                    message_count INTEGER NOT NULL,
                    first_seen TIMESTAMP WITH TIME ZONE NOT NULL,
                    last_seen TIMESTAMP WITH TIME ZONE NOT NULL,
                    growth_rate FLOAT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            ''')
            
            # Create indexes
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_trends_cluster_id ON trends(cluster_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_trends_last_seen ON trends(last_seen)')
    
    async def save_trend(self, trend: TrendModel) -> Optional[int]:
        """Save a trend to the database"""
        if not self.pool:
            logger.error("Database not connected")
            return None
        
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchval('''
                    INSERT INTO trends
                    (cluster_id, keywords, top_messages, channels, message_count, 
                     first_seen, last_seen, growth_rate)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    RETURNING id
                ''', 
                trend.cluster_id,
                json.dumps(trend.keywords),
                json.dumps(trend.top_messages),
                json.dumps(trend.channels),
                trend.message_count,
                trend.first_seen,
                trend.last_seen,
                trend.growth_rate
                )
                
                trend.id = result
                logger.info(f"Saved trend {trend.id} to database")
                return result
                
        except Exception as e:
            logger.error(f"Error saving trend to database: {str(e)}")
            return None
    
    async def get_trends(self, limit: int = 20) -> List[TrendModel]:
        """Get recent trends from the database"""
        if not self.pool:
            logger.error("Database not connected")
            return []
        
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch('''
                    SELECT id, cluster_id, keywords, top_messages, channels, 
                           message_count, first_seen, last_seen, growth_rate
                    FROM trends
                    ORDER BY last_seen DESC, growth_rate DESC
                    LIMIT $1
                ''', limit)
                
                trends = []
                for row in rows:
                    trend = TrendModel(
                        id=row['id'],
                        cluster_id=row['cluster_id'],
                        keywords=json.loads(row['keywords']),
                        top_messages=json.loads(row['top_messages']),
                        channels=json.loads(row['channels']),
                        message_count=row['message_count'],
                        first_seen=row['first_seen'],
                        last_seen=row['last_seen'],
                        growth_rate=row['growth_rate']
                    )
                    trends.append(trend)
                
                return trends
                
        except Exception as e:
            logger.error(f"Error getting trends from database: {str(e)}")
            return []
