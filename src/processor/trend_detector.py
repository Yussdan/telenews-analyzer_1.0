import logging
from typing import List
from datetime import datetime, timedelta
import pandas as pd

from ..models.message import MessageModel
from ..models.trend import TrendModel
from .clustering import TextClusterer
from ..config.settings import settings

logger = logging.getLogger(__name__)

class TrendDetector:
    def __init__(self, clusterer: TextClusterer):
        self.clusterer = clusterer
        self.time_window = timedelta(hours=settings.TREND_TIME_WINDOW_HOURS)
        self.min_messages = settings.TREND_MIN_MESSAGES
        logger.info(f"Trend detector initialized with {self.time_window} window, {self.min_messages} min messages")
    
    def detect_trends(self, messages: List[MessageModel]) -> List[TrendModel]:
        """
        Detect trends in the messages:
        1. Filter messages within time window
        2. Cluster the messages
        3. Identify growing clusters (trends)
        4. Extract trend details
        """
        if not messages:
            return []
        
        # Sort messages by date
        messages.sort(key=lambda x: x.date)
        
        # Get current time and calculate start of window
        current_time = datetime.utcnow()
        window_start = current_time - self.time_window
        
        # Filter messages within the time window
        recent_messages = [msg for msg in messages if msg.date >= window_start]
        
        if len(recent_messages) < self.min_messages:
            logger.info(f"Not enough recent messages ({len(recent_messages)}) for trend detection")
            return []
        
        # Train clustering model if not trained recently
        if not self.clusterer.last_trained or \
           (current_time - self.clusterer.last_trained) > timedelta(hours=1):
            logger.info("Retraining clustering model")
            self.clusterer.fit(recent_messages)
        
        # Get clusters for messages
        msg_to_cluster = self.clusterer.predict(recent_messages)
        
        if not msg_to_cluster:
            logger.warning("No clusters predicted")
            return []
        
        # Convert to DataFrame for easier analysis
        df = pd.DataFrame([
            {
                'telegram_id': msg.telegram_id,
                'channel_id': msg.channel_id,
                'date': msg.date,
                'cluster': msg_to_cluster.get(msg.telegram_id, -1)
            }
            for msg in recent_messages if msg.telegram_id in msg_to_cluster
        ])
        
        if df.empty:
            return []
        
        # Count messages per cluster
        cluster_counts = df['cluster'].value_counts()
        
        # Filter clusters with enough messages
        potential_trends = cluster_counts[cluster_counts >= self.min_messages].index.tolist()
        
        trends = []
        for cluster_id in potential_trends:
            # Get messages in this cluster
            cluster_msgs = df[df['cluster'] == cluster_id]
            
            # Calculate trend metrics
            first_seen = cluster_msgs['date'].min()
            last_seen = cluster_msgs['date'].max()
            
            # Calculate growth rate (messages per hour)
            time_span = (last_seen - first_seen).total_seconds() / 3600
            if time_span < 0.5:  # At least 30 minutes
                time_span = 0.5
            growth_rate = len(cluster_msgs) / time_span
            
            # Get top messages (by recency)
            top_msgs = cluster_msgs.sort_values('date', ascending=False).head(5)['telegram_id'].tolist()
            
            # Get channels
            channels = cluster_msgs['channel_id'].unique().tolist()
            
            # Get keywords for this cluster
            keywords = self.clusterer.get_cluster_keywords(cluster_id)
            
            # Create trend model
            trend = TrendModel(
                cluster_id=cluster_id,
                keywords=keywords,
                top_messages=top_msgs,
                channels=channels,
                message_count=len(cluster_msgs),
                first_seen=first_seen,
                last_seen=last_seen,
                growth_rate=growth_rate
            )
            
            trends.append(trend)
        
        # Sort trends by growth rate (descending)
        trends.sort(key=lambda x: x.growth_rate, reverse=True)
        
        logger.info(f"Detected {len(trends)} trends")
        return trends
