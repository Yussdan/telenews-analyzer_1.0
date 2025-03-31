import logging
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from typing import List, Dict
from datetime import datetime

from ..models.message import MessageModel
from ..config.settings import settings

logger = logging.getLogger(__name__)

class TextClusterer:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(max_features=5000)
        self.kmeans = KMeans(
            n_clusters=settings.CLUSTER_COUNT,
            random_state=42,
            n_init=10
        )
        self.last_trained = None
        self.feature_names = []
        logger.info(f"Text clusterer initialized with {settings.CLUSTER_COUNT} clusters")
    
    def _combine_tokens(self, messages: List[MessageModel]) -> List[str]:
        """Combine tokens into documents for vectorization"""
        return [' '.join(msg.tokens) for msg in messages if msg.tokens]
    
    def fit(self, messages: List[MessageModel]) -> bool:
        """Fit the vectorizer and clustering model on the messages"""
        if not messages:
            logger.warning("No messages to fit the model")
            return False
        
        try:
            # Combine tokens into documents
            documents = self._combine_tokens(messages)
            if not documents:
                logger.warning("No valid documents for fitting")
                return False
            
            # Fit TF-IDF vectorizer
            X = self.vectorizer.fit_transform(documents)
            
            # Get feature names for later analysis
            self.feature_names = self.vectorizer.get_feature_names_out()
            
            # Fit KMeans
            self.kmeans.fit(X)
            
            self.last_trained = datetime.now()
            logger.info(f"Successfully trained on {len(documents)} documents")
            return True
            
        except Exception as e:
            logger.error(f"Error during model fitting: {str(e)}")
            return False
    
    def predict(self, messages: List[MessageModel]) -> Dict[int, int]:
        """Predict clusters for new messages"""
        if not messages or not self.last_trained:
            return {}
        
        try:
            # Combine tokens into documents
            documents = self._combine_tokens(messages)
            if not documents:
                return {}
            
            # Transform using existing vectorizer
            X = self.vectorizer.transform(documents)
            
            # Predict clusters
            clusters = self.kmeans.predict(X)
            
            # Map message IDs to cluster IDs
            msg_to_cluster = {}
            for i, msg in enumerate(messages):
                if i < len(clusters):
                    msg_to_cluster[msg.telegram_id] = int(clusters[i])
            
            logger.info(f"Predicted clusters for {len(msg_to_cluster)} messages")
            return msg_to_cluster
            
        except Exception as e:
            logger.error(f"Error during prediction: {str(e)}")
            return {}
    
    def get_cluster_keywords(self, cluster_id: int, top_n: int = 10) -> List[str]:
        """Get top keywords that define a cluster"""
        if not self.last_trained or not self.feature_names.size:
            return []
        
        try:
            # Get cluster center
            center = self.kmeans.cluster_centers_[cluster_id]
            
            # Get indices of top terms
            indices = np.argsort(center)[-top_n:]
            
            # Get the corresponding terms
            keywords = [self.feature_names[i] for i in indices]
            
            return keywords
            
        except Exception as e:
            logger.error(f"Error getting cluster keywords: {str(e)}")
            return []
