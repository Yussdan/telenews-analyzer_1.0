import re
import logging
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from pymystem3 import Mystem

from ..models.message import MessageModel

logger = logging.getLogger(__name__)

# Download required NLTK data
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('punkt')
    nltk.download('stopwords')

class TextPreprocessor:
    def __init__(self):
        # Initialize stemmer for Russian language
        self.mystem = Mystem()
        # Get Russian stopwords
        self.stop_words = set(stopwords.words('russian'))
        # Add custom stopwords
        self.stop_words.update([
            'это', 'который', 'такой', 'весь', 'свой', 'наш', 'ваш', 'https', 'http', 'www'
        ])
        logger.info("Text preprocessor initialized")
    
    def preprocess(self, message: MessageModel) -> MessageModel:
        """
        Preprocess the text of a message:
        1. Convert to lowercase
        2. Remove URLs, mentions, hashtags
        3. Remove punctuation and special characters
        4. Tokenize
        5. Remove stopwords
        6. Lemmatize tokens
        """
        if not message.text:
            message.tokens = []
            return message
        
        # Convert to lowercase
        text = message.text.lower()
        
        # Remove URLs
        text = re.sub(r'https?://\S+|www\.\S+', '', text)
        
        # Remove Telegram mentions and hashtags
        text = re.sub(r'@\w+|#\w+', '', text)
        
        # Remove punctuation and special characters
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\d+', '', text)  # Remove numbers
        
        # Tokenize
        tokens = word_tokenize(text, language='russian')
        
        # Remove stopwords and short tokens
        tokens = [t for t in tokens if t not in self.stop_words and len(t) > 2]
        
        # Lemmatize
        lemmas = []
        for token in tokens:
            lemma_list = self.mystem.lemmatize(token)
            if lemma_list and lemma_list[0].strip():
                lemmas.append(lemma_list[0].strip())
        
        # Update the message
        message.tokens = lemmas
        message.processed = True
        
        logger.debug(f"Preprocessed message {message.telegram_id}, found {len(lemmas)} tokens")
        return message
