import logging
from datetime import datetime, timedelta
from gensim import corpora, models

logger = logging.getLogger(__name__)

class TopicModeler:
    def __init__(self, nlp, num_topics=10):
        self.nlp = nlp
        self.num_topics = num_topics
        self.dictionary = None
        self.model = None
        self.topics = {}
    
    def preprocess_text(self, text):
        """Предобработка текста для тематического моделирования"""
        doc = self.nlp(text)
        tokens = [token.lemma_.lower() for token in doc 
                 if (token.pos_ in ['NOUN', 'ADJ', 'VERB', 'ADV']) 
                 and (not token.is_stop) 
                 and (len(token.text) > 3)]
        return tokens
    
    def update_model(self, messages_collection):
        """Обновление модели тематического моделирования"""
        logger.info("Updating topic model...")
        
        # Получение сообщений за последнюю неделю
        week_ago = datetime.now() - timedelta(days=7)
        messages = messages_collection.find({
            "date": {"$gte": week_ago},
            "processed": True,
            "text": {"$exists": True}
        })
        
        # Предобработка текстов
        texts = []
        for message in messages:
            text = message.get("text", "")
            if text and len(text) > 20:  # Игнорирование слишком коротких сообщений
                tokens = self.preprocess_text(text)
                if tokens:
                    texts.append(tokens)
        
        if not texts:
            logger.warning("No texts available for topic modeling")
            return
        
        # Создание словаря и корпуса
        self.dictionary = corpora.Dictionary(texts)
        self.dictionary.filter_extremes(no_below=5, no_above=0.7)
        corpus = [self.dictionary.doc2bow(text) for text in texts]
        
        # Обучение модели LDA
        self.model = models.LdaModel(
            corpus=corpus, 
            id2word=self.dictionary,
            num_topics=self.num_topics,
            passes=10,
            alpha='auto',
            eta='auto'
        )
        
        # Сохранение названий тем
        self.topics = {}
        for topic_id in range(self.num_topics):
            topic_terms = self.model.show_topic(topic_id, topn=5)
            self.topics[topic_id] = [term for term, _ in topic_terms]
        
        logger.info(f"Topic model updated with {len(texts)} documents and {self.num_topics} topics")
    
    def get_topics(self, text):
        """Получение тем для текста"""
        if not self.model or not self.dictionary:
            return []
            
        # Предобработка текста
        tokens = self.preprocess_text(text)
        if not tokens:
            return []
            
        # Преобразование в Bag-of-Words
        bow = self.dictionary.doc2bow(tokens)
        
        # Получение распределения тем
        topic_distribution = self.model[bow]
        
        # Возвращаем только самые релевантные темы (с вероятностью > 0.2)
        relevant_topics = []
        for topic_id, prob in topic_distribution:
            if prob > 0.2:
                if topic_id in self.topics:
                    topic_terms = " ".join(self.topics[topic_id])
                    relevant_topics.append(topic_terms)
        
        return relevant_topics
