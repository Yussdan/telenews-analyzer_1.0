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
        try:
            doc = self.nlp(text)
            tokens = [token.lemma_.lower() for token in doc
                      if token.pos_ in ['NOUN', 'ADJ', 'VERB', 'ADV']
                      and not token.is_stop
                      and token.is_alpha
                      and len(token.text) > 3]
            return tokens
        except Exception as e:
            logger.error(f"Failed to preprocess text: {e}")
            return []

    def update_model(self, messages_collection):
        """Обновление модели тематического моделирования"""
        logger.info("Updating topic model...")

        week_ago = datetime.now() - timedelta(days=7)
        try:
            messages = list(messages_collection.find({
                "date": {"$gte": week_ago},
                "processed": True,
                "text": {"$exists": True}
            }))
        except Exception as e:
            logger.error(f"Failed to query messages: {e}")
            return

        texts = []
        for message in messages:
            text = message.get("text", "")
            if isinstance(text, str) and len(text) > 20:
                tokens = self.preprocess_text(text)
                if tokens:
                    texts.append(tokens)

        if not texts:
            logger.warning("No valid texts for topic modeling.")
            return

        try:
            self.dictionary = corpora.Dictionary(texts)
            self.dictionary.filter_extremes(no_below=2, no_above=0.6)

            if len(self.dictionary) == 0:
                logger.warning("Dictionary is empty after filtering. Skipping model training.")
                return

            corpus = [self.dictionary.doc2bow(text) for text in texts]
            if all(len(doc) == 0 for doc in corpus):
                logger.warning("All documents are empty after BoW conversion.")
                return

            self.model = models.LdaModel(
                corpus=corpus,
                id2word=self.dictionary,
                num_topics=self.num_topics,
                passes=10,
                alpha='auto',
                eta='auto',
                random_state=42
            )

            self.topics = {
                topic_id: [term for term, _ in self.model.show_topic(topic_id, topn=5)]
                for topic_id in range(self.num_topics)
            }

            logger.info(f"Topic model updated: {len(texts)} documents, {len(self.dictionary)} unique tokens.")

        except Exception as e:
            logger.error(f"Failed to build topic model: {e}")

    def get_topics(self, text):
        """Получение тем для текста"""
        if not self.model or not self.dictionary:
            logger.warning("Topic model or dictionary is not initialized.")
            return []

        tokens = self.preprocess_text(text)
        if not tokens:
            return []

        bow = self.dictionary.doc2bow(tokens)
        if not bow:
            return []

        try:
            topic_distribution = self.model[bow]
            relevant_topics = []
            for topic_id, prob in topic_distribution:
                if prob > 0.2:
                    terms = self.topics.get(topic_id, [])
                    if terms:
                        relevant_topics.append(" ".join(terms))
            return relevant_topics
        except Exception as e:
            logger.error(f"Failed to get topics for text: {e}")
            return []
