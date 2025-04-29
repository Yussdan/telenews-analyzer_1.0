import logging
import spacy
from gensim import corpora, models

from natasha import (
    Segmenter, NewsEmbedding, NewsNERTagger, Doc
)

logger = logging.getLogger(__name__)

KEY_ENTITY_WORDS = {
    "экономика", "рынок", "политика", "наука", "валюта", "спорт", 
    "правительство", "коронавирус", "бизнес", "энергетика", "финансы", "технологии"
}

class TopicModeler:
    def __init__(self, nlp, num_topics=10):
        self.nlp = nlp
        self.num_topics = num_topics
        self.dictionary = None
        self.model = None
        self.topics = {}

        self.segmenter = Segmenter()
        self.emb = NewsEmbedding()
        self.ner_tagger = NewsNERTagger(self.emb)
        
    def preprocess_text(self, text):
        """Лемматизация и подготовка для LDA"""
        try:
            doc = self.nlp(text)
            return [
                token.lemma_.lower() 
                for token in doc
                if token.pos_ in ['NOUN', 'ADJ', 'VERB', 'ADV']
                and not token.is_stop and token.is_alpha and len(token.text) > 3
            ]
        except Exception as e:
            logger.error(f"Failed to preprocess text: {e}")
            return []

    def update_model(self, texts):
        """Обновить LDA модель по списку текстов (list[str])"""
        logger.info("Updating topic model...")

        prepared_texts = []
        for text in texts:
            if isinstance(text, str) and len(text) > 20:
                tokens = self.preprocess_text(text)
                if tokens:
                    prepared_texts.append(tokens)

        if not prepared_texts:
            logger.warning("No valid texts for topic modeling.")
            return

        try:
            self.dictionary = corpora.Dictionary(prepared_texts)
            self.dictionary.filter_extremes(no_below=1, no_above=0.95)
            if len(self.dictionary) == 0:
                logger.warning("Dictionary is empty after filtering. Skipping model training.")
                return
            corpus = [self.dictionary.doc2bow(text) for text in prepared_texts]
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
            logger.info(f"Topic model updated: {len(prepared_texts)} documents, {len(self.dictionary)} unique tokens.")

        except Exception as e:
            logger.error(f"Failed to build topic model: {e}")

    def get_topics(self, text):
        """Возвращает массив: [главная сущность, ... topWords ...]"""
        main_entity = self.extract_main_entity(text)
        topic_words = []
        tokens = self.preprocess_text(text)

        if self.model and self.dictionary and tokens:
            bow = self.dictionary.doc2bow(tokens)
            logger.info(f"get_topics Debug: tokens={tokens}, bow={bow}")
            if bow:
                try:
                    topic_distribution = self.model[bow]
                    if topic_distribution:
                        for topic_id, prob in topic_distribution:
                            if prob > 0.05:
                                words = self.topics.get(topic_id, [])
                                if words:
                                    topic_words.extend(words)
                        if not topic_words:
                            main_topic_id = max(topic_distribution, key=lambda x: x[1])[0]
                            topic_words = self.topics.get(main_topic_id, [])
                except Exception as e:
                    logger.warning(f"Failed to get LDA topics for text: {e}")
        result = []
        if main_entity:
            result.append(main_entity)
        if topic_words:
            topic_words = list(dict.fromkeys(topic_words))
            result.extend(topic_words)
        return result

    def extract_main_entity(self, text):
        """Извлечет главную сущность (PER/ORG/LOC/важное слово) через Natasha"""
        doc = Doc(text)
        doc.segment(self.segmenter)
        doc.tag_ner(self.ner_tagger)
        for entity_type in ('PER', 'ORG', 'LOC'):
            for span in doc.spans:
                if span.type == entity_type:
                    return span.text
        for kw in KEY_ENTITY_WORDS:
            if kw in text.lower():
                return kw.capitalize()
        return None
