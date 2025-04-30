import logging
import gc
from datetime import datetime, timedelta
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
import spacy
import langdetect
from pymongo import MongoClient
import os
import requests
import random
import re
from typing import List, Dict, Any, Optional, Union
import traceback

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TopicModeler:
    """Расширенная система тематического моделирования с гибридным подходом,
    использующая комбинацию кластеризации и LLM для выявления тем."""
    
    def __init__(self, 
                 nlp=None, 
                 mongodb_uri=None, 
                 min_topics=5, 
                 max_topics=20, 
                 embedding_model_name="paraphrase-multilingual-mpnet-base-v2",
                 llm_url=None,
                 use_local_llm=True):
        """
        Инициализация модуля тематического моделирования.
        
        Args:
            nlp: Предварительно загруженная модель spaCy или None
            mongodb_uri: URI для подключения к MongoDB или None
            min_topics: Минимальное количество тем
            max_topics: Максимальное количество тем
            embedding_model_name: Имя модели для эмбеддингов
        """
        logger.info("Initializing TopicModeler with hybrid approach")
        
        # Параметры моделирования
        self.min_topics = min_topics
        self.max_topics = max_topics
        self.is_trained = False
        self.topics = {}
        self.labeled_topics = {}
        self.topic_hierarchy = {}
        self.topic_entities = {}
        self.last_update_time = None
        self.version = "1.0.0"
        
        # Параметры LLM
        self.use_local_llm = use_local_llm

        self.llm_url = llm_url or os.getenv("LOCAL_LLM_URL", "http://llm-service:8000/generate")
        
        self.llm_available = self._check_llm_availability()
        
        # Подключение к MongoDB, если предоставлен URI
        if mongodb_uri:
            try:
                self.mongodb_client = MongoClient(mongodb_uri)
                self.db = self.mongodb_client.telegram_news
                self.mongodb_available = True
                logger.info("Connected to MongoDB successfully")
            except Exception as e:
                logger.error(f"MongoDB connection error: {e}")
                self.mongodb_client = None
                self.db = None
                self.mongodb_available = False
        else:
            self.mongodb_client = None
            self.db = None
            self.mongodb_available = False
            logger.warning("MongoDB URI not provided, will work in local mode")
        
        # Инициализация моделей NLP
        if nlp:
            self.nlp = nlp
            logger.info(f"Using provided spaCy model: {nlp.meta['name']}")
        else:
            try:
                logger.info("Loading Russian spaCy model")
                self.nlp = spacy.load("ru_core_news_md")
            except:
                logger.warning("ru_core_news_md not available, downloading...")
                try:
                    spacy.cli.download("ru_core_news_md")
                    self.nlp = spacy.load("ru_core_news_md")
                except Exception as e:
                    logger.error(f"Failed to load spaCy model: {e}")
                    self.nlp = None

        # Кэширование для предобработки текста и запросов к LLM
        self.preprocessing_cache = {}
        self.llm_cache = {}
        self.cache_hits = 0
        self.cache_size_limit = 10000
        
        # Загрузка модели трансформеров для эмбеддингов
        try:
            logger.info(f"Loading SentenceTransformer: {embedding_model_name}")
            self.sentence_model = SentenceTransformer(embedding_model_name)
            
            # Использование GPU, если доступен
            self.device = "cuda" if self.sentence_model.device.type == "cuda" else "cpu"
            logger.info(f"SentenceTransformer will use {self.device}")
            
            # Ограничение для экономии памяти на CPU
            if self.device == "cpu":
                os.environ["TOKENIZERS_PARALLELISM"] = "false"
            
            # Инициализация TF-IDF векторизатора для извлечения ключевых слов
            self.vectorizer = TfidfVectorizer(
                max_features=1000,
                min_df=2,
                max_df=0.85
            )
            
            self.model_available = True
            logger.info("Topic modeling components initialized successfully")
        except Exception as e:
            logger.error(f"Failed to load transformer model: {e}")
            traceback.print_exc()
            self.sentence_model = None
            self.vectorizer = None
            self.model_available = False
        
        # Загрузка сохраненной модели, если есть
        if self.mongodb_available:
            self._load_model_from_db()
    
    def _check_llm_availability(self) -> bool:
        """Проверка доступности LLM"""
        try:
            logger.info(f"Checking LLM availability at {self.llm_url}")
            
            test_data = {
                "prompt": "Тестовый запрос",
                "max_tokens": 10
            }
            response = requests.post(self.llm_url, json=test_data, timeout=240)
    
            if response.status_code == 200:
                logger.info("LLM is available for topic extraction")
                return True
            else:
                logger.warning(f"LLM returned status code {response.status_code}")
                return False
        except Exception as e:
            logger.warning(f"LLM is not available: {e}")
            return False
    
    def _load_model_from_db(self) -> bool:
        """Загрузка сохраненной модели и метаданных из базы данных"""
        if not self.mongodb_available:
            return False
        
        try:
            # Проверяем наличие сохраненной модели
            metadata = self.db.topic_model_metadata.find_one({"name": "last_update"})
            if not metadata:
                logger.info("No saved topic model found in the database")
                return False
            
            # Загружаем информацию о темах
            topic_docs = list(self.db.topic_model.find({}))
            if not topic_docs:
                logger.info("No topics found in the database")
                return False
            
            # Восстанавливаем словари тем
            self.topics = {}
            self.labeled_topics = {}
            self.topic_entities = {}
            
            for doc in topic_docs:
                topic_id = doc.get("topic_id")
                if topic_id is not None:
                    self.topics[topic_id] = doc.get("terms", [])
                    self.labeled_topics[topic_id] = {
                        "label": doc.get("label", ""),
                        "terms": doc.get("terms", [])
                    }
                    # Загрузка связанных сущностей, если есть
                    entities = doc.get("entities", [])
                    if entities:
                        self.topic_entities[topic_id] = entities
            
            # Восстанавливаем иерархию тем
            hierarchy_docs = list(self.db.topic_hierarchy.find({}))
            if hierarchy_docs:
                self.topic_hierarchy = {}
                for doc in hierarchy_docs:
                    group_id = doc.get("group_id")
                    topics = doc.get("topics", [])
                    if group_id is not None and topics:
                        self.topic_hierarchy[group_id] = topics
            
            self.last_update_time = metadata.get("timestamp")
            self.is_trained = True
            
            logger.info(f"Loaded topic model from database. Last update: {self.last_update_time}, topics: {len(self.topics)}")
            return True
        except Exception as e:
            logger.error(f"Error loading model from database: {e}")
            return False
    
    def preprocess_text(self, text: str, language: Optional[str] = None) -> List[str]:
        """
        Улучшенная предобработка текста с поддержкой разных языков и
        извлечением именованных сущностей.
        
        Args:
            text: Текст для предобработки
            language: Код языка (если известен) или None для автоопределения
            
        Returns:
            Список токенов после предобработки
        """
        if not text or len(text) < 10:
            return []
        
        # Проверка кэша
        cache_key = text[:100]  # Кэшируем по первым 100 символам
        if cache_key in self.preprocessing_cache:
            self.cache_hits += 1
            return self.preprocessing_cache[cache_key]
        
        # Если нет модели spaCy, возвращаем пустой список
        if not self.nlp:
            return []
        
        try:
            # Определение языка, если не указан
            if language is None:
                try:
                    language = langdetect.detect(text)
                except:
                    language = "ru"  # Fallback to Russian
            
            # Обработка текста через spaCy
            doc = self.nlp(text[:10000])  # Ограничиваем длину для экономии памяти
            
            # Получение токенов
            tokens = [
                token.lemma_.lower() for token in doc 
                if (token.pos_ in ['NOUN', 'ADJ', 'VERB', 'PROPN'] and 
                    token.is_alpha and 
                    len(token.text) > 3 and
                    not token.is_stop)
            ]
            
            # Извлечение именованных сущностей
            named_entities = []
            for entity in doc.ents:
                if entity.label_ in ['PER', 'ORG', 'GPE', 'LOC', 'EVENT', 'PRODUCT']:
                    entity_text = entity.text.lower()
                    if len(entity_text.split()) > 1:
                        named_entities.append(entity_text)
                    else:
                        entity_lemma = entity.lemma_.lower()
                        if len(entity_lemma) > 3:
                            named_entities.append(entity_lemma)
            
            # Объединение токенов и сущностей
            all_tokens = tokens + named_entities
            result = list(set(all_tokens))  # Удаление дубликатов
            
            # Сохранение в кэш
            self.preprocessing_cache[cache_key] = result
            
            # Очистка кэша, если он слишком большой
            if len(self.preprocessing_cache) > self.cache_size_limit:
                # Оставляем только половину записей
                keys_to_keep = list(self.preprocessing_cache.keys())[-self.cache_size_limit//2:]
                self.preprocessing_cache = {k: self.preprocessing_cache[k] for k in keys_to_keep}
                
            return result
        except Exception as e:
            logger.error(f"Error preprocessing text: {e}")
            return []
    
    def update_model(self, messages: Optional[List[Union[Dict[str, Any], str]]] = None, force: bool = False) -> bool:
        """
        Обновление модели тематического моделирования с использованием гибридного подхода.
        
        Args:
            messages: Список сообщений (словари с полем 'text' или строки) или None для загрузки из БД
            force: Принудительное обновление, игнорируя время последнего обновления
            
        Returns:
            True, если модель успешно обновлена, иначе False
        """
        current_time = datetime.utcnow()
        
        # Проверка времени последнего обновления
        if not force and self.last_update_time:
            hours_since_update = (current_time - self.last_update_time).total_seconds() / 3600
            if hours_since_update < 12:  # Обновляем не чаще раза в 12 часов
                logger.info(f"Topic model is up to date (last update: {hours_since_update:.1f} hours ago)")
                return False
        
        # Получение текстов для обучения
        texts = []
        cleaned_messages = []
        
        # Если сообщения не переданы, пытаемся загрузить из БД
        if not messages and self.mongodb_available:
            try:
                # Получаем сообщения за последние 7 дней
                one_week_ago = current_time - timedelta(days=7)
                db_messages = list(self.db.messages.find(
                    {"date": {"$gte": one_week_ago}},
                    {"text": 1, "channel_id": 1, "channel_name": 1, "date": 1, "views": 1}
                ).limit(5000))  # Ограничиваем для экономии памяти
                
                if db_messages:
                    for msg in db_messages:
                        if msg.get("text") and len(msg.get("text", "")) >= 50:
                            texts.append(msg.get("text"))
                            cleaned_messages.append(msg)
                    
                    logger.info(f"Loaded {len(texts)} messages from MongoDB for topic modeling")
                else:
                    logger.warning("No messages found in the database")
            except Exception as e:
                logger.error(f"Error loading messages from database: {e}")
        
        # Если переданы сообщения, извлекаем тексты
        elif messages:
            for msg in messages:
                if isinstance(msg, dict) and "text" in msg and len(msg.get("text", "")) >= 50:
                    texts.append(msg["text"])
                    cleaned_messages.append(msg)
                elif isinstance(msg, str) and len(msg) >= 50:
                    texts.append(msg)
                    cleaned_messages.append({"text": msg})
            
            logger.info(f"Using {len(texts)} provided messages for topic modeling")
        
        # Если недостаточно текстов, выходим
        if len(texts) < 50:
            logger.warning(f"Not enough texts for topic modeling: {len(texts)} (need at least 50)")
            return False
        
        # Выбор подхода к тематическому моделированию в зависимости от объема данных
        try:
            logger.info(f"Applying hybrid topic modeling approach for {len(texts)} texts")
            
            # Очистка памяти перед началом работы
            gc.collect()
            
            # ПОДХОД 1: Для больших объемов данных (> 1000 сообщений)
            if len(texts) > 1000:
                logger.info("Using large-scale approach (clustering with LLM enrichment)")
                topics_info = self._large_scale_topic_modeling(texts, cleaned_messages)
            
            # ПОДХОД 2: Для средних объемов данных (100-1000 сообщений)
            elif len(texts) > 100:
                logger.info("Using medium-scale approach (LLM with clustering)")
                topics_info = self._medium_scale_topic_modeling(texts, cleaned_messages)
            
            # ПОДХОД 3: Для малых объемов данных (< 100 сообщений)
            else:
                logger.info("Using small-scale approach (direct LLM topic extraction)")
                topics_info = self._small_scale_topic_modeling(texts, cleaned_messages)
            
            # Обработка результатов
            if not topics_info or len(topics_info) < 2:
                logger.warning(f"Topic modeling resulted in too few topics: {len(topics_info) if topics_info else 0}")
                return False
                
            # Сохранение результатов
            self.topics = {}
            self.labeled_topics = {}
            self.topic_entities = {}
            
            for topic_id, topic_data in topics_info.items():
                self.topics[topic_id] = topic_data.get("terms", [])
                self.labeled_topics[topic_id] = {
                    "label": topic_data.get("label", f"Тема {topic_id}"),
                    "terms": topic_data.get("terms", [])
                }
                if "entities" in topic_data and topic_data["entities"]:
                    self.topic_entities[topic_id] = topic_data["entities"]
            
            # Построение иерархии тем
            if len(self.topics) >= 5:
                self.topic_hierarchy = self.build_topic_hierarchy()
                logger.info(f"Built topic hierarchy with {len(self.topic_hierarchy)} groups")
            else:
                self.topic_hierarchy = {}
            
            # Обогащение тем метаданными
            if hasattr(topics_info, 'messages_by_topic'):
                enriched_topics = self.enrich_topics_with_metadata(topics_info.get('messages_by_topic', {}))
            else:
                enriched_topics = {
                    topic_id: {
                        "terms": self.topics.get(topic_id, []),
                        "label": self.labeled_topics.get(topic_id, {}).get("label", ""),
                        "entities": self.topic_entities.get(topic_id, []),
                        "message_count": topic_data.get("message_count", 0)
                    }
                    for topic_id, topic_data in topics_info.items()
                }
            
            # Сохранение в базу данных
            if self.mongodb_available:
                try:
                    # Сохранение информации о темах
                    self.db.topic_model.delete_many({})
                    topic_docs = []
                    
                    for topic_id, topic_data in enriched_topics.items():
                        # Получаем группу из иерархии
                        hierarchy_group = None
                        for group_id, topics in self.topic_hierarchy.items():
                            if any(t.get("id") == topic_id for t in topics):
                                hierarchy_group = group_id
                                break
                        
                        # Создаем документ
                        topic_doc = {
                            "topic_id": topic_id,
                            "terms": topic_data.get("terms", []),
                            "label": self.labeled_topics.get(topic_id, {}).get("label", ""),
                            "entities": self.topic_entities.get(topic_id, []),
                            "message_count": topic_data.get("message_count", 0),
                            "top_channels": topic_data.get("top_channels", []),
                            "time_distribution": topic_data.get("time_distribution", {}),
                            "hierarchy_group": hierarchy_group
                        }
                        topic_docs.append(topic_doc)
                    
                    if topic_docs:
                        # Вставка пакетом
                        self.db.topic_model.insert_many(topic_docs)
                    
                    # Сохранение иерархии тем
                    self.db.topic_hierarchy.delete_many({})
                    hierarchy_docs = []
                    
                    for group_id, topics in self.topic_hierarchy.items():
                        hierarchy_docs.append({
                            "group_id": group_id,
                            "topics": topics,
                            "label": f"Группа {group_id}"
                        })
                    
                    if hierarchy_docs:
                        self.db.topic_hierarchy.insert_many(hierarchy_docs)
                    
                    # Обновление метаданных
                    self.db.topic_model_metadata.update_one(
                        {"name": "last_update"},
                        {"$set": {
                            "timestamp": current_time,
                            "version": self.version,
                            "topic_count": len(self.topics),
                            "approach": topics_info.get("approach", "hybrid")
                        }},
                        upsert=True
                    )
                    
                    logger.info(f"Saved topic model to database")
                except Exception as e:
                    logger.error(f"Error saving topic model to database: {e}")
            
            self.last_update_time = current_time
            self.is_trained = True
            
            # Отслеживание эволюции тем, если есть предыдущие данные
            if self.mongodb_available:
                self.track_topic_evolution()
            
            return True
                
        except Exception as e:
            logger.error(f"Error updating topic model: {e}")
            traceback.print_exc()
            return False
    
    def _large_scale_topic_modeling(self, texts: List[str], messages: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
        """
        Подход для крупномасштабного тематического моделирования (>1000 сообщений).
        Использует кластеризацию с последующим обогащением тем через LLM.
        
        Args:
            texts: Список текстов сообщений
            messages: Список словарей с данными сообщений
            
        Returns:
            Словарь с информацией о темах
        """
        # Вычисление эмбеддингов для документов
        logger.info("Computing document embeddings")
        document_embeddings = self.sentence_model.encode(
            texts, 
            batch_size=32, 
            show_progress_bar=True,
            convert_to_numpy=True
        )
        
        # Определение оптимального количества кластеров
        n_clusters = min(max(self.min_topics, len(texts) // 100), self.max_topics)
        logger.info(f"Using {n_clusters} clusters for KMeans")
        
        # Запуск KMeans кластеризации
        kmeans = KMeans(
            n_clusters=n_clusters,
            random_state=42,
            n_init=10
        )
        
        logger.info("Starting KMeans clustering")
        topics = kmeans.fit_predict(document_embeddings)
        
        # Создание словаря с текстами и сообщениями для каждой темы
        topic_docs = {}
        messages_by_topic = {}
        
        for i, topic_id in enumerate(topics):
            if topic_id not in topic_docs:
                topic_docs[topic_id] = []
                messages_by_topic[topic_id] = []
            
            topic_docs[topic_id].append(texts[i])
            if i < len(messages):
                messages_by_topic[topic_id].append(messages[i])
        
        # Обработка LLM для представителей каждой темы
        topics_info = {}
        
        logger.info("Enriching clusters with LLM")
        for topic_id, cluster_texts in topic_docs.items():
            # Выбор репрезентативных текстов для каждого кластера
            if len(cluster_texts) <= 5:
                sample_texts = cluster_texts
            else:
                # Выбираем тексты, близкие к центроиду кластера
                cluster_center = kmeans.cluster_centers_[topic_id]
                
                # Вычисляем расстояния до центра
                indices = [i for i, t in enumerate(topics) if t == topic_id]
                distances = [
                    np.linalg.norm(document_embeddings[i] - cluster_center) 
                    for i in indices
                ]
                
                # Сортируем по расстоянию и выбираем самые близкие к центру
                sorted_indices = [indices[i] for i in np.argsort(distances)[:5]]
                sample_texts = [texts[i] for i in sorted_indices]
            
            # Обращение к LLM для обогащения темы
            if self.llm_available:
                topic_data = self._extract_topic_with_llm(sample_texts)
                
                if topic_data:
                    # Дополнительно добавляем статистические ключевые слова
                    statistical_terms = self._extract_statistical_keywords(cluster_texts)
                    # Объединяем ключевые слова и удаляем дубликаты
                    combined_terms = list(set(topic_data.get("terms", []) + statistical_terms))
                    
                    topics_info[int(topic_id)] = {
                        "label": topic_data.get("label", f"Тема {topic_id}"),
                        "terms": combined_terms,
                        "entities": topic_data.get("entities", []),
                        "description": topic_data.get("description", ""),
                        "message_count": len(cluster_texts)
                    }
                else:
                    # Фолбэк на статистический подход
                    terms = self._extract_statistical_keywords(cluster_texts)
                    topics_info[int(topic_id)] = {
                        "label": terms[0].capitalize() if terms else f"Тема {topic_id}",
                        "terms": terms,
                        "message_count": len(cluster_texts)
                    }
            else:
                # Если LLM недоступен, используем только статистический подход
                terms = self._extract_statistical_keywords(cluster_texts)
                topics_info[int(topic_id)] = {
                    "label": terms[0].capitalize() if terms else f"Тема {topic_id}",
                    "terms": terms,
                    "message_count": len(cluster_texts)
                }
        
        # Добавляем информацию о подходе и связывании сообщений с темами
        topics_info["approach"] = "large_scale"
        topics_info["messages_by_topic"] = messages_by_topic
        
        return topics_info
    
    def _medium_scale_topic_modeling(self, texts: List[str], messages: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
        """
        Подход для среднемасштабного тематического моделирования (100-1000 сообщений).
        Использует LLM для извлечения тем из групп сообщений.
        
        Args:
            texts: Список текстов сообщений
            messages: Список словарей с данными сообщений
            
        Returns:
            Словарь с информацией о темах
        """
        # Вычисляем эмбеддинги для текстов
        logger.info("Computing document embeddings")
        document_embeddings = self.sentence_model.encode(
            texts, 
            batch_size=32, 
            show_progress_bar=True,
            convert_to_numpy=True
        )
        
        # Делим сообщения на пакеты для обработки LLM
        batch_size = 5  # Количество сообщений в одном запросе к LLM
        num_batches = min(20, len(texts) // batch_size)  # Максимум 20 тем для средних объемов
        
        if self.llm_available:
            # Группировка сообщений с помощью KMeans
            kmeans = KMeans(
                n_clusters=num_batches,
                random_state=42,
                n_init=10
            )
            
            logger.info(f"Grouping messages into {num_batches} batches")
            clusters = kmeans.fit_predict(document_embeddings)
            
            # Создание групп сообщений
            grouped_texts = {}
            messages_by_topic = {}
            
            for i, cluster_id in enumerate(clusters):
                if cluster_id not in grouped_texts:
                    grouped_texts[cluster_id] = []
                    messages_by_topic[cluster_id] = []
                
                grouped_texts[cluster_id].append(texts[i])
                if i < len(messages):
                    messages_by_topic[cluster_id].append(messages[i])
            
            # Обрабатываем каждую группу с помощью LLM
            topics_info = {}
            
            logger.info("Extracting topics with LLM from grouped messages")
            for group_id, group_texts in grouped_texts.items():
                # Выбираем образцы текстов для обработки LLM
                sample_size = min(5, len(group_texts))
                sample_texts = random.sample(group_texts, sample_size)
                
                
                # Извлечение темы с помощью LLM
                topic_data = self._extract_topic_with_llm(sample_texts)
                
                if topic_data:
                    topics_info[int(group_id)] = {
                        "label": topic_data.get("label", f"Тема {group_id}"),
                        "terms": topic_data.get("terms", []),
                        "entities": topic_data.get("entities", []),
                        "description": topic_data.get("description", ""),
                        "message_count": len(group_texts)
                    }
                else:
                    # Фолбэк на статистические ключевые слова
                    terms = self._extract_statistical_keywords(group_texts)
                    topics_info[int(group_id)] = {
                        "label": terms[0].capitalize() if terms else f"Тема {group_id}",
                        "terms": terms,
                        "message_count": len(group_texts)
                    }
            
            # Добавляем информацию о подходе и связывании сообщений с темами
            topics_info["approach"] = "medium_scale"
            topics_info["messages_by_topic"] = messages_by_topic
            
            return topics_info
        else:
            # Если LLM недоступен, используем упрощенный подход с кластеризацией
            return self._large_scale_topic_modeling(texts, messages)
    
    def _small_scale_topic_modeling(self, texts: List[str], messages: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
        """
        Подход для малого объема данных (<100 сообщений).
        Использует LLM для прямого извлечения тем.
        
        Args:
            texts: Список текстов сообщений
            messages: Список словарей с данными сообщений
            
        Returns:
            Словарь с информацией о темах
        """
        if not self.llm_available:
            # Если LLM недоступен, используем базовую кластеризацию
            return self._large_scale_topic_modeling(texts, messages)
        
        # Для малых объемов обрабатываем все тексты вместе одним запросом
        logger.info("Using direct LLM approach for small dataset")
        
        # Выбираем до 10 сообщений для анализа
        sample_size = min(10, len(texts))
        sample_texts = random.sample(texts, sample_size)
        
        # Формируем более сложный запрос для выделения нескольких тем
        prompt = f"""
        Проанализируй следующую коллекцию сообщений и определи 3-5 основных тем, которые в них затрагиваются.
        
        Сообщения:
        {'-' * 40}
        """ + '\n'.join([f"{i+1}. {text[:300]}..." for i, text in enumerate(sample_texts)]) + f"""
        {'-' * 40}
        
        Для каждой выявленной темы укажи:
        1. Название темы (краткое, 1-3 слова)
        2. 5-10 ключевых слов, характеризующих эту тему
        3. Основные упоминаемые сущности (люди, организации, места)
        4. Краткое описание темы (1-2 предложения)
        
        Формат ответа:
        ТЕМА 1:
        Название: [название темы]
        Ключевые слова: [слово1], [слово2], [слово3], ...
        Сущности: [сущность1], [сущность2], ...
        Описание: [краткое описание]
        
        ТЕМА 2:
        ... и так далее для каждой темы
        """
        
        # Обращение к LLM
        response = self._call_llm(prompt)
        
        if not response:
            # Если не удалось получить ответ, используем базовую кластеризацию
            return self._large_scale_topic_modeling(texts, messages)
        
        # Разбор ответа LLM
        topics_info = {}
        topic_sections = re.split(r'ТЕМА \d+:', response)
        
        if len(topic_sections) <= 1:
            # Если разбить не удалось, пробуем другой формат
            topic_sections = re.split(r'Тема \d+:', response)
            
        # Удаляем пустые секции
        topic_sections = [s.strip() for s in topic_sections if s.strip()]
        
        # Равномерно распределяем сообщения по темам
        messages_by_topic = {}
        
        for topic_idx, section in enumerate(topic_sections):
            try:
                # Извлечение названия темы
                label_match = re.search(r'Название:\s*(.+?)[\n\r]', section)
                label = label_match.group(1).strip() if label_match else f"Тема {topic_idx}"
                
                # Извлечение ключевых слов
                keywords_match = re.search(r'Ключевые слова:\s*(.+?)[\n\r]', section)
                keywords_str = keywords_match.group(1).strip() if keywords_match else ""
                keywords = [kw.strip() for kw in keywords_str.split(',') if kw.strip()]
                
                # Извлечение сущностей
                entities_match = re.search(r'Сущности:\s*(.+?)[\n\r]', section)
                entities_str = entities_match.group(1).strip() if entities_match else ""
                entities = [ent.strip() for ent in entities_str.split(',') if ent.strip()]
                
                # Извлечение описания
                desc_match = re.search(r'Описание:\s*(.+?)(?:$|[\n\r]ТЕМА|[\n\r]Тема)', section, re.DOTALL)
                description = desc_match.group(1).strip() if desc_match else ""
                
                # Если не нашли ключевые слова, извлекаем из описания
                if not keywords and description:
                    keywords = self._extract_keywords_from_text(description)
                
                topics_info[topic_idx] = {
                    "label": label,
                    "terms": keywords,
                    "entities": entities,
                    "description": description,
                    "message_count": 0  # Обновим позже
                }
                
                # Создаем пустой список для сообщений этой темы
                messages_by_topic[topic_idx] = []
                
            except Exception as e:
                logger.error(f"Error parsing LLM response section: {e}")
                continue
        
        # Если не удалось извлечь ни одной темы, используем базовую кластеризацию
        if not topics_info:
            return self._large_scale_topic_modeling(texts, messages)
        
        # Распределение сообщений по темам на основе эмбеддингов
        topic_embeddings = {}
        for topic_id, topic_data in topics_info.items():
            # Создаем текстовое представление темы
            topic_text = f"{topic_data['label']} {' '.join(topic_data['terms'])} {' '.join(topic_data['entities'])}"
            # Вычисляем эмбеддинг темы
            topic_embeddings[topic_id] = self.sentence_model.encode([topic_text])[0]
        
        # Вычисляем эмбеддинги для всех текстов
        text_embeddings = self.sentence_model.encode(texts, batch_size=32, show_progress_bar=True)
        
        # Распределяем сообщения по темам на основе косинусной близости
        for i, text_embedding in enumerate(text_embeddings):
            # Вычисляем близость к каждой теме
            similarities = {
                topic_id: cosine_similarity([text_embedding], [topic_emb])[0][0]
                for topic_id, topic_emb in topic_embeddings.items()
            }
            
            # Определяем наиболее близкую тему
            best_topic_id = max(similarities.items(), key=lambda x: x[1])[0]
            
            # Добавляем сообщение в соответствующую тему
            if i < len(messages):
                messages_by_topic[best_topic_id].append(messages[i])
            
            # Обновляем счетчик сообщений для темы
            topics_info[best_topic_id]["message_count"] += 1
        
        # Добавляем информацию о подходе и связывании сообщений с темами
        topics_info["approach"] = "small_scale"
        topics_info["messages_by_topic"] = messages_by_topic
        
        return topics_info
    
    def _extract_topic_with_llm(self, texts: List[str]) -> Dict[str, Any]:
        """
        Извлечение информации о теме из текстов с помощью LLM.
        
        Args:
            texts: Список текстов для анализа
            
        Returns:
            Словарь с информацией о теме или пустой словарь в случае ошибки
        """
        if not texts or not self.llm_available:
            return {}
        
        # Ограничиваем длину текстов для запроса LLM
        samples = [text[:500] + "..." if len(text) > 500 else text for text in texts[:5]]
        
        # Создаем ключ кэширования на основе хеша содержимого
        cache_key = hash(str(samples))
        if cache_key in self.llm_cache:
            return self.llm_cache[cache_key]
        
        # Формируем запрос к LLM
        prompt = f"""
        Проанализируй следующие тексты и определи их общую тему:
        {'-' * 40}
        """ + '\n'.join([f"{i+1}. {text}" for i, text in enumerate(samples)]) + f"""
        {'-' * 40}
        
        Определи:
        1. Название темы (кратко, 1-3 слова)
        2. Ключевые слова, характеризующие эту тему (5-7 слов)
        3. Основные упоминаемые сущности (люди, организации, места)
        4. Краткое описание темы (1-2 предложения)
        
        Формат ответа (строго придерживайся этого формата):
        Название: [название темы]
        Ключевые слова: [слово1], [слово2], [слово3], ...
        Сущности: [сущность1], [сущность2], ...
        Описание: [краткое описание]
        """
        
        # Обращение к LLM
        response = self._call_llm(prompt)
        
        if not response:
            return {}
        
        try:
            # Извлечение названия темы
            label_match = re.search(r'Название:\s*(.+?)[\n\r]', response)
            label = label_match.group(1).strip() if label_match else ""
            
            # Извлечение ключевых слов
            keywords_match = re.search(r'Ключевые слова:\s*(.+?)[\n\r]', response)
            keywords_str = keywords_match.group(1).strip() if keywords_match else ""
            keywords = [kw.strip() for kw in keywords_str.split(',') if kw.strip()]
            
            # Извлечение сущностей
            entities_match = re.search(r'Сущности:\s*(.+?)[\n\r]', response)
            entities_str = entities_match.group(1).strip() if entities_match else ""
            entities = [ent.strip() for ent in entities_str.split(',') if ent.strip()]
            
            # Извлечение описания
            desc_match = re.search(r'Описание:\s*(.+?)$', response, re.DOTALL)
            description = desc_match.group(1).strip() if desc_match else ""
            
            result = {
                "label": label,
                "terms": keywords,
                "entities": entities,
                "description": description
            }
            
            # Сохраняем в кэш
            self.llm_cache[cache_key] = result
            
            # Очистка кэша, если он слишком большой
            if len(self.llm_cache) > self.cache_size_limit:
                # Оставляем только половину записей
                keys_to_keep = list(self.llm_cache.keys())[-self.cache_size_limit//2:]
                self.llm_cache = {k: self.llm_cache[k] for k in keys_to_keep}
                
            return result
        except Exception as e:
            logger.error(f"Error parsing LLM response: {e}")
            return {}
    
    def _extract_statistical_keywords(self, texts: List[str]) -> List[str]:
        """
        Извлечение ключевых слов из текстов на основе TF-IDF.
        
        Args:
            texts: Список текстов для анализа
            
        Returns:
            Список ключевых слов
        """
        if not texts:
            return []
        
        try:
            # Используем TF-IDF для извлечения ключевых слов
            tfidf_matrix = self.vectorizer.fit_transform(texts)
            feature_names = self.vectorizer.get_feature_names_out()
            
            # Получаем средние значения TF-IDF для каждого термина
            tfidf_means = tfidf_matrix.mean(axis=0).A1
            
            # Получаем индексы топ-N терминов
            top_n = 15
            top_indices = tfidf_means.argsort()[-top_n:][::-1]
            
            # Получаем термины
            top_terms = [feature_names[i] for i in top_indices]
            
            return top_terms
        except Exception as e:
            logger.error(f"Error extracting statistical keywords: {e}")
            
            # Фолбэк: используем предобработанные тексты
            all_words = []
            for text in texts:
                tokens = self.preprocess_text(text)
                all_words.extend(tokens)
            
            # Подсчет частоты слов
            word_counts = {}
            for word in all_words:
                word_counts[word] = word_counts.get(word, 0) + 1
            
            # Сортировка по частоте
            sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
            top_terms = [word for word, _ in sorted_words[:15]]
            
            return top_terms
    
    def _extract_keywords_from_text(self, text: str) -> List[str]:
        """
        Извлечение ключевых слов из одного текста.
        
        Args:
            text: Текст для анализа
            
        Returns:
            Список ключевых слов
        """
        # Используем предобработку текста для извлечения ключевых слов
        tokens = self.preprocess_text(text)
        
        # Если слишком мало токенов, пытаемся разбить текст на существительные и прилагательные
        if len(tokens) < 5 and self.nlp:
            try:
                doc = self.nlp(text)
                nouns_adj = [token.text.lower() for token in doc if token.pos_ in ['NOUN', 'ADJ', 'PROPN']]
                if nouns_adj:
                    tokens = nouns_adj
            except:
                pass
        
        # Ограничиваем количество ключевых слов
        return tokens[:10]
    
    def _call_llm(self, prompt: str) -> str:
        """
        Вызов LLM для генерации текста.
        
        Args:
            prompt: Текст запроса
            
        Returns:
            Ответ модели или пустая строка в случае ошибки
        """
        try:
                # Вызов локального LLM
            data = {
                "prompt": prompt,
                "max_tokens": 800,
                "temperature": 0.3
            }
            
            response = requests.post(self.llm_url, json=data, timeout=240)
            response.raise_for_status()
            
            result = response.json()
            return result.get("generated_text", "")
           
        except Exception as e:
            logger.error(f"Error calling LLM: {e}")
            return ""
    
    def get_topics(self, text: str, threshold: float = 0.1) -> List[Dict[str, Any]]:
        """
        Извлечение тем из текста с использованием гибридного подхода.
        
        Args:
            text: Текст для анализа
            threshold: Порог вероятности для включения темы
            
        Returns:
            Список тем с метками и ключевыми словами
        """
        # Проверка на валидность входных данных
        if not text or len(text) < 10:
            return []
        
        # Если у нас есть доступ к LLM и текст достаточно длинный, используем его для прямого извлечения
        if self.llm_available and len(text) > 200:
            try:
                # Прямое извлечение темы с LLM
                prompt = f"""
                Проанализируй следующий текст и определи:
                1. Основную тему (1-3 слова)
                2. 3-5 ключевых слов
                3. Упоминаемые сущности (люди, организации, места)
                
                Текст:
                {text[:1000]}...
                
                Ответ в формате:
                Тема: [тема]
                Ключевые слова: [слово1], [слово2], ...
                Сущности: [сущность1], [сущность2], ...
                """
                
                response = self._call_llm(prompt)
                
                if response:
                    # Извлечение данных из ответа
                    label_match = re.search(r'Тема:\s*(.+?)[\n\r]', response)
                    keywords_match = re.search(r'Ключевые слова:\s*(.+?)[\n\r]', response)
                    entities_match = re.search(r'Сущности:\s*(.+?)(?:$|[\n\r])', response)
                    
                    label = label_match.group(1).strip() if label_match else ""
                    keywords_str = keywords_match.group(1).strip() if keywords_match else ""
                    entities_str = entities_match.group(1).strip() if entities_match else ""
                    
                    keywords = [kw.strip() for kw in keywords_str.split(',') if kw.strip()]
                    entities = [ent.strip() for ent in entities_str.split(',') if ent.strip()]
                    
                    return [{
                        "label": label,
                        "keywords": keywords,
                        "entities": entities
                    }]
            except Exception as e:
                logger.error(f"Error extracting topics with LLM: {e}")
                # Продолжаем с фолбэком
        
        # Фолбэк подход если LLM недоступен или произошла ошибка
        
        # Если модель не обучена или нет тем, возвращаем пустой список
        if not self.is_trained or not self.topics:
            # Простое извлечение ключевых слов
            keywords = self._extract_keywords_from_text(text)
            if keywords:
                return [{
                    "label": keywords[0].capitalize(),
                    "keywords": keywords[:5],
                    "entities": []
                }]
            return []
        
        try:
            # Вычисление эмбеддинга текста
            text_embedding = self.sentence_model.encode([text])[0]
            
            # Создание эмбеддингов для каждой темы
            topic_embeddings = {}
            for topic_id, terms in self.topics.items():
                # Объединяем термины в один текст
                topic_text = " ".join(terms[:5])
                topic_embedding = self.sentence_model.encode([topic_text])[0]
                topic_embeddings[topic_id] = topic_embedding
            
            # Вычисление схожести с каждой темой
            similarities = {}
            for topic_id, topic_embedding in topic_embeddings.items():
                sim = cosine_similarity([text_embedding], [topic_embedding])[0][0]
                similarities[topic_id] = sim
            
            # Сортировка тем по схожести
            sorted_topics = sorted(similarities.items(), key=lambda x: x[1], reverse=True)
            
            # Формирование результата
            result = []
            for topic_id, sim in sorted_topics:
                if sim > threshold and topic_id in self.topics:
                    topic_label = self.labeled_topics.get(topic_id, {}).get("label", "")
                    topic_terms = self.topics.get(topic_id, [])
                    topic_entities = self.topic_entities.get(topic_id, [])
                    
                    result.append({
                        "label": topic_label,
                        "keywords": topic_terms[:5],
                        "entities": topic_entities[:5],
                        "similarity": float(sim)
                    })
                
                # Ограничиваем количество возвращаемых тем
                if len(result) >= 3:
                    break
            
            return result
        except Exception as e:
            logger.error(f"Error extracting topics: {e}")
            
            # Фолбэк на ключевые слова
            keywords = self._extract_keywords_from_text(text)
            if keywords:
                return [{
                    "label": keywords[0].capitalize(),
                    "keywords": keywords[:5],
                    "entities": []
                }]
            return []
    
    def extract_main_entity(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Извлечение основной сущности из текста с использованием LLM и NER.
        
        Args:
            text: Текст для анализа
            
        Returns:
            Словарь с информацией о сущности или None
        """
        if not text or len(text) < 10:
            return None
        
        # Пытаемся использовать LLM для точного извлечения сущностей
        if self.llm_available and len(text) > 100:
            try:
                prompt = f"""
                Определи главную сущность (человек, организация, место или событие) в следующем тексте:
                
                {text[:1000]}...
                
                Укажи:
                1. Название сущности (как она указана в тексте)
                2. Тип сущности (ЧЕЛОВЕК, ОРГАНИЗАЦИЯ, МЕСТО, СОБЫТИЕ или ДРУГОЕ)
                3. Короткое пояснение, почему это главная сущность в тексте
                
                Формат ответа:
                Сущность: [название]
                Тип: [тип]
                Пояснение: [короткое пояснение]
                """
                
                response = self._call_llm(prompt)
                
                if response:
                    entity_match = re.search(r'Сущность:\s*(.+?)[\n\r]', response)
                    type_match = re.search(r'Тип:\s*(.+?)[\n\r]', response)
                    desc_match = re.search(r'Пояснение:\s*(.+?)(?:$|[\n\r])', response)
                    
                    if entity_match:
                        entity_name = entity_match.group(1).strip()
                        entity_type = type_match.group(1).strip() if type_match else "UNKNOWN"
                        description = desc_match.group(1).strip() if desc_match else ""
                        
                        return {
                            "name": entity_name,
                            "type": entity_type,
                            "description": description,
                            "method": "llm"
                        }
            except Exception as e:
                logger.error(f"Error extracting entity with LLM: {e}")
                # Продолжаем с фолбэком
        
        # Фолбэк на spaCy NER
        if self.nlp:
            try:
                # Обработка текста через spaCy
                doc = self.nlp(text[:5000])  # Ограничиваем длину для экономии ресурсов
                
                # Извлечение именованных сущностей
                entities = []
                for entity in doc.ents:
                    if entity.label_ in ['PER', 'ORG', 'GPE', 'LOC', 'EVENT']:
                        entities.append((entity.text, entity.label_))
                
                # Если нет сущностей, возвращаем None
                if not entities:
                    return None
                
                # Подсчет частоты сущностей
                entity_counts = {}
                for entity, label in entities:
                    if entity.lower() in entity_counts:
                        entity_counts[entity.lower()] += 1
                        entity_counts[entity.lower() + "_label"] = label
                    else:
                        entity_counts[entity.lower()] = 1
                        entity_counts[entity.lower() + "_label"] = label
                
                # Выбор наиболее частой сущности
                if entity_counts:
                    main_entity = max(entity_counts.items(), key=lambda x: x[1] if "_label" not in x[0] else 0)[0]
                    entity_label = entity_counts.get(main_entity + "_label")
                    
                    # Преобразование меток spaCy в более понятные
                    entity_type_map = {
                        'PER': 'ЧЕЛОВЕК',
                        'ORG': 'ОРГАНИЗАЦИЯ',
                        'GPE': 'МЕСТО',
                        'LOC': 'МЕСТО',
                        'EVENT': 'СОБЫТИЕ'
                    }
                    
                    return {
                        "name": main_entity,
                        "type": entity_type_map.get(entity_label, entity_label),
                        "method": "spacy"
                    }
                
                return None
            except Exception as e:
                logger.error(f"Error extracting main entity with spaCy: {e}")
                return None
        
        return None
    
    def build_topic_hierarchy(self) -> Dict[int, List[Dict[str, Any]]]:
        """
        Построение иерархии тем на основе семантической близости.
        
        Returns:
            Словарь групп тем
        """
        if not self.is_trained or not self.topics or not self.model_available:
            return {}
        
        try:
            # Получение эмбеддингов тем
            topic_embeddings = []
            topic_ids = []
            
            for topic_id in sorted(self.topics.keys()):
                # Создаем эмбеддинги для ключевых слов
                label = self.labeled_topics.get(topic_id, {}).get("label", "")
                terms = self.topics.get(topic_id, [])
                
                topic_text = f"{label} {' '.join(terms[:5])}"
                embedding = self.sentence_model.encode([topic_text])[0]
                topic_embeddings.append(embedding)
                topic_ids.append(topic_id)
            
            # Если тем недостаточно для построения иерархии
            if len(topic_embeddings) < 3:
                return {}
            
            # Создание иерархии с помощью агломеративной кластеризации
            n_clusters = max(2, min(len(topic_embeddings) // 3, 8))  # От 2 до 8 кластеров
            
            clustering = AgglomerativeClustering(
                n_clusters=n_clusters,
                linkage='ward'
            ).fit(topic_embeddings)
            
            # Построение словаря иерархии тем
            hierarchy = {}
            for i, (topic_id, label) in enumerate(zip(topic_ids, clustering.labels_)):
                if label not in hierarchy:
                    hierarchy[int(label)] = []
                
                topic_terms = self.topics.get(topic_id, [])
                topic_label = self.labeled_topics.get(topic_id, {}).get("label", 
                                                                     topic_terms[0].capitalize() if topic_terms else f"Тема {topic_id}")
                
                hierarchy[int(label)].append({
                    "id": int(topic_id),
                    "terms": topic_terms[:5],
                    "label": topic_label
                })
            
            # Генерация названий групп с помощью LLM, если доступно
            if self.llm_available:
                for group_id, topics in hierarchy.items():
                    # Создаем описание группы на основе тем
                    topic_descriptions = [f"{t['label']}: {', '.join(t['terms'][:3])}" for t in topics]
                    # Создаем запрос к LLM для именования группы
                    prompt = f"""
                    Придумай короткое название (1-3 слова) для группы тем, включающей следующие темы:
                    {', '.join([t['label'] for t in topics])}
                    
                    Подробнее о темах:
                    {'. '.join(topic_descriptions)}
                    
                    Дай только название группы без дополнительных пояснений:
                    """
                    
                    response = self._call_llm(prompt)
                    if response:
                        # Добавляем название группы
                        hierarchy[group_id] = {
                            "label": response.strip(),
                            "topics": topics
                        }
                    else:
                        # Фолбэк на автоматическое название
                        hierarchy[group_id] = {
                            "label": f"Группа {group_id}",
                            "topics": topics
                        }
            
            return hierarchy
        except Exception as e:
            logger.error(f"Error building topic hierarchy: {e}")
            traceback.print_exc()
            return {}
    
    def enrich_topics_with_metadata(self, topics_with_messages: Dict[int, List[Dict[str, Any]]]) -> Dict[int, Dict[str, Any]]:
        """
        Обогащение тем метаданными из связанных сообщений.
        
        Args:
            topics_with_messages: Словарь тем с сообщениями
            
        Returns:
            Обогащенные темы с метаданными
        """
        enriched_topics = {}
        
        for topic_id, messages in topics_with_messages.items():
            if topic_id not in self.topics:
                continue
                
            # Подсчет каналов
            channel_counts = {}
            for msg in messages:
                channel_id = msg.get("channel_id")
                channel_name = msg.get("channel_name", "Неизвестный канал")
                
                if channel_id:
                    key = f"{channel_id}:{channel_name}"
                    channel_counts[key] = channel_counts.get(key, 0) + 1
            
            # Сортировка каналов по количеству сообщений
            top_channels = sorted(
                channel_counts.items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:5]
            
            # Временное распределение
            timestamps = [msg.get("date") for msg in messages if msg.get("date")]
            time_histogram = {}
            
            for ts in timestamps:
                # Преобразование строки в datetime, если необходимо
                if isinstance(ts, str):
                    try:
                        ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    except (ValueError, TypeError):
                        continue
                
                # Получение часа
                try:
                    hour = ts.hour
                    time_histogram[hour] = time_histogram.get(hour, 0) + 1
                except (AttributeError, TypeError):
                    continue
            
            # Счетчик просмотров
            view_count = sum(msg.get("views", 0) for msg in messages if msg.get("views") is not None)
            
            # Обогащение темы метаданными
            enriched_topics[topic_id] = {
                "terms": self.topics.get(topic_id, []),
                "label": self.labeled_topics.get(topic_id, {}).get("label", ""),
                "entities": self.topic_entities.get(topic_id, []),
                "message_count": len(messages),
                "view_count": view_count,
                "top_channels": [{"channel": k, "count": v} for k, v in top_channels],
                "time_distribution": time_histogram,
                "first_seen": min(timestamps).isoformat() if timestamps else None,
                "last_seen": max(timestamps).isoformat() if timestamps else None
            }
        
        return enriched_topics
    
    def track_topic_evolution(self, window_hours: int = 24) -> List[Dict[str, Any]]:
        """
        Отслеживание эволюции тем за определенный период времени.
        
        Args:
            window_hours: Размер временного окна в часах
            
        Returns:
            Список с трендами тем
        """
        if not self.mongodb_available or not self.db:
            return []
        
        try:
            # Получаем текущее время
            current_time = datetime.utcnow()
            previous_time = current_time - timedelta(hours=window_hours)
            
            # Запрос к базе данных для получения сообщений с временными метками
            current_period_pipeline = [
                {"$match": {"date": {"$gte": previous_time, "$lte": current_time}}},
                {"$unwind": "$topics"},
                {"$group": {"_id": "$topics", "count": {"$sum": 1}}}
            ]
            
            current_period_results = list(self.db.messages.aggregate(current_period_pipeline))
            
            # Предыдущий период для сравнения
            previous_period_start = previous_time - timedelta(hours=window_hours)
            
            previous_period_pipeline = [
                {"$match": {"date": {"$gte": previous_period_start, "$lte": previous_time}}},
                {"$unwind": "$topics"},
                {"$group": {"_id": "$topics", "count": {"$sum": 1}}}
            ]
            
            previous_period_results = list(self.db.messages.aggregate(previous_period_pipeline))
            
            # Преобразование результатов в словари для удобства
            current_topic_counts = {item["_id"]: item["count"] for item in current_period_results}
            previous_topic_counts = {item["_id"]: item["count"] for item in previous_period_results}
            
            # Анализ изменений в трендах тем
            topic_trends = []
            
            for topic, current_count in current_topic_counts.items():
                previous_count = previous_topic_counts.get(topic, 0)
                
                # Расчет изменения (в процентах)
                if previous_count > 0:
                    change_percent = ((current_count - previous_count) / previous_count) * 100
                else:
                    change_percent = 100  # Новая тема
                
                # Определение тренда
                trend = "stable"
                if change_percent > 50:
                    trend = "rapidly_growing"
                elif change_percent > 20:
                    trend = "growing"
                elif change_percent < -50:
                    trend = "rapidly_declining"
                elif change_percent < -20:
                    trend = "declining"
                
                # Создание записи о тренде темы
                topic_trends.append({
                    "topic": topic,
                    "current_count": current_count,
                    "previous_count": previous_count,
                    "change_percent": change_percent,
                    "trend": trend,
                    "timestamp": current_time.isoformat()
                })
            
            # Сортировка трендов по изменению (от наиболее растущих к снижающимся)
            topic_trends.sort(key=lambda x: x["change_percent"], reverse=True)
            
            # Сохранение трендов в базу данных
            if topic_trends and self.mongodb_available:
                self.db.topic_trends.insert_many(topic_trends)
            
            return topic_trends
        except Exception as e:
            logger.error(f"Error tracking topic evolution: {e}")
            return []
    
    def suggest_related_topics(self, topic_id: int, threshold: float = 0.6) -> List[Dict[str, Any]]:
        """
        Поиск тем, связанных с указанной темой.
        
        Args:
            topic_id: ID темы для поиска связанных
            threshold: Порог схожести для включения в результат
            
        Returns:
            Список связанных тем
        """
        if not self.model_available or not self.is_trained or topic_id not in self.topics:
            return []
        
        try:
            # Создаем текстовое представление искомой темы
            main_topic_label = self.labeled_topics.get(topic_id, {}).get("label", "")
            main_topic_terms = self.topics.get(topic_id, [])
            main_topic_text = f"{main_topic_label} {' '.join(main_topic_terms[:10])}"
            
            # Создаем эмбеддинг этой темы
            main_topic_embedding = self.sentence_model.encode([main_topic_text])[0]
            
            # Вычисляем эмбеддинги для всех остальных тем
            related_topics = []
            
            for other_id in self.topics.keys():
                if other_id == topic_id:
                    continue
                
                other_label = self.labeled_topics.get(other_id, {}).get("label", "")
                other_terms = self.topics.get(other_id, [])
                other_text = f"{other_label} {' '.join(other_terms[:10])}"
                
                other_embedding = self.sentence_model.encode([other_text])[0]
                
                # Вычисляем косинусную близость
                similarity = cosine_similarity([main_topic_embedding], [other_embedding])[0][0]
                
                if similarity >= threshold:
                    related_topics.append({
                        "topic_id": other_id,
                        "label": other_label,
                        "terms": other_terms[:5],
                        "similarity": float(similarity)
                    })
            
            # Сортируем по схожести
            related_topics.sort(key=lambda x: x["similarity"], reverse=True)
            
            return related_topics[:5]  # Возвращаем до 5 наиболее связанных тем
            
        except Exception as e:
            logger.error(f"Error finding related topics: {e}")
            return []
    
    def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        """
        Анализ тональности текста с использованием LLM.
        
        Args:
            text: Текст для анализа
            
        Returns:
            Словарь с данными о тональности
        """
        if not self.llm_available or not text or len(text) < 10:
            return {"sentiment": "neutral", "score": 0.0}
        
        try:
            # Формируем запрос к LLM
            prompt = f"""
            Определи тональность следующего текста. Тональность может быть:
            - Позитивная
            - Негативная
            - Нейтральная
            
            Текст:
            {text[:1000]}...
            
            Ответ дай в формате:
            Тональность: [тип тональности]
            Уверенность: [число от 0 до 1]
            Обоснование: [краткое пояснение в одном предложении]
            """
            
            response = self._call_llm(prompt)
            
            if response:
                # Извлечение тональности
                sentiment_match = re.search(r'Тональность:\s*(.+?)[\n\r]', response)
                confidence_match = re.search(r'Уверенность:\s*(.+?)[\n\r]', response)
                justification_match = re.search(r'Обоснование:\s*(.+?)(?:$|[\n\r])', response)
                
                sentiment = sentiment_match.group(1).strip().lower() if sentiment_match else "neutral"
                
                # Маппинг на английские термины для совместимости
                sentiment_map = {
                    "позитивная": "positive",
                    "положительная": "positive",
                    "негативная": "negative",
                    "отрицательная": "negative",
                    "нейтральная": "neutral"
                }
                
                mapped_sentiment = sentiment_map.get(sentiment, "neutral")
                
                # Извлечение уверенности
                confidence = 0.5
                if confidence_match:
                    confidence_text = confidence_match.group(1).strip()
                    try:
                        confidence = float(confidence_text)
                    except ValueError:
                        # Если не число, пытаемся извлечь из текста
                        confidence_digits = re.findall(r'0\.\d+|\d+\.\d+|\d+', confidence_text)
                        if confidence_digits:
                            try:
                                confidence = float(confidence_digits[0])
                            except ValueError:
                                pass
                
                # Извлечение обоснования
                justification = justification_match.group(1).strip() if justification_match else ""
                
                # Расчет итогового числового значения для sentiment score
                if mapped_sentiment == "positive":
                    score = confidence
                elif mapped_sentiment == "negative":
                    score = -confidence
                else:
                    score = 0
                
                return {
                    "sentiment": mapped_sentiment,
                    "score": score,
                    "confidence": confidence,
                    "justification": justification
                }
                
            return {"sentiment": "neutral", "score": 0.0}
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {e}")
            return {"sentiment": "neutral", "score": 0.0}
    
    def get_model_info(self) -> Dict[str, Any]:
        """
        Получение информации о текущем состоянии модели.
        
        Returns:
            Словарь с информацией о модели
        """
        return {
            "is_trained": self.is_trained,
            "topic_count": len(self.topics),
            "last_update": self.last_update_time.isoformat() if self.last_update_time else None,
            "model_available": self.model_available,
            "llm_available": self.llm_available,
            "llm_type": "local" if self.use_local_llm else "external",
            "version": self.version,
            "preprocessing_cache_size": len(self.preprocessing_cache),
            "llm_cache_size": len(self.llm_cache),
            "cache_hits": self.cache_hits,
            "topic_hierarchy_groups": len(self.topic_hierarchy) if self.topic_hierarchy else 0
        }
    
    def clear_cache(self):
        """Очистка кэшей"""
        self.preprocessing_cache = {}
        self.llm_cache = {}
        self.cache_hits = 0
        logger.info("All caches cleared")
