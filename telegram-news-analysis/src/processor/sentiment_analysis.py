import logging
import re
import json
from datetime import datetime
from typing import Dict, Any, Optional, List
from pymorphy2 import MorphAnalyzer
from transformers import pipeline
import requests

logger = logging.getLogger(__name__)

# Словари для анализа тональности
DEFAULT_NEGATIVE = {
    "плохо", "ужасно", "отвратительно", "провал", "неудача", "кризис", "катастрофа",
    "болезнь", "сбой", "апатия", "гнев", "грусть", "жалоба", "бесправие",
    "печаль", "разочарование", "негативно", "падение", "проблема", "ухудшение", "поражение",
    "потеря", "наказание", "страх", "тревога", "вред", "опасность", "беспокойство",
    "загрязнение", "выброс", "ограничение", "закрытие", "критикует", "критикующий", "требовать", 
    "кричащий", "беспокойство", "рекордный", "санкция", "карантин", "штраф", "отмена", "отказ", 
    "срыв", "блокировка", "воровство", "инцидент"
}

DEFAULT_POSITIVE = {
    "хорошо", "отлично", "превосходно", "замечательно", "успех", "прекрасно", "радость",
    "удовольствие", "позитивно", "рост", "достижение", "улучшение", "победа", "выигрыш", "награда",
    "благодарность", "счастье", "довольный", "полезный", "поддержка", "феноменальный", "гений",
    "красиво", "рекорд", "удачно", "вдохновляет", "впечатляет", "превосходит"
}

class SentimentAnalyzer:
    """
    Анализатор тональности с возможностью использования локальной LLM
    """
    
    def __init__(self, 
                 model_name: str = "DeepPavlov/rubert-base-cased-conversational", 
                 use_local_llm: bool = True,
                 local_llm_url: str = "http://llm-service:8000/generate",
                 positive_words: Optional[set] = None,
                 negative_words: Optional[set] = None):
        """
        Инициализация анализатора тональности
        
        Args:
            model_name: Имя модели трансформера
            use_local_llm: Использовать локальную LLM
            local_llm_url: URL локального LLM API
            positive_words: Набор позитивных слов
            negative_words: Набор негативных слов
        """
        self.morph = MorphAnalyzer()
        self.positive_words = (positive_words or set()).union(DEFAULT_POSITIVE)
        self.negative_words = (negative_words or set()).union(DEFAULT_NEGATIVE)
        
        # Инициализация модели трансформеров
        self.model_available = False
        try:
            logger.info(f"Loading sentiment model: {model_name}")
            self.model = pipeline(
                "sentiment-analysis", 
                model=model_name,
                device=-1
            )
            self.model_available = True
            logger.info("Sentiment model loaded successfully")
        except Exception as e:
            logger.warning(f"Failed to load sentiment model: {e}")
        
        # Конфигурация локального LLM
        self.use_local_llm = use_local_llm
        self.local_llm_url = local_llm_url
        self.llm_available = use_local_llm
        
        # Добавляем кэш для результатов анализа
        self.cache = {}
        self.cache_hits = 0
        self.cache_size_limit = 10000
        
        # Проверка доступности локального LLM
        if self.use_local_llm:
            try:
                # Тестовый запрос для проверки доступности
                response = self._call_local_llm("Тестовый запрос")
                if response:
                    logger.info("Local LLM is available")
                else:
                    logger.warning("Local LLM test request failed, will fall back to transformer model")
                    self.llm_available = False
            except Exception as e:
                logger.warning(f"Local LLM is not available: {e}")
                self.llm_available = False
        
        # Статистика
        self.model_calls = 0
        self.llm_calls = 0
        self.dictionary_calls = 0
    
    def _preprocess_text(self, text: str) -> str:
        """Предварительная обработка текста"""
        if not text:
            return ""
        
        # Удаление URL, упоминаний, хэштегов
        text = re.sub(r'http\S+|www\S+|https\S+', '', text)
        text = re.sub(r'\@\w+|\#\w+', '', text)
        
        # Нормализация пробелов
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def dictionary_analyze(self, text: str) -> Dict[str, Any]:
        """Словарный анализ с лемматизацией"""
        self.dictionary_calls += 1
        
        text = text.lower()
        words = re.findall(r'\b\w+\b', text)
        if not words:
            return {"label": "neutral", "score": 0.5, "method": "dictionary"}

        lemmatized = []
        for word in words:
            try:
                parsed = self.morph.parse(word)[0]
                lemmatized.append(parsed.normal_form)
            except Exception:
                lemmatized.append(word)

        positive_count = sum(1 for word in lemmatized if word in self.positive_words)
        negative_count = sum(1 for word in lemmatized if word in self.negative_words)
        total = len(lemmatized)

        if total == 0:
            return {"label": "neutral", "score": 0.5, "method": "dictionary"}

        if positive_count > negative_count:
            label, score = "positive", 0.5 + (positive_count / total) * 0.5
        elif negative_count > positive_count:
            label, score = "negative", 0.5 + (negative_count / total) * 0.5
        else:
            label, score = "neutral", 0.5

        return {"label": label, "score": min(max(score, 0.1), 0.9), "method": "dictionary"}
    
    def model_analyze(self, text: str) -> Dict[str, Any]:
        """Анализ с использованием модели трансформеров"""
        if not self.model_available:
            return self.dictionary_analyze(text)
        
        self.model_calls += 1
        
        try:
            result = self.model(text)
            if isinstance(result, list):
                result = result[0]
            
            label = result.get("label", "NEUTRAL").lower()
            
            # Нормализация меток
            if "positive" in label or "pos" in label:
                label = "positive"
            elif "negative" in label or "neg" in label:
                label = "negative"
            else:
                label = "neutral"
            
            score = result.get("score", 0.5)
            
            return {"label": label, "score": float(score), "method": "transformer"}
        except Exception as e:
            logger.error(f"Error in model analysis: {e}")
            return self.dictionary_analyze(text)
    
    def _call_local_llm(self, prompt: str) -> str:
        """
        Вызов локальной LLM через API
        
        Args:
            prompt: Текст запроса
            
        Returns:
            Ответ модели
        """
        try:
            data = {
                "prompt": prompt,
                "max_tokens": 500,
                "temperature": 0.3
            }
            
            response = requests.post(self.local_llm_url, json=data, timeout=240)
            response.raise_for_status()
            result = response.json()
            
            return result.get("generated_text", "")
        except Exception as e:
            logger.error(f"Error calling local LLM API: {e}")
            return ""
    
    def llm_analyze(self, text: str, entity: Optional[str] = None) -> Dict[str, Any]:
        """Анализ с использованием локальной LLM"""
        if not self.llm_available:
            return {"label": "neutral", "score": 0.5, "method": "fallback"}
        
        self.llm_calls += 1
        
        # Ограничение длины текста
        max_prompt_length = 2000
        if len(text) > max_prompt_length:
            text = text[:max_prompt_length] + "..."
        
        # Конструирование промпта
        if entity:
            prompt = f"""
            Проанализируй фрагмент новостного текста и определи тональность упоминания сущности "{entity}".
            Фрагмент: «{text}»
            
            Дай ответ в формате JSON:
            {{
              "label": "positive", // или "negative", или "neutral"
              "score": 0.8, // число от 0.1 до 0.9, показывающее уверенность
              "explanation": "Краткое объяснение тональности"
            }}
            """
        else:
            prompt = f"""
            Проанализируй тональность следующего новостного сообщения:
            «{text}»
            
            Дай ответ в формате JSON:
            {{
              "label": "positive", // или "negative", или "neutral"
              "score": 0.8, // число от 0.1 до 0.9, показывающее уверенность
              "explanation": "Краткое объяснение тональности"
            }}
            """
        
        try:
            # Вызов локальной LLM
            response = self._call_local_llm(prompt)
            
            # Парсинг ответа
            return self._parse_llm_response(response)
        except Exception as e:
            logger.error(f"LLM analysis error: {e}")
            # Фолбэк на модель или словарный анализ
            if self.model_available:
                return self.model_analyze(text)
            else:
                return self.dictionary_analyze(text)
    
    def _parse_llm_response(self, response_text: str) -> Dict[str, Any]:
        """Парсинг ответа LLM"""
        try:
            # Извлечение JSON
            match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if match:
                json_str = match.group(0)
                data = json.loads(json_str)
                
                label = data.get("label", "neutral").lower()
                score = float(data.get("score", 0.5))
                explanation = data.get("explanation", "")
                
                # Нормализация меток
                if "positive" in label:
                    label = "positive"
                elif "negative" in label:
                    label = "negative"
                else:
                    label = "neutral"
                
                return {
                    "label": label,
                    "score": min(max(score, 0.1), 0.9),
                    "explanation": explanation,
                    "method": "llm"
                }
            
            # Резервный анализ текста ответа
            if "положительн" in response_text.lower() or "позитивн" in response_text.lower():
                return {"label": "positive", "score": 0.7, "method": "llm"}
            elif "отрицательн" in response_text.lower() or "негативн" in response_text.lower():
                return {"label": "negative", "score": 0.7, "method": "llm"}
            else:
                return {"label": "neutral", "score": 0.5, "method": "llm"}
        except Exception as e:
            logger.error(f"Error parsing LLM response: {e}")
            return {"label": "neutral", "score": 0.5, "method": "llm_error"}
    
    def analyze(self, text: str, use_llm: bool = False, entity: Optional[str] = None) -> Dict[str, Any]:
        """
        Анализ тональности текста с выбором метода.
        
        Args:
            text: Текст для анализа
            use_llm: Использовать LLM вместо модели трансформеров
            entity: Сущность, тональность которой нужно определить
            
        Returns:
            Результат анализа тональности
        """
        # Проверка входных данных
        if not text or not isinstance(text, str) or len(text.strip()) < 10:
            return {"label": "neutral", "score": 0.5, "method": "default"}
        
        # Создание ключа кэша
        cache_key = f"{text[:100]}_{use_llm}_{entity}"
        
        # Проверка кэша
        if cache_key in self.cache:
            self.cache_hits += 1
            return self.cache[cache_key].copy()
        
        # Предобработка текста
        processed_text = self._preprocess_text(text)
        
        # Выбор метода анализа
        if use_llm and self.llm_available:
            result = self.llm_analyze(processed_text, entity)
        elif self.model_available:
            result = self.model_analyze(processed_text)
        else:
            result = self.dictionary_analyze(processed_text)
        
        # Добавление метаданных
        result["timestamp"] = datetime.now().isoformat()
        
        # Добавление информации о сущности, если указана
        if entity:
            result["entity"] = entity
        
        # Сохранение в кэш
        self.cache[cache_key] = result.copy()
        
        # Очистка кэша, если он слишком большой
        if len(self.cache) > self.cache_size_limit:
            # Оставляем только последние 5000 записей
            self.cache = {k: self.cache[k] for k in list(self.cache.keys())[-5000:]}
        
        return result
    
    def batch_analyze(self, texts: List[str], use_llm: bool = False, batch_size: int = 16) -> List[Dict[str, Any]]:
        """
        Пакетный анализ нескольких текстов.
        
        Args:
            texts: Список текстов для анализа
            use_llm: Использовать LLM для анализа
            batch_size: Размер пакета для обработки
            
        Returns:
            Список результатов анализа
        """
        results = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i+batch_size]
            batch_results = []
            for text in batch:
                result = self.analyze(text, use_llm=use_llm)
                batch_results.append(result)
            results.extend(batch_results)
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Получение статистики использования анализатора"""
        return {
            "model_calls": self.model_calls,
            "llm_calls": self.llm_calls,
            "dictionary_calls": self.dictionary_calls,
            "total_calls": self.model_calls + self.llm_calls + self.dictionary_calls,
            "cache_hits": self.cache_hits,
            "cache_size": len(self.cache),
            "llm_available": self.llm_available,
            "model_available": self.model_available
        }
    
    def clear_cache(self) -> None:
        """Очистка кэша"""
        self.cache = {}
        logger.info("Sentiment analyzer cache cleared")


class EntityMentionAnalyzer:
    """Модуль для анализа упоминаний отслеживаемых сущностей"""
    
    def __init__(self, postgres_connection, sentiment_analyzer=None, 
                 use_local_llm=True, local_llm_url="http://llm-service:8000/generate"):
        """
        Инициализация анализатора упоминаний сущностей
        
        Args:
            postgres_connection: Соединение с базой данных PostgreSQL
            sentiment_analyzer: Экземпляр SentimentAnalyzer или None
            use_local_llm: Использовать локальный LLM
            local_llm_url: URL локального LLM API
        """
        self.postgres = postgres_connection
        
        # Инициализация анализатора тональности
        if sentiment_analyzer:
            self.sentiment_analyzer = sentiment_analyzer
        else:
            self.sentiment_analyzer = SentimentAnalyzer(
                use_local_llm=use_local_llm,
                local_llm_url=local_llm_url
            )
        
        # Загрузка списка отслеживаемых сущностей
        self.entities = self._load_entities()
        
        # Статистика
        self.mentions_found = 0
    
    def _load_entities(self):
        """Загрузка списка отслеживаемых сущностей из базы данных"""
        query = "SELECT id, name, synonyms FROM tracked_entities WHERE active = TRUE"
        try:
            cursor = self.postgres.cursor()
            cursor.execute(query)
            entities = []
            for row in cursor.fetchall():
                entities.append({
                    "id": row[0],
                    "name": row[1],
                    "synonyms": row[2] if row[2] else []
                })
            cursor.close()
            return entities
        except Exception as e:
            logger.error(f"Error loading entities: {e}")
            return []
    
    def refresh_entities(self):
        """Обновление списка отслеживаемых сущностей"""
        self.entities = self._load_entities()
    
    def find_entity_mentions(self, message):
        """
        Поиск упоминаний всех отслеживаемых сущностей в сообщении
        
        Args:
            message: Словарь с информацией о сообщении (id, text, channel_id, timestamp)
            
        Returns:
            Список обнаруженных упоминаний
        """
        if not message or not message.get("text"):
            return []
        
        text = message["text"]
        mentions = []
        
        for entity in self.entities:
            # Проверка основного имени сущности
            name_variants = [entity["name"]] + entity.get("synonyms", [])
            
            for variant in name_variants:
                if variant.lower() in text.lower():
                    # Нашли упоминание сущности
                    # Извлекаем контекст (100 символов до и после)
                    position = text.lower().find(variant.lower())
                    start = max(0, position - 100)
                    end = min(len(text), position + len(variant) + 100)
                    mention_text = text[start:end]
                    
                    # Анализируем тональность упоминания с учетом сущности
                    sentiment = self.sentiment_analyzer.analyze(
                        text=mention_text,
                        use_llm=True,  # Используем LLM для более точного анализа сущностей
                        entity=variant
                    )
                    
                    mentions.append({
                        "entity_id": entity["id"],
                        "entity_name": entity["name"],
                        "mention_text": mention_text,
                        "sentiment": sentiment,
                        "message_id": message.get("id"),
                        "channel_id": message.get("channel_id"),
                        "timestamp": message.get("timestamp")
                    })
                    
                    # После нахождения первого упоминания переходим к следующей сущности
                    self.mentions_found += 1
                    break
        
        return mentions
    
    def store_mentions(self, mentions):
        """
        Сохранение обнаруженных упоминаний в базе данных
        
        Args:
            mentions: Список обнаруженных упоминаний
            
        Returns:
            Количество сохраненных упоминаний
        """
        if not mentions:
            return 0
        
        query = """
        INSERT INTO entity_mentions 
        (entity_id, message_id, channel_id, mention_text, sentiment, timestamp, relevance_score) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        try:
            cursor = self.postgres.cursor()
            for mention in mentions:
                cursor.execute(query, (
                    mention["entity_id"],
                    mention["message_id"],
                    mention["channel_id"],
                    mention["mention_text"],
                    json.dumps(mention["sentiment"]),
                    mention["timestamp"],
                    mention["sentiment"].get("score", 0.5)
                ))
            self.postgres.commit()
            cursor.close()
            return len(mentions)
        except Exception as e:
            logger.error(f"Error storing mentions: {e}")
            self.postgres.rollback()
            return 0
    
    def generate_entity_report(self, entity_id, start_date, end_date):
        """
        Генерация отчета по упоминаниям сущности за указанный период
        
        Args:
            entity_id: ID сущности
            start_date: Начальная дата периода
            end_date: Конечная дата периода
            
        Returns:
            Словарь с отчетом
        """
        # Получение информации о сущности
        entity_query = "SELECT name FROM tracked_entities WHERE id = %s"
        
        # Получение упоминаний сущности за период
        mentions_query = """
        SELECT 
            mention_text, 
            sentiment, 
            channel_id, 
            timestamp 
        FROM 
            entity_mentions 
        WHERE 
            entity_id = %s AND timestamp BETWEEN %s AND %s
        ORDER BY timestamp
        """
        
        try:
            # Получение имени сущности
            cursor = self.postgres.cursor()
            cursor.execute(entity_query, (entity_id,))
            entity_name = cursor.fetchone()[0]
            
            # Получение упоминаний
            cursor.execute(mentions_query, (entity_id, start_date, end_date))
            mentions = []
            for row in cursor.fetchall():
                mentions.append({
                    "text": row[0],
                    "sentiment": json.loads(row[1]),
                    "channel_id": row[2],
                    "timestamp": row[3].isoformat() if hasattr(row[3], 'isoformat') else row[3]
                })
            
            cursor.close()
            
            # Группировка упоминаний по тональности
            positive_mentions = [m for m in mentions if m["sentiment"]["label"] == "positive"]
            negative_mentions = [m for m in mentions if m["sentiment"]["label"] == "negative"]
            neutral_mentions = [m for m in mentions if m["sentiment"]["label"] == "neutral"]
            
            # Формирование отчета
            report = {
                "entity_id": entity_id,
                "entity_name": entity_name,
                "start_date": start_date.isoformat() if hasattr(start_date, 'isoformat') else start_date,
                "end_date": end_date.isoformat() if hasattr(end_date, 'isoformat') else end_date,
                "total_mentions": len(mentions),
                "positive_mentions": len(positive_mentions),
                "negative_mentions": len(negative_mentions),
                "neutral_mentions": len(neutral_mentions),
                "positive_examples": positive_mentions[:3],
                "negative_examples": negative_mentions[:3]
            }
            
            # Генерация текстового резюме с использованием LLM, если есть достаточно упоминаний
            if len(mentions) >= 5 and self.sentiment_analyzer.llm_available:
                summary_text = self._generate_summary_with_llm(entity_name, mentions)
                if summary_text:
                    report["summary"] = summary_text
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating entity report: {e}")
            return {
                "entity_id": entity_id,
                "error": str(e)
            }
    
    def _generate_summary_with_llm(self, entity_name, mentions):
        """
        Генерация текстового резюме по упоминаниям с использованием LLM
        
        Args:
            entity_name: Имя сущности
            mentions: Список упоминаний
            
        Returns:
            Текстовое резюме или None в случае ошибки
        """
        try:
            # Выбор наиболее показательных примеров (до 10 упоминаний)
            examples = []
            positive_examples = [m for m in mentions if m["sentiment"]["label"] == "positive"]
            negative_examples = [m for m in mentions if m["sentiment"]["label"] == "negative"]
            
            # Добавляем до 5 положительных примеров
            for mention in positive_examples[:5]:
                examples.append(f"ПОЛОЖИТЕЛЬНОЕ УПОМИНАНИЕ: «{mention['text']}»")
            
            # Добавляем до 5 отрицательных примеров
            for mention in negative_examples[:5]:
                examples.append(f"ОТРИЦАТЕЛЬНОЕ УПОМИНАНИЕ: «{mention['text']}»")
            
            examples_text = "\n\n".join(examples)
            
            # Формирование промпта для LLM
            prompt = f"""
            Проанализируй упоминания сущности "{entity_name}" в новостных сообщениях из Telegram-каналов.
            
            Статистика:
            - Всего упоминаний: {len(mentions)}
            - Положительных: {len(positive_examples)}
            - Отрицательных: {len(negative_examples)}
            - Нейтральных: {len(mentions) - len(positive_examples) - len(negative_examples)}
            
            Примеры упоминаний:
            {examples_text}
            
            Составь краткое резюме (3-5 предложений) о том, как упоминается сущность "{entity_name}" 
            в Telegram-каналах за указанный период. Отметь основные темы, тональность и какие аспекты 
            сущности упоминаются в положительном и отрицательном контексте.
            """
            
            # Вызов локального LLM для генерации резюме
            response = self.sentiment_analyzer._call_local_llm(prompt)
            
            # Очистка ответа (удаление markdown форматирования, которое может добавить LLM)
            clean_response = re.sub(r'```.*?```', '', response, flags=re.DOTALL)
            clean_response = re.sub(r'#+\s*', '', clean_response)
            clean_response = clean_response.strip()
            
            return clean_response
            
        except Exception as e:
            logger.error(f"Error generating summary with LLM: {e}")
            return None
