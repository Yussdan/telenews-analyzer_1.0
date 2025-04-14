import logging
import re
from collections import Counter
from pymorphy2 import MorphAnalyzer
from transformers import pipeline, AutoModelForSequenceClassification, AutoTokenizer

logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    def __init__(self, positive_words=None, negative_words=None):
        """Инициализация анализатора тональности"""
        self.morph = MorphAnalyzer()
        self.hf_available = False

        try:
            model_name = "blanchefort/rubert-base-cased-sentiment"
            model = AutoModelForSequenceClassification.from_pretrained(model_name)
            tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = pipeline('sentiment-analysis', model=model, tokenizer=tokenizer)
            self.hf_available = True
        except Exception as e:
            logger.warning(f"Hugging Face model loading failed: {e}")

        default_positive = {
            "хорошо", "отлично", "превосходно", "замечательно", "успех",
            "прекрасно", "радость", "удовольствие", "позитивно", "рост",
            "достижение", "улучшение", "победа", "выигрыш", "награда",
            "благодарность", "счастье", "довольный", "полезный", "поддержка",
            "феноменальный", "гений", "красиво", "рекорд", "побил", "акция",
            "удачно", "вдохновляет", "впечатляет", "превосходит"
        }

        default_negative = {
            "плохо", "ужасно", "отвратительно", "провал", "неудача",
            "печально", "разочарование", "негативно", "кризис", "падение",
            "проблема", "ухудшение", "поражение", "потеря", "наказание",
            "страх", "тревога", "грусть", "вред", "опасность",
            "неудачный", "провальный", "разочаровывает", "отсутствие"
        }

        self.positive_words = default_positive.union(positive_words or set())
        self.negative_words = default_negative.union(negative_words or set())

    def dictionary_analyze(self, text):
        """Анализ на основе словаря"""
        text = text.lower()
        words = re.findall(r'\b\w+\b', text)

        if not words:
            return {"label": "neutral", "score": 0.5}

        lemmatized = []
        for word in words:
            try:
                parsed = self.morph.parse(word)[0]
                lemmatized.append(parsed.normal_form)
            except Exception as e:
                logger.warning(f"Could not parse word '{word}': {e}")
                lemmatized.append(word)

        positive_count = sum(1 for word in lemmatized if word in self.positive_words)
        negative_count = sum(1 for word in lemmatized if word in self.negative_words)
        total = len(lemmatized)

        if total == 0:
            return {"label": "neutral", "score": 0.5}

        if positive_count > negative_count:
            label = "positive"
            score = 0.5 + (positive_count / total) * 0.5
        elif negative_count > positive_count:
            label = "negative"
            score = 0.5 + (negative_count / total) * 0.5
        else:
            label = "neutral"
            score = 0.5

        return {"label": label, "score": min(max(score, 0.1), 0.9)}

    def hf_analyze(self, text):
        """Анализ с использованием Hugging Face"""
        try:
            sentences = re.split(r'[.!?]+', text)
            sentences = [s.strip() for s in sentences if len(s.strip()) > 10]

            if not sentences:
                return {"label": "neutral", "score": 0.5}

            results = []
            for sent in sentences:
                try:
                    res = self.model(sent)[0]
                    results.append(res)
                except Exception as e:
                    logger.warning(f"Failed to analyze sentence: '{sent}' — {e}")

            if not results:
                return self.dictionary_analyze(text)

            labels = [res['label'] for res in results]
            scores = [res['score'] for res in results]
            label_counts = Counter(labels)
            label = label_counts.most_common(1)[0][0]
            label_scores = [res['score'] for res in results if res['label'] == label]
            avg_score = sum(label_scores) / len(label_scores)

            return {"label": label.lower(), "score": round(avg_score, 4)}

        except Exception as e:
            logger.error(f"HF analysis error: {e}")
            return self.dictionary_analyze(text)

    def analyze(self, text):
        """Финальный анализ с fallback-логикой"""
        if not text or not isinstance(text, str):
            return {"label": "neutral", "score": 0.5}

        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        text = re.sub(r'\@\w+|\#\w+', '', text)
        text = re.sub(r'[^\w\s]', ' ', text)
        text = ' '.join(text.split())

        if len(text) < 10:
            return {"label": "neutral", "score": 0.5}

        if self.hf_available:
            return self.hf_analyze(text)
        else:
            return self.dictionary_analyze(text)
