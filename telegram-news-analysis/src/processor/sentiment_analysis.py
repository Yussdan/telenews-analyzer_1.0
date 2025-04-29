import logging
import re
from collections import Counter
from pymorphy2 import MorphAnalyzer
from transformers import pipeline

logger = logging.getLogger(__name__)

# Дополняйте этот список для вашей доменной области!
DEFAULT_NEGATIVE = {
    "плохо", "ужасно", "отвратительно", "провал", "неудача", "кризис", "катастрофа",
    "болезнь", "сбой", "апатия", "гнев", "грусть", "жалоба", "бесправие",
    "печаль", "разочарование", "негативно", "падение", "проблема", "ухудшение", "поражение",
    "потеря", "наказание", "страх", "тревога", "вред", "опасность", "беспокойство",
    "загрязнение", "выброс", "ограничение", "закрытие", "критикует", "критикующий", "требовать", "кричащий", "беспокойство", "рекордный", "санкция", "карантин", "штраф", "отмена", "отказ", "срыв", "блокировка", "воровство", "инцидент"
}

DEFAULT_POSITIVE = {
    "хорошо", "отлично", "превосходно", "замечательно", "успех", "прекрасно", "радость",
    "удовольствие", "позитивно", "рост", "достижение", "улучшение", "победа", "выигрыш", "награда",
    "благодарность", "счастье", "довольный", "полезный", "поддержка", "феноменальный", "гений",
    "красиво", "рекорд", "удачно", "вдохновляет", "впечатляет", "превосходит"
}

class SentimentAnalyzer:
    def __init__(self, positive_words=None, negative_words=None):
        self.morph = MorphAnalyzer()
        self.hf_available = False

        try:
            model_name = "cointegrated/rubert-tiny2-ru-sentiment"
            self.model = pipeline('sentiment-analysis', model=model_name, tokenizer=model_name)
            self.hf_available = True
        except Exception as e:
            logger.warning(f"Hugging Face model loading failed: {e}")

        self.positive_words = (positive_words or set()).union(DEFAULT_POSITIVE)
        self.negative_words = (negative_words or set()).union(DEFAULT_NEGATIVE)

    def dictionary_analyze(self, text):
        """Словарный анализ с лемматизацией"""
        text = text.lower()
        words = re.findall(r'\b\w+\b', text)
        if not words:
            return {"label": "neutral", "score": 0.5}

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
            return {"label": "neutral", "score": 0.5}

        if positive_count > negative_count:
            label, score = "positive", 0.5 + (positive_count / total) * 0.5
        elif negative_count > positive_count:
            label, score = "negative", 0.5 + (negative_count / total) * 0.5
        else:
            label, score = "neutral", 0.5

        return {"label": label, "score": min(max(score, 0.1), 0.99)}

    def hf_analyze(self, text):
        """Анализ нейросетью. Без split на предложения!"""
        try:
            result = self.model(text)
            if isinstance(result, list):
                result = result[0]
            label = result.get("label", "neutral").lower()
            score = result.get("score", 0.5)
            return {"label": label, "score": float(score)}
        except Exception as e:
            logger.error(f"HF analysis error: {e}")
            return self.dictionary_analyze(text)

    def analyze(self, text):
        """Интегральный анализ: HF + словарь"""
        if not text or not isinstance(text, str) or len(text.strip()) < 10:
            return {"label": "neutral", "score": 0.5}

        text = re.sub(r'http\S+|www\S+|https\S+', '', text)
        text = re.sub(r'\@\w+|\#\w+', '', text)
        text = re.sub(r'[^\w\s]', ' ', text)
        text = ' '.join(text.split())

        if not self.hf_available:
            return self.dictionary_analyze(text)

        base = self.hf_analyze(text)
        dict_res = self.dictionary_analyze(text)
        if base["label"] == "neutral" and dict_res["label"] in ("negative", "positive") and dict_res["score"] > 0.6:
            base["label"] = dict_res["label"]
            base["score"] = max(base["score"], dict_res["score"])

        return base
