import logging
import re
from pymorphy2 import MorphAnalyzer
from transformers import pipeline, AutoModelForSequenceClassification, AutoTokenizer

logger = logging.getLogger(__name__)

class SentimentAnalyzer:
    def __init__(self):
        """Инициализация анализатора тональности"""
        
        self.morph = MorphAnalyzer()
        self.hf_available = False
        
        try:
            # Используем готовую модель для анализа тональности на русском языке
            model_name = "blanchefort/rubert-base-cased-sentiment"
            model = AutoModelForSequenceClassification.from_pretrained(model_name)
            tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = pipeline('sentiment-analysis', model=model, tokenizer=tokenizer)
            self.hf_available = True
        except Exception as e:
            logger.warning(f"Hugging Face model loading failed: {e}")

        self.positive_words = set([
            "хорошо", "отлично", "превосходно", "замечательно", "успех", 
            "прекрасно", "радость", "удовольствие", "позитивно", "рост",
            "достижение", "улучшение", "победа", "выигрыш", "награда",
            "благодарность", "счастье", "довольный", "полезный", "поддержка",
            "феноменальный", "гений", "красиво", "рекорд", "побил", "акция", 
            "удачно", "вдохновляет", "впечатляет", "превосходит", "успех"
        ])

        self.negative_words = set([
            "плохо", "ужасно", "отвратительно", "провал", "неудача",
            "печально", "разочарование", "негативно", "кризис", "падение",
            "проблема", "ухудшение", "поражение", "потеря", "наказание",
            "страх", "тревога", "грусть", "вред", "опасность",
            "неудачный", "провальный", "разочаровывает", "отсутствие"
        ])

    def dictionary_analyze(self, text):
        """Улучшенный анализ тональности на основе словаря"""
        text = text.lower()
        words = re.findall(r'\b\w+\b', text)
        
        if not words:
            return {"label": "neutral", "score": 0.5}
        
        # Лемматизация с обработкой исключений
        lemmatized = []
        for word in words:
            try:
                parsed = self.morph.parse(word)[0]
                lemmatized.append(parsed.normal_form)
            except:
                lemmatized.append(word)
        
        positive_count = sum(1 for word in lemmatized 
                           if any(word.startswith(root) for root in self.positive_words))
        negative_count = sum(1 for word in lemmatized 
                           if any(word.startswith(root) for root in self.negative_words))
        
        total = len(lemmatized)
        if total == 0:
            return {"label": "neutral", "score": 0.5}
        
        # Более сбалансированная оценка
        positive_score = positive_count / total * 0.5
        negative_score = negative_count / total * 0.5
        
        if positive_score > negative_score + 0.1:
            label = "positive"
            score = 0.5 + positive_score
        elif negative_score > positive_score + 0.1:
            label = "negative"
            score = 0.5 + negative_score
        else:
            label = "neutral"
            score = 0.5
            
        return {"label": label, "score": min(max(score, 0.1), 0.9)}

    def hf_analyze(self, text):
        """Улучшенный анализ с использованием Hugging Face"""
        try:
            # Обработка длинного текста
            sentences = re.split(r'[.!?]+', text)
            sentences = [s.strip() for s in sentences if len(s) > 10]
            
            if not sentences:
                return {"label": "neutral", "score": 0.5}

            # Анализ всех предложений
            results = []
            for sent in sentences:
                try:
                    res = self.model(sent)[0]
                    results.append(res)
                except:
                    continue

            if not results:
                return self.dictionary_analyze(text)

            # Агрегация результатов
            pos_scores = []
            neg_scores = []
            neu_scores = []
            
            for res in results:
                if res['label'] == 'POSITIVE':
                    pos_scores.append(res['score'])
                elif res['label'] == 'NEGATIVE':
                    neg_scores.append(res['score'])
                else:
                    neu_scores.append(res['score'])
            
            avg_pos = sum(pos_scores)/len(pos_scores) if pos_scores else 0
            avg_neg = sum(neg_scores)/len(neg_scores) if neg_scores else 0
            avg_neu = sum(neu_scores)/len(neu_scores) if neu_scores else 0
            
            # Определение доминирующего настроения
            if avg_pos > max(avg_neg, avg_neu) + 0.15:
                return {"label": "positive", "score": avg_pos}
            elif avg_neg > max(avg_pos, avg_neu) + 0.15:
                return {"label": "negative", "score": avg_neg}
            else:
                return {"label": "neutral", "score": avg_neu}

        except Exception as e:
            logger.error(f"HF analysis error: {e}")
            return self.dictionary_analyze(text)

    def analyze(self, text):
        """Анализ тональности текста с улучшенной обработкой"""
        if not text or not isinstance(text, str):
            return {"label": "neutral", "score": 0.5}

        # Предварительная очистка текста
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
