import logging
import re
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

def clean_text(text):
    """Очистка текста от специальных символов, ссылок и т.д."""
    if not text:
        return ""
    
    # Удаление URL
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    
    # Удаление упоминаний и хэштегов
    text = re.sub(r'@\S+|#\S+', '', text)
    
    # Удаление лишних пробелов
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def extract_hashtags(text):
    """Извлечение хэштегов из текста"""
    if not text:
        return []
    
    hashtags = re.findall(r'#(\w+)', text)
    return hashtags

def extract_mentions(text):
    """Извлечение упоминаний из текста"""
    if not text:
        return []
    
    mentions = re.findall(r'@(\w+)', text)
    return mentions

def normalize_date(date_str):
    """Нормализация строки даты в единый формат"""
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.astimezone(pytz.UTC)
    except:
        logger.error(f"Error normalizing date: {date_str}")
        return None
