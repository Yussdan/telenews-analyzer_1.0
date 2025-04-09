FROM python:3.9-slim

WORKDIR /app

# Установка необходимых системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*


RUN mkdir -p /app/sessions
RUN chmod -R 777 /app/sessions
COPY telegram_news_session.session /app/sessions/telegram_news_session.session
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .

CMD ["python", "telegram_collector.py"]
