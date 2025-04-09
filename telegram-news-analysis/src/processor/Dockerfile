FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
RUN python -m spacy download ru_core_news_md

COPY . .

CMD ["python", "news_processor.py"]


