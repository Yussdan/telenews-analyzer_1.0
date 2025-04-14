FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    curl \
    openjdk-17-jdk \
    procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN pip install --no-cache-dir -r requirements.txt
RUN python -m spacy download ru_core_news_sm

COPY . .
CMD ["python", "news_processor.py"]


