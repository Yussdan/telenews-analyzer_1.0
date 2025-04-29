FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    curl \
    openjdk-17-jdk \
    git \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir -r requirements.txt

RUN python -m spacy download ru_core_news_sm

RUN python -c "import nltk; nltk.download('punkt')" || true

ENV TRANSFORMERS_CACHE=/app/hf_cache
RUN mkdir -p $TRANSFORMERS_CACHE


COPY . .

CMD ["python", "news_processor.py"]
