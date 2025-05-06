FROM python:3.10-slim

WORKDIR /app

# Установка необходимых пакетов
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN CMAKE_ARGS="-DLLAMA_BLAS=ON -DLLAMA_BLAS_VENDOR=OpenBLAS" pip install --no-cache-dir llama-cpp-python==0.2.54

COPY . .

RUN mkdir -p /app/models

ENV MODEL_PATH=/app/models/model.gguf
ENV LLAMA_PORT=8080
ENV MAX_RETRIES=3
ENV RETRY_DELAY=5
ENV LLAMA_TIMEOUT=60
ENV CONTEXT_SIZE=4096
ENV MODEL_TYPE=llama
ENV GPU_LAYERS=0

COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Запуск сервиса
CMD ["python", "server.py"]
