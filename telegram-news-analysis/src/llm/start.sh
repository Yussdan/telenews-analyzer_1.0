#!/bin/bash

# Переменные окружения
MODEL_PATH=${MODEL_PATH:-"/app/models/mistral-7b-instruct-q4_0.gguf"}
LLAMA_PORT=${LLAMA_PORT:-8080}
MAX_RETRIES=${MAX_RETRIES:-3}
RETRY_DELAY=${RETRY_DELAY:-5}
LLAMA_TIMEOUT=${LLAMA_TIMEOUT:-60}

# Проверка модели
if [ ! -f "$MODEL_PATH" ]; then
    echo "Error: Model not found at $MODEL_PATH"
    echo "Available models:"
    ls -l /app/models/ 2>/dev/null || echo "No models found in /app/models/"
    exit 1
fi

# Экспорт переменных для использования в Python
export MODEL_PATH
export LLAMA_PORT
export MAX_RETRIES
export RETRY_DELAY
export LLAMA_TIMEOUT

# Запуск FastAPI
echo "Starting FastAPI service on port 8000"
echo "LLM server will be started automatically"
exec python3 -m uvicorn server:app --host 0.0.0.0 --port 8000 --log-level info
