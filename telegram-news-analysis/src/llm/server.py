# Обновленный server.py для llm-service
import os
import subprocess
import time
import logging
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
import uvicorn
import requests
import asyncio
from fastapi.middleware.cors import CORSMiddleware

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("llm-service")

app = FastAPI()

# Добавляем CORS для возможного веб-интерфейса
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Переменные и пути
LLAMA_SERVER_PATHS = [
    "/app/llama.cpp/server",
    "/app/llama.cpp/build/bin/llama-server"
]
MODEL_PATH = os.environ.get("MODEL_PATH", "/app/models/mistral-7b-instruct-q4_0.gguf")
LLAMA_PORT = int(os.environ.get("LLAMA_PORT", "8080"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.environ.get("RETRY_DELAY", "5"))
LLAMA_TIMEOUT = int(os.environ.get("LLAMA_TIMEOUT", "60"))
MODEL_READY = False

# Кэш для ускорения повторных запросов
PROMPT_CACHE = {}
CACHE_SIZE_LIMIT = 100

def find_llama_server():
    for path in LLAMA_SERVER_PATHS:
        if os.path.exists(path):
            return path
    raise FileNotFoundError("Could not find llama.cpp server executable")

def start_llama_server():
    server_path = find_llama_server()
    
    cmd = [
        server_path,
        "-m", MODEL_PATH,
        "-c", "2048",
        "--host", "0.0.0.0",
        "--port", str(LLAMA_PORT),
        "-t", "4",
        "--ctx-size", "2048"
    ]
    
    try:
        logger.info(f"Starting llama.cpp server with command: {' '.join(cmd)}")
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Ждем некоторое время для запуска
        time.sleep(2)
        if process.poll() is not None:
            stderr = process.stderr.read().decode()
            raise RuntimeError(f"Server failed to start: {stderr}")
            
        return process
    except Exception as e:
        logger.error(f"Failed to start llama server: {str(e)}")
        raise RuntimeError(f"Failed to start llama server: {str(e)}")

class GenerationRequest(BaseModel):
    prompt: str
    max_tokens: Optional[int] = 200
    temperature: Optional[float] = 0.7
    cache_key: Optional[str] = None
    is_report: bool = False

class GenerationResponse(BaseModel):
    generated_text: str
    cached: bool = False
    error: Optional[str] = None

async def check_model_readiness():
    global MODEL_READY
    max_attempts = 10
    for attempt in range(max_attempts):
        try:
            # Проверка, что llama сервер отвечает
            response = requests.get(f"http://localhost:{LLAMA_PORT}/health", timeout=5)
            if response.status_code == 200:
                # Тестовый запрос генерации
                test_req = {
                    "prompt": "test",
                    "n_predict": 1,
                    "temperature": 0.7
                }
                test_resp = requests.post(
                    f"http://localhost:{LLAMA_PORT}/completion", 
                    json=test_req,
                    timeout=10
                )
                if test_resp.status_code == 200:
                    MODEL_READY = True
                    logger.info("Model is ready and fully initialized!")
                    return
        except Exception as e:
            logger.warning(f"Model readiness check attempt {attempt+1}/{max_attempts} failed: {str(e)}")
        
        # Увеличивающиеся интервалы ожидания
        wait_time = 5 + attempt * 5
        logger.info(f"Waiting {wait_time}s before next readiness check...")
        await asyncio.sleep(wait_time)
    
    logger.warning("Could not confirm model readiness after multiple attempts. Will try to serve requests anyway.")
    MODEL_READY = True 

@app.on_event("startup")
async def startup_event():
    try:
        app.state.llama_process = start_llama_server()
        # Запускаем проверку готовности в фоне
        asyncio.create_task(check_model_readiness())
    except Exception as e:
        logger.error(f"Failed to initialize LLM service: {str(e)}")
        raise RuntimeError(f"Failed to initialize LLM service: {str(e)}")

@app.post("/generate", response_model=GenerationResponse)
async def generate(request: GenerationRequest):
    # Проверка готовности модели
    if not MODEL_READY:
        logger.warning("Model not yet ready, returning early response")
        return GenerationResponse(
            generated_text="",
            error="Model is still initializing, please try again in a few moments."
        )
    
    # Проверка кэша
    cache_key = request.cache_key or request.prompt[:100]
    if cache_key in PROMPT_CACHE:
        logger.info(f"Cache hit for prompt beginning with: {request.prompt[:20]}...")
        return GenerationResponse(
            generated_text=PROMPT_CACHE[cache_key],
            cached=True
        )
    
    # Укорачиваем длинные промпты
    if not request.is_report:
        safe_prompt = request.prompt
        if len(safe_prompt) > 1000:
            logger.warning(f"Long prompt detected ({len(safe_prompt)} chars), truncating to 1000 chars")
            safe_prompt = safe_prompt[:1000] + "..."
    
    llama_request = {
        "prompt": safe_prompt,
        "n_predict": min(request.max_tokens, 500),
        "temperature": request.temperature,
        "stop": ["</s>", "user:", "User:"]
    }
    
    # Используем retry логику
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                f"http://localhost:{LLAMA_PORT}/completion",
                json=llama_request,
                timeout=LLAMA_TIMEOUT
            )
            
            if response.status_code == 200:
                generated_text = response.json().get("content", "")
                
                # Кэширование результата
                if len(PROMPT_CACHE) >= CACHE_SIZE_LIMIT:
                    # Простая стратегия вытеснения: удаляем первый элемент
                    PROMPT_CACHE.pop(next(iter(PROMPT_CACHE)))
                PROMPT_CACHE[cache_key] = generated_text
                
                return GenerationResponse(generated_text=generated_text)
            else:
                logger.warning(f"LLM request attempt {attempt+1} returned status code {response.status_code}")
        except requests.exceptions.Timeout:
            logger.warning(f"LLM request attempt {attempt+1} timed out after {LLAMA_TIMEOUT}s")
        except Exception as e:
            logger.error(f"LLM request attempt {attempt+1} error: {str(e)}")
        
        # Не ждем перед последней попыткой
        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(RETRY_DELAY)
    
    # Если все попытки не удались, возвращаем пустую строку с ошибкой
    logger.error("All LLM request attempts failed")
    return GenerationResponse(
        generated_text="",
        error="LLM service failed to generate text after multiple attempts"
    )

@app.get("/health")
async def health_check():
    llama_status = "unknown"
    try:
        response = requests.get(f"http://localhost:{LLAMA_PORT}/health", timeout=5)
        llama_status = "running" if response.ok else "unhealthy"
    except:
        llama_status = "down"
    
    return {
        "status": "ok", 
        "llm_status": llama_status,
        "model_ready": MODEL_READY,
        "cache_size": len(PROMPT_CACHE)
    }

@app.get("/clear-cache")
async def clear_cache():
    global PROMPT_CACHE
    cache_size = len(PROMPT_CACHE)
    PROMPT_CACHE = {}
    return {"status": "ok", "cleared_entries": cache_size}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
