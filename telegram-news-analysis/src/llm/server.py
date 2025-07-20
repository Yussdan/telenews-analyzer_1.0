import os
import gc
import time
import logging
import threading
from flask import Flask, request, jsonify
import json
import psutil

try:
    from llama_cpp import Llama
except Exception as e:
    print("Failed to import llama_cpp:", e)
    raise

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Конфигурация модели
MODEL_PATH = os.getenv("MODEL_PATH", "/app/models/model.gguf")
CONTEXT_SIZE = int(os.getenv("CONTEXT_SIZE", "1024"))
MODEL_TYPE = os.getenv("MODEL_TYPE", "llama").lower()
GPU_LAYERS = int(os.getenv("GPU_LAYERS", "0"))
LLAMA_THREADS = int(os.getenv("LLAMA_THREADS", "6"))
LLAMA_BATCH_SIZE = int(os.getenv("LLAMA_BATCH_SIZE", "512"))
LLAMA_CPU_OFFLOAD = int(os.getenv("LLAMA_CPU_OFFLOAD", "0")) > 0
GENERATION_TIMEOUT = int(os.getenv("GENERATION_TIMEOUT", "90"))
ROPE_FREQ_BASE = float(os.getenv("ROPE_FREQ_BASE", "10000.0"))
MAX_TOKENS_DEFAULT = int(os.getenv("MAX_TOKENS_DEFAULT", "512"))
USE_MLOCK = int(os.getenv("USE_MLOCK", "1")) > 0
MUL_MAT_Q = int(os.getenv("MUL_MAT_Q", "1")) > 0

# Параметры генерации по умолчанию
TFS_Z = float(os.getenv("TFS_Z", "1.0"))
TYPICAL_P = float(os.getenv("TYPICAL_P", "1.0"))
MIROSTAT_MODE = int(os.getenv("MIROSTAT_MODE", "0"))
MIROSTAT_TAU = float(os.getenv("MIROSTAT_TAU", "5.0"))
MIROSTAT_ETA = float(os.getenv("MIROSTAT_ETA", "0.1"))

# Системные параметры
OPTIMAL_CORE_COUNT = min(os.cpu_count() or 1, 6)
PHYSICAL_CORES = psutil.cpu_count(logical=False) or 1
REPORT_MAX_NEWS = int(os.getenv("REPORT_MAX_NEWS", "3"))

app = Flask(__name__)

model = None
model_lock = threading.RLock()
model_ready = False

class GenerationTimeout(Exception):
    pass

def cleanup_memory():
    """Очистка памяти и возврат неиспользуемой памяти системе"""
    gc.collect()
    if os.name == 'posix':
        try:
            import ctypes
            libc = ctypes.CDLL('libc.so.6')
            if hasattr(libc, 'malloc_trim'):
                libc.malloc_trim(0)
        except Exception as e:
            logger.debug(f"malloc_trim: {e}")

def load_model():
    """Загрузка и инициализация модели"""
    global model, model_ready
    with model_lock:
        try:
            if model is not None:
                del model
                model = None
                gc.collect()
                time.sleep(1)

            n_threads = min(LLAMA_THREADS, PHYSICAL_CORES)
            logger.info(f"Загрузка модели из {MODEL_PATH} с n_threads={n_threads}, n_batch={LLAMA_BATCH_SIZE}")
            
            params = {
                "model_path": MODEL_PATH,
                "n_ctx": CONTEXT_SIZE,
                "n_batch": LLAMA_BATCH_SIZE,
                "n_threads": n_threads,
                "n_gpu_layers": GPU_LAYERS,
                "f16_kv": True,
                "verbose": False,
                "offload_kqv": LLAMA_CPU_OFFLOAD,
                "embedding": False,
                "rope_freq_base": ROPE_FREQ_BASE,
                "use_mlock": USE_MLOCK,
                "mul_mat_q": MUL_MAT_Q,
            }

            available_memory = psutil.virtual_memory().available / (1024 * 1024 * 1024)
            logger.info(f"Доступная память: {available_memory:.2f} GB")

            model = Llama(**params)

            # Прогрев модели для стабильной работы
            for prompt in ["Проверка загрузки модели.", "Повторный прогрев модели для улучшения производительности."]:
                _ = model.create_completion(
                    prompt,
                    max_tokens=10,
                    temperature=0.1
                )
            
            logger.info(f"Модель {MODEL_TYPE} успешно загружена и оптимизирована!")
            model_ready = True
            return True
        except Exception as e:
            logger.error(f"Ошибка загрузки модели: {e}", exc_info=True)
            model_ready = False
            return False

def periodic_cleanup():
    """Периодическая очистка памяти"""
    while True:
        time.sleep(300)  # 5 минут
        logger.debug("Периодическая очистка памяти")
        cleanup_memory()

cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)
cleanup_thread.start()

@app.route("/health", methods=["GET"])
def health_check():
    """Проверка состояния сервиса"""
    cpu_percent = psutil.cpu_percent(interval=0.1)
    memory = psutil.virtual_memory()
    memory_percent = memory.percent
    available_memory_gb = memory.available / (1024 * 1024 * 1024)
    
    return jsonify({
        "status": "ready" if model_ready else "loading",
        "model_path": MODEL_PATH,
        "model_type": MODEL_TYPE,
        "context_size": CONTEXT_SIZE,
        "threads": LLAMA_THREADS,
        "batch_size": LLAMA_BATCH_SIZE,
        "gpu_layers": GPU_LAYERS,
        "cpu_offload": LLAMA_CPU_OFFLOAD,
        "system_stats": {
            "cpu_percent": cpu_percent,
            "memory_percent": memory_percent,
            "available_memory_gb": round(available_memory_gb, 2)
        }
    })

@app.route("/generate", methods=["POST"])
def generate():
    """Генерация текста с помощью модели"""
    global model, model_ready
    if not model_ready:
        return jsonify({"error": "Модель еще загружается", "status": "loading"}), 503

    try:
        data = request.json
        if not data:
            return jsonify({"error": "Отсутствуют входные данные"}), 400

        prompt = data.get("prompt", "")
        if not prompt:
            return jsonify({"error": "Пустой промпт"}), 400

        logger.info(f"Получен промпт (длина: {len(prompt)}): {prompt[:200]}...")

        # Настройка параметров генерации
        max_tokens = min(int(data.get("max_tokens", MAX_TOKENS_DEFAULT)), CONTEXT_SIZE - 100)
        temperature = float(data.get("temperature", 0.7))
        top_p = float(data.get("top_p", 0.9))
        top_k = int(data.get("top_k", 40))
        repeat_penalty = float(data.get("repeat_penalty", 1.1))
        presence_penalty = float(data.get("presence_penalty", 0.0))
        frequency_penalty = float(data.get("frequency_penalty", 0.0))
        stop_words = data.get("stop", None)

        is_report = data.get("is_report", False)

        # Оптимизированные параметры для отчетов
        if is_report:
            temperature = min(temperature, 0.6)
            top_p = 0.85
            repeat_penalty = 1.2
            presence_penalty = 0.1
            frequency_penalty = 0.2
            mirostat_mode = 2
            mirostat_tau = 5.0
            mirostat_eta = 0.1
        else:
            mirostat_mode = MIROSTAT_MODE
            mirostat_tau = MIROSTAT_TAU
            mirostat_eta = MIROSTAT_ETA

        # Проверка длины промпта
        max_prompt_length = CONTEXT_SIZE * 2
        if len(prompt) > max_prompt_length and not is_report:
            logger.warning(f"Промпт слишком длинный ({len(prompt)}), обрезаем.")
            prompt = prompt[:max_prompt_length]

        adjusted_timeout = GENERATION_TIMEOUT * 2
        cleanup_memory()

        with model_lock:
            try:
                start_time = time.time()
                logger.debug(f"Параметры генерации: max_tokens={max_tokens}, temp={temperature}, top_p={top_p}, repeat_penalty={repeat_penalty}")
                
                generation_params = {
                    "prompt": prompt,
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                    "top_p": top_p,
                    "top_k": top_k,
                    "repeat_penalty": repeat_penalty,
                    "presence_penalty": presence_penalty,
                    "frequency_penalty": frequency_penalty,
                    "mirostat_mode": mirostat_mode,
                    "mirostat_tau": mirostat_tau,
                    "mirostat_eta": mirostat_eta,
                    "tfs_z": TFS_Z,
                    "typical_p": TYPICAL_P,
                    "stop": stop_words
                }

                output = model.create_completion(**generation_params)
                generation_time = time.time() - start_time
                logger.debug(f"Полный ответ модели: {json.dumps(output, ensure_ascii=False)}")

                generated_text = output["choices"][0]["text"]

                logger.info(f"Сгенерировано {len(generated_text)} символов за {generation_time:.2f} сек.")
                logger.info(f"Ответ LLM (полностью): {generated_text}")

                cleanup_memory()

                return jsonify({
                    "generated_text": generated_text,
                    "model": MODEL_TYPE,
                    "generation_time": round(generation_time, 2),
                    "tokens_generated": output["usage"].get("completion_tokens", 0),
                    "tokens_total": output["usage"].get("total_tokens", 0)
                })

            except GenerationTimeout:
                logger.error("Превышено время генерации")
                return jsonify({
                    "error": "Превышено время генерации",
                    "timeout": adjusted_timeout
                }), 408

            except Exception as e:
                logger.error(f"Ошибка генерации: {e}", exc_info=True)
                return jsonify({"error": f"Ошибка генерации: {str(e)}"}), 500

    except Exception as e:
        logger.error(f"Ошибка запроса: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/reload", methods=["POST"])
def reload_model():
    """Перезагрузка модели"""
    global model_ready
    model_ready = False
    try:
        reload_thread = threading.Thread(target=load_model, daemon=True)
        reload_thread.start()
        return jsonify({"status": "reload_started", "message": "Перезагрузка модели запущена"})
    except Exception as e:
        logger.error(f"Ошибка при запуске перезагрузки: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    try:
        import os
        os.nice(-10)  # Повышаем приоритет процесса
    except:
        pass

    load_model()
    app.run(host="0.0.0.0", port=8000)
