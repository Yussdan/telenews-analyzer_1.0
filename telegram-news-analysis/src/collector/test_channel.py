from telethon.sync import TelegramClient
import os
import sys

try:
    from dotenv import load_dotenv
    load_dotenv('../.env')
except ImportError:
    print("python-dotenv не установлен. Загрузка .env файла не выполнена.")

# Получение API ID и HASH из переменных окружения или ввод вручную
API_ID = os.getenv("TELEGRAM_API_ID") or input("Введите ваш API ID: ")
API_HASH = os.getenv("TELEGRAM_API_HASH") or input("Введите ваш API Hash: ")
SESSION_NAME = 'teeeeest'

# Получаем список каналов из переменной окружения
raw_channels = os.getenv("TELEGRAM_CHANNELS", "").split(",")
channels_to_test = []
for channel in raw_channels:
    channel = channel.strip()
    if channel:
        if not channel.startswith('@'):
            channel = '@' + channel
        channels_to_test.append(channel)

# Если список пуст, добавляем некоторые стандартные каналы для теста
if not channels_to_test:
    channels_to_test = ['@telegram', '@durov', '@bbcrussian']

print(f"Тестирование доступа к каналам: {', '.join(channels_to_test)}")

# Создаем директорию для хранения сессий, если она не существует
os.makedirs("sessions", exist_ok=True)
session_path = '/Users/yussdan/work/telenews-analyzer/telegram-news-analysis/src/collector/telegram_news_session.session'

try:
    # Создание клиента и запуск авторизации
    client = TelegramClient(session_path, API_ID, API_HASH)
    client.start()
    
    if client.is_user_authorized():
        print(f"Аутентификация успешна! Сессия сохранена в файл 'sessions/teeeeest.session'")
        
        # Проверка доступа к каналам
        print("\nПроверка доступа к указанным каналам:")
        for channel in channels_to_test:
            try:
                entity = client.get_entity(channel)
                print(f" ✓ Канал {channel} найден - {entity.title} (ID: {entity.id})")
                
                # Проверка возможности получения сообщений
                messages = client.get_messages(entity, limit=1)
                if messages and len(messages) > 0:
                    print(f"   ✓ Получено сообщение: '{messages[0].text[:50]}...'")
                else:
                    print(f"   ✗ Не удалось получить сообщения из канала {channel}")
                
            except Exception as e:
                print(f" ✗ Ошибка доступа к каналу {channel}: {e}")
                
        # Предложение популярных каналов
        print("\nПоиск и предложение популярных каналов:")
        popular_channels = ['telegram', 'durov', 'bbcrussian', 'breakingmash', 'tjournal', 'meduzalive']
        
        for channel in popular_channels:
            try:
                formatted_channel = f"@{channel}"
                entity = client.get_entity(formatted_channel)
                print(f" ✓ Канал {formatted_channel} доступен - {entity.title}")
            except Exception as e:
                print(f" ✗ Канал {formatted_channel} недоступен: {e}")
    else:
        print("Ошибка: Авторизация не удалась")
        
except Exception as e:
    print(f"Ошибка: {e}")
    sys.exit(1)
finally:
    client.disconnect()

print("\nИнструкции для запуска контейнера:")
print(f"1. Убедитесь, что файл sessions/{SESSION_NAME}.session существует")
print(f"2. Обновите переменную TELEGRAM_CHANNELS в .env файле, используя доступные каналы")
print("3. Запустите: podman-compose up -d collector")
