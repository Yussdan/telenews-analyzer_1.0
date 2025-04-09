from telethon.sync import TelegramClient
import sys

# Получение учетных данных Telegram API
API_ID = input("Введите ваш API ID: ")
API_HASH = input("Введите ваш API Hash: ")
PHONE = input("Введите ваш номер телефона (с кодом страны, например +79123456789): ")

# Создание сессионного файла
SESSION_NAME = "telegram_session"

try:
    # Создание клиента и запуск интерактивной авторизации
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    client.start(phone=PHONE)
    
    if client.is_user_authorized():
        print(f"Аутентификация успешна! Сессия сохранена в файл '{SESSION_NAME}.session'")
        print("Скопируйте этот файл в директорию с Docker volume для коллектора")
        
        # Получение списка диалогов, чтобы проверить работу
        print("\nПроверка подключения - получение списка диалогов:")
        dialogs = client.get_dialogs(limit=5)
        for dialog in dialogs:
            print(f" - {dialog.name} ({dialog.entity.id})")
            
    else:
        print("Ошибка: Авторизация не удалась")
        
except Exception as e:
    print(f"Ошибка при аутентификации: {e}")
    sys.exit(1)
finally:
    client.disconnect()