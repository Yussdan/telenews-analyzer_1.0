# TelTrends

Система real-time сбора и анализа новостей на основе данных из Telegram с использованием Big Data инструментов.

## О проекте

TelTrends анализирует русскоязычный контент из Telegram-каналов, выявляет тренды и позволяет отслеживать ключевые темы в информационном пространстве. Система работает в режиме реального времени, обеспечивая актуальные данные и аналитику.

### Ключевые возможности

- Автоматический сбор сообщений из русскоязычных Telegram-каналов
- Выявление и отслеживание трендов в информационном пространстве
- Мониторинг по ключевым словам и фразам
- Аналитический дашборд для визуализации данных

## Технический стек

- **Язык программирования**: Python 3.11
- **Сбор данных**: Telegram API (Telethon)
- **Обработка потоков**: Apache Kafka
- **Хранение данных**: MongoDB, PostgreSQL
- **Обработка текста**: Python NLP библиотеки
- **API**: FastAPI
- **Инфраструктура**: Docker, Яндекс.Облако

## Структура проекта

```
teltrends-collector/
├── src/
│   ├── api/
│   │   ├── __init__.py
│   │   └── routes.py
│   ├── collector/
│   │   ├── __init__.py
│   │   ├── telegram_client.py
│   │   ├── message_processor.py
│   │   └── channel_manager.py
│   ├── kafka/
│   │   ├── __init__.py
│   │   └── producer.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── channel.py
│   │   └── message.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py
│   ├── utils/
│   │   ├── __init__.py
│   │   └── logging.py
│   ├── __init__.py
│   └── main.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## Запуск проекта

### Предварительные требования

- Docker и Docker Compose
- Python 3.11+
- Доступ к Telegram API (api_id и api_hash)

### Настройка окружения

1. Клонировать репозиторий:
   ```bash
   git clone https://github.com/yourusername/teltrends.git
   cd teltrends
   ```

2. Создать файл .env на основе .env.example и заполнить необходимыми значениями:
   ```bash
   cp .env.example .env
   # Отредактировать .env файл
   ```

3. Запустить проект:
   ```bash
   docker-compose up -d
   ```

## Стандарт коммитов

Проект использует следующий формат коммитов:

```
тип(область): краткое описание
```

Где:
- **тип**: feat, fix, refactor, docs, test, chore
- **область**: компонент проекта (collector, processor, storage, api)

Примеры:
- `feat(collector): add telegram channels subscription`
- `fix(storage): prevent duplicate messages in MongoDB`

## Команда разработки

- Менеджер продукта(Claude 3.7 Sonnet)
- Дата-инженер(Yussdan)

## Лицензия

[MIT License](LICENSE)