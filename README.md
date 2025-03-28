# telenews-analyzer

структура Проекта

/
├── docker-compose.yml
├── .env.example
├── README.md
├── src/
│   ├── collector/  # модуль сбора Telegram
│   ├── processor/  # модуль обработки текста
│   ├── api/        # модуль API
│   └── common/     # общие компоненты
├── infrastructure/
│   ├── postgres/
│   ├── mongo/
│   └── kafka/
└── scripts/
    └── setup.sh