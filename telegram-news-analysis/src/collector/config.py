import os

# Telegram API settings
TELEGRAM_SESSION_NAME = "telegram_session"
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID", "")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
raw_channels = os.getenv("TELEGRAM_CHANNELS", "").split(",")

# MongoDB settings
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "telegram_news")
MESSAGES_COLLECTION = "messages"
CHANNELS_COLLECTION = "channels"

# Logging settings
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")