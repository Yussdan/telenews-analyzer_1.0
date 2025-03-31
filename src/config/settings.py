import os
from pydantic import BaseSettings, Field

class Settings(BaseSettings):
    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS: str = Field("localhost:9092", env="KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_TOPIC_MESSAGES: str = Field("telegram-messages", env="KAFKA_TOPIC_MESSAGES")
    KAFKA_TOPIC_TRENDS: str = Field("telegram-trends", env="KAFKA_TOPIC_TRENDS")
    KAFKA_CONSUMER_GROUP: str = Field("teltrends-processor", env="KAFKA_CONSUMER_GROUP")
    
    # Database settings
    POSTGRES_HOST: str = Field("localhost", env="POSTGRES_HOST")
    POSTGRES_PORT: int = Field(5432, env="POSTGRES_PORT")
    POSTGRES_DB: str = Field("teltrends", env="POSTGRES_DB")
    POSTGRES_USER: str = Field("postgres", env="POSTGRES_USER")
    POSTGRES_PASSWORD: str = Field(..., env="POSTGRES_PASSWORD")
    
    MONGO_HOST: str = Field("localhost", env="MONGO_HOST")
    MONGO_PORT: int = Field(27017, env="MONGO_PORT")
    MONGO_DB: str = Field("teltrends", env="MONGO_DB")
    MONGO_USER: str = Field(None, env="MONGO_USER")
    MONGO_PASSWORD: str = Field(None, env="MONGO_PASSWORD")
    
    # Processing settings
    TREND_MIN_MESSAGES: int = Field(5, env="TREND_MIN_MESSAGES")
    TREND_TIME_WINDOW_HOURS: int = Field(24, env="TREND_TIME_WINDOW_HOURS")
    CLUSTER_COUNT: int = Field(10, env="CLUSTER_COUNT")
    
    # API settings
    API_HOST: str = Field("0.0.0.0", env="API_HOST")
    API_PORT: int = Field(8001, env="API_PORT")
    
    # Logging
    LOG_LEVEL: str = Field("INFO", env="LOG_LEVEL")
    
    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()
