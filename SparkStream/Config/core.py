import os
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    class Config:
        env_file = os.getenv("ENV_FILE", "dev.env") 
        env_file_encoding = "utf-8"

    APP_NAME: str
    KAFKA_HOST: str
    KAFKA_CONSUMER_TOPIC: str
    KAFKA_PRODUCER_TOPIC: str
    REDIS_HOST: str
    REDIS_PORT: str
    REDIS_TABLE: str
    CASSANDRA_HOST: str
    CASSANDRA_KEYSPACE: str
    CASSANDRA_TABLE: str


@lru_cache()
def get_settings():
    return Settings()

settings = get_settings()