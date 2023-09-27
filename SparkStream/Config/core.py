from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    APP_NAME: str
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_RAW_TEXT_TOPIC: str
    KAFKA_CLEANED_TEXT_TOPIC: str
    # REDIS_HOST: str
    # REDIS_PORT: str
    # REDIS_TABLE: str
    # CASSANDRA_HOST: str
    # CASSANDRA_KEYSPACE: str
    # CASSANDRA_TABLE: str


@lru_cache()
def get_settings():
    return Settings()

settings = get_settings()