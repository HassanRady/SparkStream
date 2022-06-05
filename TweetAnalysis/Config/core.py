from pathlib import Path
from typing import Dict, List, Sequence

from pydantic import BaseModel
from strictyaml import YAML, load

import TweetAnalysis

# Project Directories
PACKAGE_ROOT = Path(TweetAnalysis.__file__).resolve().parent
ROOT = PACKAGE_ROOT.parent
CONFIG_FILE_PATH = PACKAGE_ROOT / "config.yml"





class TwitterConfig(BaseModel):
    """
    Twitter-level config.
    """


class KafkaConfig(BaseModel):
    """
    Kafka-level config.
    """

    KAFKA_TOPIC_NAME: str
    KAFKA_HOST: str

class CassandraConfig(BaseModel):
    """
    Cassandra-level config.
    """
    CASSANDRA_SPARK_APP_NAME: str
    CASSANDRA_HOST: str
    CASSANDRA_KEYSPACE: str
    CASSANDRA_TABLE: str
    CASSANDRA_OFFLINE_TABLE: str


class Config(BaseModel):
    """Master config object."""

    twitter: TwitterConfig
    kafka: KafkaConfig
    cassandra: CassandraConfig


def find_config_file() -> Path:
    """Locate the configuration file."""
    if CONFIG_FILE_PATH.is_file():
        return CONFIG_FILE_PATH
    raise Exception(f"Config not found at {CONFIG_FILE_PATH!r}")


def fetch_config_from_yaml(cfg_path: Path = None) -> YAML:
    """Parse YAML containing the package configuration."""

    if not cfg_path:
        cfg_path = find_config_file()

    if cfg_path:
        with open(cfg_path, "r") as conf_file:
            parsed_config = load(conf_file.read())
            return parsed_config
    raise OSError(f"Did not find config file at path: {cfg_path}")


def create_and_validate_config(parsed_config: YAML = None) -> Config:
    """Run validation on config values."""
    if parsed_config is None:
        parsed_config = fetch_config_from_yaml()

    # specify the data attribute from the strictyaml YAML type.
    _config = Config(
        twitter=TwitterConfig(**parsed_config.data),
        kafka=KafkaConfig(**parsed_config.data),
        cassandra=CassandraConfig(**parsed_config.data),
    )

    return _config


config = create_and_validate_config()
