import os
import sys

from pyspark.sql import SparkSession


from TweetAnalysis.config.core import config
from TweetAnalysis.config import logging_config


_logger = logging_config.get_logger(__name__)


# env variables for spark and kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions pyspark-shell'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


class CassandraApi(object):
    def __init__(self):
        self.__spark = SparkSession.builder.master("local[1]").appName("cassandra reader")\
            .config("spark.some.config.option", "some-value")\
            .config("spark.cassandra.connection.host", config.cassandra.CASSANDRA_HOST)\
            .getOrCreate()

    def get_all_data(self):
        _logger.info('reading data from cassandra...')

        df = self.__spark \
            .read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="tweets", keyspace="tweetsstream") \
            .load()

        return df

    def get_data_on_topic(self, topic):
        _logger.info('reading data from cassandra...')

        df = self.__spark \
            .read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="tweets", keyspace="tweetsstream") \
            .load()

        df = df.filter(df.topic == topic)

        return df