from pyspark.sql import SparkSession


from SparkStream.Config.core import config
from SparkStream.Config import logging_config


_logger = logging_config.get_logger(__name__)


class CassandraApi(object):
    def __init__(self):
        self.__spark = SparkSession.builder.master("local[2]").appName(config.cassandra.CASSANDRA_SPARK_APP_NAME)\
            .config("spark.some.config.option", "some-value")\
            .config("spark.cassandra.connection.host", config.cassandra.CASSANDRA_HOST)\
            .getOrCreate()

    def get_all_data(self):

        df = self.__spark \
            .read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=config.cassandra.CASSANDRA_TABLE, keyspace=config.cassandra.CASSANDRA_KEYSPACE) \
            .load()

        return df

    def get_data_on_topic(self, topic):

        df = self.__spark \
            .read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=config.cassandra.CASSANDRA_TABLE, keyspace=config.cassandra.CASSANDRA_KEYSPACE) \
            .load()

        df = df.filter(df.topic == topic)   
        return df

    def get_offline_data(self):

        df = self.__spark \
            .read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=config.cassandra.CASSANDRA_OFFLINE_TABLE, keyspace=config.cassandra.CASSANDRA_KEYSPACE) \
            .load()

        return df
