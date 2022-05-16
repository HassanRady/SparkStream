import os
import sys
import time
import threading
import copy

from pyspark.sql import dataframe, functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, StructType, StructField, ArrayType


from TweetAnalysis.config.core import config
from TweetAnalysis.config import logging_config
from TweetAnalysis.tweets_streamer import get_stream


_logger = logging_config.get_logger(__name__)


# env variables for spark and kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions pyspark-shell'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


class SparkStreamer(object):
    def __init__(self):
        self.__spark = SparkSession.builder.master("local[1]").appName("tweets reader")\
            .config("spark.some.config.option", "some-value")\
            .config("spark.cassandra.connection.host", config.cassandra.CASSANDRA_HOST)\
            .getOrCreate()
        self.topic = None
        
        

    def connect_to_kafka_stream(self) -> dataframe:
        """reading stream from kafka"""

        _logger.info('reading stream from kafka...')

        df = self.__spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.kafka.KAFKA_HOST) \
            .option("subscribe", config.kafka.KAFKA_TOPIC_NAME) \
            .load()

        df = df.selectExpr("CAST(value AS string)")

        schema = StructType([StructField('text', StringType()), StructField('created_at', StringType()),
                             StructField('id', StringType()),
                             StructField('user', StructType([
                                StructField('id', StringType()),
                                StructField('name', StringType(), True),
                                StructField('screen_name', StringType()),
                                StructField('location', StringType()),
                                StructField('followers_count', StringType()),
                                StructField('friends_count',StringType()),
                            ]))])

        df = df.select(F.from_json(col('value'), schema).alias(
            'data')).select("data.*")
        df = df.select('text', 'created_at', 'id', col('user.id').alias('user_id'), 'user.name', 'user.screen_name', 'user.location', 'user.followers_count', 'user.friends_count')
        return df

    def write_stream_to_memory(self, df):
        """writing the tweets stream to memory"""

        _logger.info(f'writing {self.topic} tweets stream to memory...')

        self.stream = df.writeStream \
            .trigger(processingTime='3 seconds') \
            .option("truncate", "false") \
            .format('memory') \
            .outputMode("append") \
            .queryName(self.topic) \
            .start()
        return self.stream

    def write_stream_to_csv(self, df, path='stream/', checkpoint='checkpoint/', ):
        """writing the tweets stream to csv"""
        _logger.info(f'writing {self.topic} tweets stream to csv...')

        df = df.copy()
        self.stream = df.writeStream \
            .trigger(processingTime='3 seconds') \
            .option("truncate", "false") \
            .format('csv') \
            .option("path", path) \
            .option("checkpointLocation", checkpoint) \
            .outputMode("append") \
            .queryName(self.topic) \
            .start() 
        return self.stream

    def write_stream_to_cassandra(self, df, keyspace='tweetsstream', table='tweets',):
        """writing the tweets stream to cassandra"""
        _logger.info(f'writing {self.topic} tweets stream to cassandra...')

        df = df.alias('other')
        df.writeStream\
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .option("checkpointLocation", 'checkpoint/') \
        .start()


    def start_stream(self, topic, stop=True):
        self.topic = topic
        _logger.info(f'starting stream on topic {topic}')
        thread = threading.Thread(target=get_stream, kwargs={
                                  'topic': topic}, daemon=stop)
        thread.start()

        df = self.connect_to_kafka_stream()
        df = df.withColumn('topic', lit(self.topic))

        self.write_stream_to_memory(df)
        self.write_stream_to_cassandra(df)

    def get_stream_data(self, wait=0, stop=True, ):
        time.sleep(wait)
        pdf = self.__spark.sql(f"""select * from {self.topic}""") 
        if stop:
            try:
                self.stream.stop()
                # self.__spark.stop()
                _logger.info('spark stopped')
            except BaseException as e:
                _logger.warning(f"Error: {e}")
        return pdf


if __name__ == '__main__':
    ss = SparkStreamer()
    ss.start_stream('music', True)
    zz = ss.get_stream_data(4, True)
    print(zz.shape)
    print(zz)
