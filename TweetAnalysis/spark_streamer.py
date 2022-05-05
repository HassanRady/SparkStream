import os
import sys
import time
import threading

from pyspark.sql import dataframe, functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField, ArrayType


from TweetAnalysis.config.core import config
from TweetAnalysis.config import logging_config
from TweetAnalysis.tweets_streamer import get_stream


_logger = logging_config.get_logger(__name__)


# env variables for spark and kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


class SparkStreamer(object):
    def __init__(self):
        self.__spark = SparkSession.builder.master("local[1]").appName("tweets reader")\
            .config("spark.some.config.option", "some-value")\
            .getOrCreate()

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
                             StructField('user', StringType())])

        user_schema = ArrayType(
                            StructType([
                                StructField('id', StringType()),
                                StructField('name', StringType(), True),
                                StructField('screen_name', StringType()),
                                StructField('location', StringType()),
                                StructField('followers_count', StringType()),
                                StructField('friends_count',StringType()),
                            ]))

        df = df.select(F.from_json(col('value'), schema).alias(
            'data')).select("data.*")
        df = df.withColumn('user', F.from_json(col('user'), user_schema))
        return df

    def write_stream_to_memory(self, df):
        """writing the tweets stream to memory"""

        _logger.info('writing the tweets stream to memory...')

        self.stream = df.writeStream \
            .trigger(processingTime='1 seconds') \
            .option("truncate", "false") \
            .format('memory') \
            .outputMode("append") \
            .queryName('streamTable') \
            .start()  # .awaitTermination()
        return self.stream

    def start_stream(self, topic, stop=True):
        thread = threading.Thread(target=get_stream, kwargs={
                                  'topic': topic}, daemon=stop)
        thread.start()

        df = self.connect_to_kafka_stream()

        stream = self.write_stream_to_memory(df)

    def get_stream_data(self, wait=0, stop=True):
        time.sleep(wait)
        pdf = self.__spark.sql("""select * from streamTable""")  # .toPandas()
        if stop:
            try:
                self.stream.stop()
                self.__spark.stop()
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
