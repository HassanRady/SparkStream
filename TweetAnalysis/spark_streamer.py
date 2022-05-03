import os
import sys
import time
import threading

import pyspark.pandas as ps

from pyspark.sql import dataframe, functions as F
from pyspark.sql import SparkSession

from TweetAnalysis.config.core import config
from TweetAnalysis.config import logging_config
from TweetAnalysis.tweets_streamer import get_stream


_logger = logging_config.get_logger(__name__)


# env variables for spark and kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


urlPattern = r"((http://)[^ ]*|(https://)[^ ]*|(www\.)[^ ]*)"
userPattern = '@[^\s]+'
hashtagPattern = '#[^\s]+'
alphaPattern = "[^a-z0-9<>]"
sequencePattern = r"(.)\1\1+"
seqReplacePattern = r"\1\1"

# Defining regex for emojis
smileemoji = r"[8:=;]['`\-]?[)d]+"
sademoji = r"[8:=;]['`\-]?\(+"
neutralemoji = r"[8:=;]['`\-]?[\/|l*]"
lolemoji = r"[8:=;]['`\-]?p+"

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

        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS string)")
        df = df.withColumn('tweet', df['value'])

        df = df.withColumn('value', F.regexp_replace(
            'value', userPattern, '<user>'))
        df = df.withColumn('value', F.regexp_replace('value', 'RT', ''))
        df = df.withColumn('value', F.regexp_replace('value', urlPattern, '<url>'))
        df = df.withColumn('value', F.regexp_replace(
            'value', smileemoji, '<smile>'))
        df = df.withColumn('value', F.regexp_replace(
            'value', sademoji, '<sadface>'))
        df = df.withColumn('value', F.regexp_replace(
            'value', neutralemoji, '<neutralface>'))
        df = df.withColumn('value', F.regexp_replace(
            'value', lolemoji, '<lolface>'))
        df = df.withColumn('value', F.regexp_replace(
            'value', sequencePattern, seqReplacePattern))
        df = df.withColumn('value', F.regexp_replace('value', userPattern, ''))
        df = df.withColumn('value', F.regexp_replace('value', r'/', ' / '))

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
            .start()#.awaitTermination()
        return self.stream


    def start_stream(self, topic, stop=True):
        thread = threading.Thread(target=get_stream, kwargs={'topic': topic}, daemon=stop)
        thread.start()

        df = self.connect_to_kafka_stream()

        stream = self.write_stream_to_memory(df)


    def get_stream_data(self, wait=0, stop=True):
        time.sleep(wait)
        pdf = self.__spark.sql("""select * from streamTable""").toPandas()
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
    zz=ss.get_stream_data(4, True)
    print(zz.shape)
    print(zz)



    
