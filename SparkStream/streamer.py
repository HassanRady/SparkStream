import tempfile
import os

from pyspark.sql import dataframe, functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, StructType, StructField

from SparkStream.Config import logging_config
from SparkStream.Text import text_cleaner as tc

import SparkStream.utils as utils

_logger = logging_config.get_logger(__name__)

if not os.path.exists("checkpoints"):
    os.mkdir("checkpoints")

class SparkStreamer(object):
    def __init__(self, ):
        self.__spark = SparkSession.builder.master("local[1]").appName("tweets reader")\
            .config("spark.some.config.option", "some-value")\
            .config("spark.cassandra.connection.host", os.environ['CASSANDRA_HOST'])\
            .config("spark.redis.host", os.environ['REDIS_HOST'])\
            .config("spark.redis.port", os.environ['REDIS_PORT'])\
            .config("spark.streaming.stopGracefullyOnShutdown", "true")\
            .getOrCreate()
        self.__spark.sparkContext.setLogLevel("ERROR")
        self.topic = None


    def _connect_to_kafka_stream(self) -> dataframe:
        """reading stream from kafka"""

        df = self.__spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", os.environ['KAFKA_HOST']) \
            .option("subscribe", os.environ['KAFKA_TOPIC']) \
            .option('failOnDataLoss', 'false') \
            .load()

        df = df.selectExpr("CAST(value AS string)")

        schema = StructType([StructField('data', StringType()), ])

        df = df.select(F.from_json(col('value'), schema).alias(
            'data')).select('data.*')

        schema = StructType([StructField('text', StringType()),
                             StructField('author_id', StringType()),
                             ])

        df = df.select(F.from_json(col('data'), schema).alias(
            'data')).select("data.*")

        return df

    @utils.clean_query_name
    def write_stream_to_console(self, df, topic):
        """writing the tweets stream to console"""
        self.topic = topic
        stream = df.writeStream \
            .trigger(processingTime='2 seconds') \
            .option("truncate", "false") \
            .format('console') \
            .outputMode("append") \
            .queryName(f"""{self.topic}""") \
            .start()
        return stream

    def write_stream_to_cassandra(self, df,):
        """writing the tweets stream to cassandra"""

        checkpoint_dir = tempfile.mkdtemp(dir='checkpoints/', prefix='cassandra')

        df = df.alias('other')
        df = df.withColumn('id', F.expr("uuid()"))

        df.writeStream\
            .format("org.apache.spark.sql.cassandra") \
            .options(table=os.environ['CASSANDRA_TABLE'], keyspace=os.environ['CASSANDRA_KEYSPACE']) \
            .option("checkpointLocation", checkpoint_dir) \
            .start()

        return df

    def write_stream_to_redis(self, df,):
        """writing the tweets stream to redis"""

        checkpoint_dir = tempfile.mkdtemp(dir='checkpoints/', prefix='redis')

        df = df.alias('other')
        df = df.withColumn('id', F.expr("uuid()"))

        def foreach_func(df, id):
            df.write.format("org.apache.spark.sql.redis") \
                .option("table", os.environ['REDIS_TABLE']) \
                .option("key.column", "id") \
                .mode("append") \
                .save()

        df.writeStream\
            .foreachBatch(foreach_func) \
            .option("checkpointLocation", checkpoint_dir) \
            .outputMode("append") \
            .start()

        return df



    def clean_stream_data(self, df):
        """cleaning the tweets stream data"""
        df = df.withColumn('text', tc.remove_features_udf(df['text']))
        df = df.withColumn('text', tc.fix_abbreviation_udf(df['text']))
        df = df.withColumn('text', tc.remove_stopwords_udf(df['text']))       
        df = df.filter("text != ''")
        return df



class SparkClient:
    def __init__(self):
        self.topic = None
        self.spark_streamer = SparkStreamer()
        self.kafka_df = self.spark_streamer._connect_to_kafka_stream()

    def start_spark_stream(self, topic):
        self.topic = topic

        df = self.kafka_df.withColumn('topic', lit(topic))

        df = self.spark_streamer.clean_stream_data(df)

        # self.console_stream = self.spark_streamer.write_stream_to_console(df, topic=topic)

        self.cassandra_stream = self.spark_streamer.write_stream_to_cassandra(
            df, )

        self.redis_stream = self.spark_streamer.write_stream_to_redis(df)

    def stop_spark_stream(self):
        try:
            # self.console_stream.stop()
            self.cassandra_stream.stop()
            self.redis_stream.stop()
        except BaseException as e:
            _logger.warning(f"Error: {e}")


if __name__ == '__main__':
    pass
