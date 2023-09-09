import tempfile
import os

from pyspark.sql import dataframe, functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField

from SparkStream.Config.logging_config import get_logger
from SparkStream.Config.core import settings
from SparkStream.Text import text_cleaner as tc

_logger = get_logger(__name__)

if not os.path.exists("checkpoints"):
    os.mkdir("checkpoints")


class SparkStreamer(object):
    def __init__(self, ):
        self.__spark = SparkSession.builder.master("local[1]").appName(settings.APP_NAME)\
            .config("spark.some.config.option", "some-value")\
            .config("spark.cassandra.connection.host", settings.CASSANDRA_HOST)\
            .config("spark.redis.host", settings.REDIS_HOST)\
            .config("spark.redis.port", settings.REDIS_PORT)\
            .config("spark.streaming.stopGracefullyOnShutdown", "true")\
            .getOrCreate()
        self.__spark.sparkContext.setLogLevel("ERROR")

    def _connect_to_kafka_stream(self):
        df = self.__spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings.KAFKA_HOST) \
            .option("subscribe", settings.KAFKA_CONSUMER_TOPIC) \
            .option('failOnDataLoss', 'false') \
            .option("startingOffsets", "latest") \
            .load()

        df = df.selectExpr("CAST(value AS string)")

        schema = StructType([
            StructField('subreddit', StringType()),
            StructField('text', StringType()),
        ])

        df = df.select(F.from_json(col('value'), schema).alias(
            'data')).select('data.*')
        
        return df

    def write_stream_to_kafka(self, df,):
        checkpoint_dir = tempfile.mkdtemp(dir='checkpoints/', prefix='kafka')

        df = df.select(F.to_json(F.struct([df[x]
                                           for x in df.columns])).alias("value"))

        df.selectExpr("CAST(value AS STRING)") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings.KAFKA_HOST) \
            .option("topic", settings.KAFKA_PRODUCER_TOPIC) \
            .option("checkpointLocation", checkpoint_dir) \
            .start() \
            .awaitTermination()

        return df

    def write_stream_to_cassandra(self, df,):
        checkpoint_dir = tempfile.mkdtemp(
            dir='checkpoints/', prefix='cassandra')

        df = df.alias('other')
        df = df.withColumn('id', F.expr("uuid()"))

        df.writeStream\
            .format("org.apache.spark.sql.cassandra") \
            .options(table=settings.CASSANDRA_TABLE, keyspace=settings.CASSANDRA_KEYSPACE) \
            .option("checkpointLocation", checkpoint_dir) \
            .start()

        return df

    def write_stream_to_redis(self, df,):
        checkpoint_dir = tempfile.mkdtemp(dir='checkpoints/', prefix='redis')

        df = df.alias('other')
        df = df.withColumn('id', F.expr("uuid()"))

        def foreach_func(df, id):
            df.write.format("org.apache.spark.sql.redis") \
                .option("table", settings.REDIS_TABLE) \
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
        df = df.withColumn('text', tc.remove_features_udf(df['text']))
        df = df.withColumn('text', tc.fix_abbreviation_udf(df['text']))
        df = df.withColumn('text', tc.remove_stopwords_udf(df['text']))
        df = df.filter("text != ''")
        return df


class SparkClient:
    def __init__(self):
        self.spark_streamer = SparkStreamer()
        self.kafka_df = self.spark_streamer._connect_to_kafka_stream()

    def start_spark_stream(self):

        df = self.spark_streamer.clean_stream_data(self.kafka_df)

        self.kafka_stream = self.spark_streamer.write_stream_to_kafka(df)

        # self.cassandra_stream = self.spark_streamer.write_stream_to_cassandra(
            # df, )

        # self.redis_stream = self.spark_streamer.write_stream_to_redis(df)

    def stop_spark_stream(self):
        try:
            self.kafka_stream.stop()
            # self.cassandra_stream.stop()
            # self.redis_stream.stop()
        except BaseException as e:
            _logger.warning(f"Error: {e}")
