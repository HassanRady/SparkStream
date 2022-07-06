import tempfile

from pyspark.sql import dataframe, functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, StructType, StructField


from SparkStream.Config.core import config
from SparkStream.Config import logging_config
from SparkStream.Text import text_cleaner as tc

import SparkStream.utils as utils

_logger = logging_config.get_logger(__name__)


class SparkStreamer(object):
    def __init__(self, ):
        self.__spark = SparkSession.builder.master("local[1]").appName("tweets reader")\
            .config("spark.some.config.option", "some-value")\
            .config("spark.streaming.stopGracefullyOnShutdown", "true")\
            .config("spark.cassandra.connection.host", config.cassandra.CASSANDRA_HOST)\
            .getOrCreate()
        self.__spark.sparkContext.setLogLevel("ERROR")
        self.topic = None


    def _connect_to_kafka_stream(self) -> dataframe:
        """reading stream from kafka"""


        df = self.__spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.kafka.KAFKA_HOST) \
            .option("subscribe", config.kafka.KAFKA_TOPIC_NAME) \
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
    def write_stream_to_memory(self, df, topic):
        """writing the tweets stream to memory"""
        self.topic = topic


        stream = df.writeStream \
            .trigger(processingTime='2 seconds') \
            .option("truncate", "false") \
            .format('memory') \
            .outputMode("append") \
            .queryName(f"""{self.topic}""") \
            .start()
        return stream

    def write_stream_to_cassandra(self, df, keyspace=config.cassandra.CASSANDRA_KEYSPACE, table=config.cassandra.CASSANDRA_DUMP_TABLE,):
        """writing the tweets stream to cassandra"""

        checkpoint_dir = tempfile.mkdtemp(dir='checkpoints/', prefix='cassandra')

        df = df.alias('other')
        df = df.withColumn('id', F.expr("uuid()"))

        df.writeStream\
            .format("org.apache.spark.sql.cassandra") \
            .options(table=table, keyspace=keyspace) \
            .option("checkpointLocation", checkpoint_dir) \
            .start()
            # .queryName(f"""writing {topic} to Cassandra""") \


        return df

    def get_stream_data_from_memory(self, topic,):
        """getting the tweets stream data"""
        pdf = self.__spark.sql(f"""select * from {self.topic}""")

        return pdf

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
        # cs = self.spark_streamer.write_stream_to_cassandra(df, topic, table=config.cassandra.CASSANDRA_DUMP_TABLE)

        df = self.spark_streamer.clean_stream_data(df)

        self.memory_stream = self.spark_streamer.write_stream_to_memory(df, topic=topic)
        self.cassandra_stream = self.spark_streamer.write_stream_to_cassandra(
            df, table=config.cassandra.CASSANDRA_OFFLINE_TABLE)

    def stop_spark_stream(self):
        try:
            self.memory_stream.stop()
            self.cassandra_stream.stop()
        except BaseException as e:
            _logger.warning(f"Error: {e}")

    def get_stream_data(self, ):
        return self.spark_streamer.get_stream_data_from_memory(self.topic)


if __name__ == '__main__':
    pass
