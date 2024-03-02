import tempfile
import os

from pyspark.sql import functions as F
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
    def __init__(
        self,
    ):
        # .config("spark.cassandra.connection.host", settings.CASSANDRA_HOST)\
        # .config("spark.redis.host", settings.REDIS_HOST)\
        # .config("spark.redis.port", settings.REDIS_PORT)\
        self.__spark = (
            SparkSession.builder.master("local[1]")
            .appName(settings.APP_NAME)
            .config("spark.some.config.option", "some-value")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .getOrCreate()
        )
        self.__spark.sparkContext.setLogLevel("ERROR")

    def read_kafka_stream(self):
        self.kafka_df = (
            self.__spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", settings.KAFKA_STREAM_TEXT_TOPIC)
            .option("failOnDataLoss", "false")
            .option("startingOffsets", "latest")
            .load()
        )

        self.kafka_df = self.kafka_df.selectExpr("CAST(value AS string)")
        schema = StructType(
            [
                StructField("author_id", StringType()),
                StructField("text", StringType()),
            ]
        )

        self.kafka_df = self.kafka_df.select(
            F.from_json(col("value"), schema).alias("data")
        ).select("data.*")

    def write_stream_to_kafka(
        self,
        df,
    ):
        checkpoint_dir = tempfile.mkdtemp(dir="checkpoints/", prefix="kafka")

        df = df.select(F.to_json(F.struct([df[x] for x in df.columns])).alias("value"))

        df.selectExpr("CAST(value AS STRING)").writeStream.format("kafka").option(
            "kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS
        ).option("topic", settings.KAFKA_CLEANED_TEXT_TOPIC).option(
            "checkpointLocation", checkpoint_dir
        ).outputMode("append").queryName("kafka_writer").start()
        return df

    # def write_stream_to_cassandra(self, df,):
    #     checkpoint_dir = tempfile.mkdtemp(
    #         dir='checkpoints/', prefix='cassandra')

    #     df = df.alias('other')
    #     df = df.withColumn('id', F.expr("uuid()"))

    #     df.writeStream\
    #         .format("org.apache.spark.sql.cassandra") \
    #         .options(table=settings.CASSANDRA_TABLE, keyspace=settings.CASSANDRA_KEYSPACE) \
    #         .option("checkpointLocation", checkpoint_dir) \
    #         .start()

    #     return df

    # def write_stream_to_redis(self, df,):
    #     checkpoint_dir = tempfile.mkdtemp(dir='checkpoints/', prefix='redis')

    #     df = df.alias('other')
    #     df = df.withColumn('id', F.expr("uuid()"))

    #     def foreach_func(df, id):
    #         df.write.format("org.apache.spark.sql.redis") \
    #             .option("table", settings.REDIS_TABLE) \
    #             .option("key.column", "id") \
    #             .mode("append") \
    #             .save()

    #     df.writeStream\
    #         .foreachBatch(foreach_func) \
    #         .option("checkpointLocation", checkpoint_dir) \
    #         .outputMode("append") \
    #         .queryName("redis_writer") \
    #         .start()

    #     return df

    def clean_stream_data(self, df):
        df = df.withColumn("text", tc.remove_features_udf(df["text"]))
        df = df.withColumn("text", tc.fix_abbreviation_udf(df["text"]))
        df = df.withColumn("text", tc.remove_stopwords_udf(df["text"]))
        df = df.filter("text != ''")
        return df

    def start_write_clean_stream(self):
        df = self.clean_stream_data(self.kafka_df)

        self.kafka_stream = self.write_stream_to_kafka(df)

        # self.cassandra_stream = self.write_stream_to_cassandra(
        # df, )

        # self.redis_stream = self.write_stream_to_redis(df)

        self.__spark.streams.awaitAnyTermination()

    def stop_spark_stream(self):
        try:
            self.kafka_stream.stop()
            # self.cassandra_stream.stop()
            self.redis_stream.stop()
        except BaseException as e:
            _logger.warning(f"Error: {e}")


spark_streamer = SparkStreamer()
