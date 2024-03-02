from SparkStream.streamer import spark_streamer


def start():
    spark_streamer.read_kafka_stream()
    spark_streamer.start_write_clean_stream()


start()
