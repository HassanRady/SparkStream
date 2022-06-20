import os
import sys

# env variables for spark and kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions pyspark-shell'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

