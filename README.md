# Spark Streaming Package
Package: <a href="https://pypi.org/project/SparkStream/#description">SparkStream-pypi</a>

## What is it?
It is a handler for processing streaming text data from a kafka topic into cassandra and redis.

## How it works?
The stream processing is done by the following steps:
1. Read data from kafka topic 
2. Parse the data into a spark dataframe with a schema
3. Clean the data: remove unwanted chars, fix abbreviations, remove stop-words, and remove empty fields
4. Save the data into cassandra and redis

## How to use it?
Use its API: <a href="https://github.com/HassanRady/Spark-Stream-Api">SparkStream-API github</a>

## Dependency
The package requires the following dependency:
- spark-redis_2.12-3.1.0-jar-with-dependencies.jar (<a href="https://mvnrepository.com/artifact/com.redislabs/spark-redis_2.12/3.1.0">mvn Repository</a>)

Its so to be able to write data into redis.

## Environment Variables
The package requires the following environment variables:
- `KAFKA_HOST` : The kafka host address
- `KAFKA_TOPIC` : The kafka topic to listen to
- `CASSANDRA_HOST` : The cassandra host address
- `CASSANDRA_KEYSPACE` : The cassandra keyspace to use
- `CASSANDRA_TABLE` : The cassandra table to write to
- `REDIS_HOST` : The redis host address
- `REDIS_PORT` : The redis port
- `REDIS_TABLE` : The redis table to write to

