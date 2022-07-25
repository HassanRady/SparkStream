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