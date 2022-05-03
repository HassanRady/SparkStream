# Real-Time Tweets Sentiment Analysis Package

## Overview
Retrieving real-time tweets using twitter API, Apache Kafka, and Apache Spark Streaming; then, using tensorflow deep learning model to classify the tweets wether they positive, negative, or neutral; all in a pypi package.

<img src="imgs/arc.png">



## TweetsAnalysis
The streamer and model package, available on pypi <a href="https://pypi.org/project/TweetsAnalysis/">TweetsAnalysis</a>
### Package Requirements
- gensim
- pandas
- pyspark
- kafka-python
- streamlit
- scikit-learn
- seaborn
- tensorflow
- tweepy==3.9.0
- pydantic
- strictyaml
- joblib

<br>



<br>

## Model
The model architecture:

<img src='imgs/model.png'/>


The model results in about 85.5% in the train set and 84.4% accuracy on the test set, which has 160000 tweets; therefore, there is no over-fitting here.

<img src='imgs/metrics.png'/>

<br>


## Run
First we need to install the requirements with: 
```sh
 pip install TweetsAnalysis
```
To train the model run, but first we need to specifiy the model and data directories in the config file:
```sh
python train_model.py
```
### Straming
Start kafka with:
```sh
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

then create a kafka topic (tweets_stream) with:
```sh
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tweets_stream
```