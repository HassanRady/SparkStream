from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from tweepy import Stream
from kafka import KafkaProducer

import json
import sys

from TweetAnalysis.config.core import config
from TweetAnalysis.config import logging_config


_logger = logging_config.get_logger(__name__)


consumer_key = config.twitter.CONSUMER_KEY
consumer_secret = config.twitter.CONSUMER_SECRET
access_token = config.twitter.ACCESS_TOKEN
access_secret = config.twitter.ACCESS_SECRET


class StdOutListener(StreamListener):
    """Listener class for the tweets stream"""

    def __init__(self, producer):
        self.producer = producer

    def on_data(self, data):
        try:
            msg = json.loads(data)
            self.producer.send(
                config.kafka.KAFKA_TOPIC_NAME, value=msg['text'].encode('utf-8'))
            # print( msg['user']['screen_name'].encode('utf-8'), msg['text'].encode('utf-8'))
            # print(msg)
        except BaseException as e:
            _logger.warning("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        _logger.warning(f'status: {status}')


def get_stream(topic):
    """getting the tweets stream with twitter api and handling it with kafka"""

    _logger.info('tweets streaming...')
    producer = KafkaProducer(bootstrap_servers=config.kafka.KAFKA_HOST)
    l = StdOutListener(producer)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    stream = Stream(auth, l)
    stream.filter(track=[topic], languages=['en'])
    return None


if __name__ == '__main__':
    # arg = sys.argv[1]
    # print(arg)
    get_stream('music')
    # _logger.info(f'Run From: {__name__}')
