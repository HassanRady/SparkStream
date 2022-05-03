# dataset configs
DATASET_COLUMNS = ["sentiment", "ids", "date", "flag", "user", "text"]
DATASET_ENCODING = "ISO-8859-1"
DATASET_PATH = '../04_Data/training.1600000.processed.noemoticon.csv'
DATASET_OUTPUT_PATH = '../04_Data/data_processed.csv'
TARGET = 'sentiment'
NEUTRAL_MIN = 0.4
NEUTRAL_MAX = 0.7
CLASSES = ['NEGATIVE', 'NEUTRAL', 'POSITIVE']
NEGATIVE_INDEX = CLASSES.index("NEGATIVE")
NEUTRAL_INDEX = CLASSES.index("NEUTRAL")
POSITIVE_INDEX = CLASSES.index("POSITIVE")


# Paths
MODEL_PATH = "trained_models"
PIPELINE_PATH = "trained_models"
EMBEDDING_MATRIX_PATH = "trained_models/embedded_matrix.pkl"
MODEL_NAME = "model_v1"
TOKEN_NAME = 'token.pkl'

# To replicate the results
SEED = 42


# Twitter api tokens and keys
CONSUMER_KEY = 'jtrYOL3P1AmnCwrKOM8F4e0Kk'
CONSUMER_SECRET = 'G7YtQlUIo6tPIvuA88evm4eS2U9hRVxwmUMNuMCyCSqrYq3pHq'
ACCESS_TOKEN = '1438163536329560067-PfbPXO8fP8RJsV1jaotHqQRjtyie9C'
ACCESS_SECRET = 'llHnjvA0Wy8N6PXgz3jnbS16pT5FZCf1jKLTanKiMkT6r'


# kafka configs
KAFKA_TOPIC_NAME = 'tweets_stream'
KAFKA_HOST = 'localhost:9092'


# Model params
INPUT_LEN = 60
VOCAB_LEN = 60000
EMBEDDING_DIMENSIONS = 100
EPOCHS = 2
BATCH_SIZE = 1024
