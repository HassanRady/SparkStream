from SparkStream.Config import logging_config

_logger = logging_config.get_logger(__name__)

def clean_query_name(func):
    def _wrapper(*args, **kwargs):
        puncs = ['-', '_', '.', ' ', ':', ';', '!', '?', ',', '#', '@', '$', '%', '^', '&', '*', '(', ')', '+', '=', '~', '`', '\'', '\"', '\\', '/', '|', '{', '}', '[', ']', '<', '>', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
        for punc in puncs:
            kwargs['topic'] = kwargs['topic'].replace(punc, '')
        return func(*args, **kwargs)
    return _wrapper