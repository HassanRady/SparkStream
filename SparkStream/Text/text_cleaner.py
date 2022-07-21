from nltk.stem.wordnet import WordNetLemmatizer
from nltk import pos_tag
import string
import re

from pyspark.ml.feature import StopWordsRemover 
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

class TextCleaner:

    # remove irrelevant features
    @classmethod
    def remove_features(self, data_str):
        # compile regex
        url_re = re.compile('https?://(www.)?\w+\.\w+(/\w+)*/?')
        punc_re = re.compile('[%s]' % re.escape(string.punctuation))
        num_re = re.compile('(\\d+)')
        mention_re = re.compile('@(\w+)')
        alpha_num_re = re.compile("^[a-z0-9_.]+$")
        retweet_re = re.compile('^RT')
        # convert to lowercase
        data_str = data_str.lower()
        # remove RT
        data_str = retweet_re.sub('', data_str)
        # remove hyperlinks
        data_str = url_re.sub('', data_str)
        # remove @mentions
        data_str = mention_re.sub('', data_str)
        # remove puncuation
        data_str = punc_re.sub('', data_str)
        # remove numeric 'words'
        data_str = num_re.sub('', data_str)
        # remove non a-z 0-9 characters and words shorter than 3 characters
        list_pos = 0
        cleaned_str = ''
        for word in data_str.split():
            if list_pos == 0:
                if alpha_num_re.match(word) and len(word) > 2:
                    cleaned_str = word
            else:
                if alpha_num_re.match(word) and len(word) > 2:
                    cleaned_str = cleaned_str + ' ' + word

            list_pos += 1
        return cleaned_str

    # fixed abbreviation
    @classmethod
    def fix_abbreviation(self, data_str):
        data_str = data_str.lower()
        data_str = re.sub(r'\bthats\b', 'that is', data_str)
        data_str = re.sub(r'\bive\b', 'i have', data_str)
        data_str = re.sub(r'\bim\b', 'i am', data_str)
        data_str = re.sub(r'\bya\b', 'yeah', data_str)
        data_str = re.sub(r'\bcant\b', 'can not', data_str)
        data_str = re.sub(r'\bdont\b', 'do not', data_str)
        data_str = re.sub(r'\bwont\b', 'will not', data_str)
        data_str = re.sub(r'\bid\b', 'i would', data_str)
        data_str = re.sub(r'wtf', 'what the fuck', data_str)
        data_str = re.sub(r'\bwth\b', 'what the hell', data_str)
        data_str = re.sub(r'\br\b', 'are', data_str)
        data_str = re.sub(r'\bu\b', 'you', data_str)
        data_str = re.sub(r'\bk\b', 'OK', data_str)
        data_str = re.sub(r'\bsux\b', 'sucks', data_str)
        data_str = re.sub(r'\bno+\b', 'no', data_str)
        data_str = re.sub(r'\bcoo+\b', 'cool', data_str)
        data_str = re.sub(r'rt\b', '', data_str)
        data_str = data_str.strip()
        return data_str

    # remove non ASCII characters
    @classmethod
    def strip_non_ascii(self, data_str):
        ''' Returns the string without non ASCII characters'''
        stripped = (c for c in data_str if 0 < ord(c) < 127)
        return ''.join(stripped)

    # check to see if a row only contains whitespace
    @classmethod
    def check_blanks(self, data_str):
        is_blank = str(data_str.isspace())
        return is_blank

    # @classmethod
    # def remove_stopwords(self, df, col_in, col_out):
    #     remover = StopWordsRemover(stopWords=StopWordsRemover.loadDefaultStopWords('english'))
    #     remover.setInputCols([col_in])
    #     remover.setOutputCols([col_out])
    #     return remover.transform(df)

    @classmethod
    def remove_stopwords(self, data_str):
        from nltk.corpus import stopwords
        stop_words = set(stopwords.words('english'))
        word_list = data_str.split()
        filtered_words = [w for w in word_list if not w in stop_words]
        return ' '.join(filtered_words)




    # Part-of-Speech Tagging
    @classmethod
    def tag_and_remove(self, data_str):
        cleaned_str = ' '
        # noun tags
        nn_tags = ['NN', 'NNP', 'NNP', 'NNPS', 'NNS']
        # adjectives
        jj_tags = ['JJ', 'JJR', 'JJS']
        # verbs
        vb_tags = ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ']
        nltk_tags = nn_tags + jj_tags + vb_tags

        # break string into 'words'
        text = data_str.split()

        # tag the text and keep only those with the right tags
        tagged_text = pos_tag(text)
        for tagged_word in tagged_text:
            if tagged_word[1] in nltk_tags:
                cleaned_str += tagged_word[0] + ' '

        return cleaned_str

    # lemmatization
    @classmethod
    def lemmatize(self, data_str):
        # expects a string
        list_pos = 0
        cleaned_str = ''
        lmtzr = WordNetLemmatizer()
        text = data_str.split()
        tagged_words = pos_tag(text)
        for word in tagged_words:
            if 'v' in word[1].lower():
                lemma = lmtzr.lemmatize(word[0], pos='v')
            else:
                lemma = lmtzr.lemmatize(word[0], pos='n')
            if list_pos == 0:
                cleaned_str = lemma
            else:
                cleaned_str = cleaned_str + ' ' + lemma
            list_pos += 1
        return cleaned_str




    ###############################################################################



strip_non_ascii_udf = udf(TextCleaner.strip_non_ascii, StringType())
check_blanks_udf = udf(TextCleaner.check_blanks, StringType())
# check_lang_udf = udf(TextCleaner.check_lang, StringType())
remove_stopwords_udf = udf(TextCleaner.remove_stopwords, StringType())
fix_abbreviation_udf = udf(TextCleaner.fix_abbreviation, StringType())
remove_features_udf = udf(TextCleaner.remove_features, StringType())
tag_and_remove_udf = udf(TextCleaner.tag_and_remove, StringType())
lemmatize_udf = udf(TextCleaner.lemmatize, StringType())