import os
from pathlib import Path
from scipy.special import softmax
import pandas as pd

from transformers import TFAutoModelForSequenceClassification
from transformers import AutoTokenizer, AutoConfig






class SentimentPredictor:
    def __init__(self, model_name='twitter-xlm-roberta-base-sentiment'):
        model_path= Path(os.getcwd()).parent.parent.joinpath(model_name)
        MODEL = model_path
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL)
        self.config = AutoConfig.from_pretrained(MODEL)
        self.model = TFAutoModelForSequenceClassification.from_pretrained(MODEL)
        # self.model.eval()

    def _preprocess(self, text):
        new_text = []
        for t in text.split(" "):
            t = '@user' if t.startswith('@') and len(t) > 1 else t
            t = 'http' if t.startswith('http') else t
            new_text.append(t)
        return " ".join(new_text)
    
    def predict(self, X: pd.Series):
        df = pd.DataFrame({'tweet': X})
        df['text'] = df['tweet'].apply(lambda x: self._preprocess(x))
        encoded_inputs = self.tokenizer.batch_encode_plus(df['text'].tolist(), max_length=self.config.max_length, padding=True, return_tensors='tf')
        output = self.model(encoded_inputs)
        scores = output[0].numpy()
        scores = softmax(scores, axis=1)
        df_preds = pd.DataFrame(scores, columns=list(self.config.id2label.values()))
        df_preds['label'] = df_preds.idxmax(axis=1)
        df = pd.concat([df, df_preds], axis=1)
        return df


