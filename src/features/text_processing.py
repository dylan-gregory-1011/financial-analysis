##!/usr/bin/env python
"""
Data Featurizer for Risk Factor Analysis. Moving to MongoDB
https://towardsdatascience.com/tf-idf-for-document-ranking-from-scratch-in-python-on-real-world-dataset-796d339a4089

"""

import pandas as pd
import json
import spacy
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
import string
import re
from fuzzywuzzy import fuzz

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2020 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

nlp = spacy.load("en_core_web_sm")

class TextFeatures(object):
    def __init__(self, mongo):
        """ To initialize the object, pass a mongodb connection
        """
        self.mongo = mongo

    def featurizeRiskFactors(self, proc_path):
        """ Function that takes the risk factors from all the companies and makes a word chart
        """
        # Get data from the mongodb and get the 5 largest entries per ticker
        db = self.mongo['riskfactors']
        dfr = pd.DataFrame(list(db.find()))
        dfr['date'] = pd.to_datetime(dfr['date'])
        dfr = dfr[dfr['ticker'] != ''][['date','ticker','risktext']].groupby('ticker').apply(lambda x: x.nlargest(5, 'date')).reset_index(drop=True)
        dfr['risktext'] = dfr['risktext'].str.strip()

        remove = []
        # filter out quarterly reports that dont have any material updates to the risk factors.
        for ix, row in dfr.iterrows():
            to_remove = 0
            splt_txt = row['risktext'].split(' ')

            if len(splt_txt) < 30:
                remove.append(1)
                continue
            quotes = ['There have been no material changes in risks','As a smaller reporting company we are not required']
            for quote in quotes:
                for i in range(0,65):
                    quote_check = ' '.join(splt_txt[i: i + 7])
                    if fuzz.ratio(quote_check, quote) > 65:
                        to_remove = 1
                        for j in range(i,110):
                            new_quote_check = ' '.join(splt_txt[j: j + 4])
                            if fuzz.ratio(new_quote_check, 'except for the following') > 65:
                                to_remove = 0
                                break
                        break
                if to_remove == 1:
                    break

            remove.append(to_remove)

        dfr['removal'] = remove
        dfr = dfr[dfr['removal'] == 0]
        dfr['risktext'] = dfr[['ticker','risktext']].groupby(['ticker'])['risktext'].transform(lambda x: ' '.join(x))
        dfr = dfr[['ticker','risktext']].drop_duplicates(subset = ['ticker','risktext'])
        documents = dfr['risktext'].tolist()

        #fit a tfidf object to the corpus and find the highest associated words with each company
        tfidf = TfidfVectorizer(stop_words = 'english',
                                ngram_range=(1,3),
                                max_features = 50000)

        X = tfidf.fit_transform(documents)
        features = tfidf.get_feature_names()

        # Go through all of the reviews and sort the features based on their applicability. Add to the dataframe
        reviews = []
        for ix, row in enumerate(X):
            row = np.squeeze(X[ix].toarray())
            topn_ids = np.argsort(row)[::-1][:40]
            words = [features[i] for i in topn_ids]
            reviews.append(words)

        dfr['riskwords'] = reviews
        dfr.drop(columns = ['risktext'], inplace = True)
        dfr.set_index('ticker', inplace = True)
        dfr.to_json(proc_path.joinpath('riskfactors.json'),orient = 'index')
