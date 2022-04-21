##!/usr/bin/env python
"""
Financial Analysis project.  Client that uploads the text data into a MongoDB NOSQl db
object MongoClient:
    def insertData() -> Inserts all of the raw risk factor data into the mongo database (risk_factors collection)
    def insertNewsData() -> Inserts all of the news articles from the Guardian as well as NYT.
"""
#Imports
import pandas as pd
import os

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2019 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

class MongoClient(object):

    def __init__(self, mongo, sql_mstr):
        """ Mongo Client used to connect to the MongoDB
        """
        self.mongo = mongo
        self.sql_mstr = sql_mstr

    def insertData(self, raw_dir):
        """ Function that inserts data into the Mongodb
        """
        raw_files = set(os.listdir(raw_dir))
        self.mongo.command( "compact", 'riskfactors')
        db = self.mongo['riskfactors']
        db.remove({})

        for file in raw_files:
            cik = file.split('.')[0]
            try:
                symbol = self.sql_mstr.readSQL("SELECT ticker FROM ticker_cik where cik = %i" % int(cik))['ticker'][0]
            except:
                symbol = ''

            df = pd.read_csv(raw_dir.joinpath(file),
                                compression = 'gzip',
                                sep = '\t',
                                index_col = False,
                                encoding = 'utf-8',
                                lineterminator = '\n')
            df.drop_duplicates(inplace = True)
            df.rename(columns = {"Unnamed: 0":"date"}, inplace = True)
            df['ticker'] = symbol
            df['cik'] = cik
            data = df.to_dict(orient='records')
            db.insert_many(data, ordered = False)

    def insertNewsData(self, raw_dir,):
        """ Function that inserts data into the Mongodb
        """
        db = self.mongo['news']
        db.remove({})
        self.mongo.command( "compact", 'news')
        #run through guardian news files
        raw_files = set(os.listdir(raw_dir.joinpath('guardian')))

        for file in raw_files:
            print(file)
            iter_pd = pd.read_csv(raw_dir.joinpath('guardian',file),
                                compression = 'gzip',
                                sep = '\t',
                                index_col = False,
                                encoding = 'utf-8',
                                lineterminator = '\n',
                                chunksize = 10000)
            for chunk in iter_pd:
                chunk['section_name'] = file.split('.')[0]
                chunk['news_source'] = 'guardian'
                data = chunk.to_dict(orient='records')
                db.insert_many(data, ordered = False)

        #run through new york times articles
        years = set(os.listdir(raw_dir.joinpath('nyt')))
        for yr in years:
            months = set(os.listdir(raw_dir.joinpath('nyt',yr)))
            for mn in months:
                print(raw_dir.joinpath('nyt', yr, mn))
                iter_pd = pd.read_csv(raw_dir.joinpath('nyt', yr, mn),
                                    compression = 'gzip',
                                    sep = '\t',
                                    index_col = False,
                                    encoding = 'utf-8',
                                    lineterminator = '\n',
                                    chunksize = 10000)
                for chunk in iter_pd:
                    chunk['news_source'] = 'new_york_times'
                    data = chunk.to_dict(orient='records')
                    db.insert_many(data, ordered = False)
