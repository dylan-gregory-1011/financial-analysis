##!/usr/bin/env python
"""
Stock API Download: Brings down data from the TDAmeritrade stock API into the tables.

object TDClient:
    def _headers() -> dict: function that returns the header necessary for the TD Ameritrade API
    def refreshAPIKey() -> None: If the API Key is outdated, get a new API key and update the parameters in the object
    def updateStockHistory() -> date: Update the stock history and return the max date for the master table
    def api_call() -> Calls the api with the appropriate parameters and returns the function    
"""

#Imports
import json
import requests
import pandas as pd
import time
from urllib.parse import quote_plus
from datetime import datetime, timedelta
import logging
from .api_call import api_call
import math

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2019 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

#constants
BASE = 'https://api.tdameritrade.com/v1/'
AUTH = BASE + 'oauth2/token'
HISTORY = BASE + 'marketdata/%s/pricehistory?periodType=month&frequencyType=daily&startDate=%s&endDate=%s'

class TDClient(object):
    def __init__(self, key, proc_path, mstr_path):
        """ 
        An object that allows access to the TD Ameritrade website and download new stock information
        ... 
        Parameters
        ----------
        key: The path to the api token file with the current refresh token
        proc_path: The directory with the processeed data
        """
        self.logger = logging.getLogger('stocks.TDClient')
        self.get_new_key = False
        self.api_token = key
        self.proc_path = proc_path
        self.mstr_path = mstr_path
        self.df_mstr = pd.read_csv(mstr_path.joinpath('company_info1.tsv.gz'),
                                   compression='gzip',
                                    sep = '\t',)
        self.df_mstr['sector'] = self.df_mstr['sector'].fillna('no-sector')
        self.refreshAPIKey()

    def updateStockHistory(self):
        """ 
        This function downloads stock data into the analysis database. If the symbol returns an error, update the table with Bad Symbol
        """
        stocks_DF = self.df_mstr[['ticker','last_update','sector']]
        
        # Iterate through the stocks 
        for index, row in stocks_DF.iterrows():
            # Replace the start date
            if row['ticker'] in ['A','AA','AAC','AACG','AACI','AACIU','AACIW','AADI','AAIC']:
                continue
            print(row['ticker'])
            startDate = row['last_update']
            sector = row['sector'].lower().replace(' ','-')

            # Get the start date
            if startDate == (datetime.today() - timedelta(days = 3)).strftime('%Y-%m-%d') or \
                startDate == (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d') or \
                startDate == datetime.today().strftime('%Y-%m-%d'):
                self.logger.info("%s: Stock History Passed" % row['ticker'])
                continue
            elif math.isnan(startDate):
                # Read the file
                df = pd.read_csv(self.proc_path.joinpath('%s.tsv.gz' % sector),
                            compression = 'gzip',
                            sep = '\t',
                            index_col = False,
                            encoding = 'utf-8',
                            lineterminator = '\n')
                
                # Get list of dates
                dateFound = df[df['ticker'] == row['ticker']]['date'].tolist()

                # if there are no dates, set a long term initial date
                if len(dateFound) == 0:
                    startDate = '2009-06-30'
                else:
                    startDate = max(dateFound)
            
            if startDate is None:
                startDate = '2009-06-30'

            time.sleep(0.1)
            strt_dt = int((datetime.strptime(startDate, '%Y-%m-%d') + timedelta(days = 1)).timestamp() * 1000)
            end_dt = int(datetime.today().timestamp() * 1000)
            url = HISTORY % (row['ticker'], strt_dt, end_dt)

            # Get the stock data from the process
            try:
                x = self.api_call(url)
                df = pd.DataFrame(x['candles'])
                df['datetime'] = pd.to_datetime(df['datetime'], unit= 'ms')
                df['datetime'] = df['datetime'].dt.date
            except:
                self.df_mstr.loc[self.df_mstr['ticker'] == row['ticker'],'ipoyear'] = row['ticker']
                self.logger.info("%s: Stock History Failed" % row['ticker'])
                continue
            
            # Reformat the dataframe
            df.insert(0, 'ticker', row['ticker'])
            df.rename(columns = {'datetime': 'date'}, inplace = True)
            try:
                df = df[['ticker','date', 'open','high','low','close','volume']]
                # Update the master-data object
                self.df_mstr.loc[self.df_mstr['ticker'] == row['ticker'], 'last_update'] = max(df['date'])
                
                df.to_csv(self.proc_path.joinpath('%s.tsv.gz' % sector),
                        compression = 'gzip',
                        mode = 'a',
                        sep='\t',
                        index = False,
                        encoding='utf-8',
                        line_terminator = '\n')
                self.logger.info("%s: Stock History Downloaded" % row['ticker'])
            except:
                self.logger.info("%s: Stock History Failed for Ticker" % row['ticker'])
                pass

        # Write the master file to an output
        self.df_mstr.to_csv(self.mstr_path.joinpath('company_info1.tsv.gz'),
                        compression = 'gzip',
                        mode = 'w',
                        sep='\t',
                        index = False,
                        encoding='utf-8',
                        line_terminator = '\n')


    def refreshAPIKey(self):
        """ 
        Request API Refresh token when the current one expires (Every 15 minutes).
        """
        with open(self.api_token, "r") as file:
            api_key = json.load(file)

        if self.get_new_key :
            self.logger.info('refreshAPIKey')
            api_new_key = {val: api_key[val] for val in ['refresh_token', 'redirect_uri', 'client_id'] }
            api_new_key['grant_type'] = 'refresh_token'
            api_call = requests.post(AUTH, data= api_new_key)
            api_call_json = api_call.json()
            api_key['access_token'] = api_call_json['access_token']
            with open(self.api_token, "w") as file:
                json.dump(api_key, file)

        self._token = api_key['access_token']
        self._oath_user_id = api_key['client_id']
        self.get_new_key = False

    def _headers(self):
        """ 
        Setting the heading for the API call.  Need the token for a succesful call as well as a User Agent
        """
        return {'Authorization': 'Bearer ' + self._token, 'User-agent': 'Dylan' ,'Accept': 'application/json',}

    def api_call(self, url):
        ''' 
        The function that calls the api for both the history and fundamental information
            ::param url: The url to be called for the api
            return: an api call
        '''
        api_call = requests.get(url, headers= self._headers())

        while api_call.status_code != 200:
            if api_call.status_code == 401:
                self.get_new_key = True
                self.refreshAPIKey()
            elif api_call.status_code == 400:
                return 'Bad Call'
            elif api_call.status_code == 429:
                print('RateLimit')
                time.sleep(20)
            #recall the api request
            time.sleep(1)
            print(api_call.status_code)
            api_call = requests.get(url, headers= self._headers())
        
        return api_call.json()

    def dropDuplicatesFromEquities(self):
        '''
        
        '''
        # Get the name of all sectors in the dataset
        out_files = [x for x in self.proc_data.glob('**/*') if x.is_file()]

        for file in out_files:
            # Read the file
            df = pd.read_csv(file,
                            compression = 'gzip',
                            sep = '\t',
                            index_col = False,
                            encoding = 'utf-8',
                            lineterminator = '\n')

            # Drop duplicates from the dataset
            print(len(df['ticker']))
            df.drop_duplicates(subset=['date','ticker'], inplace = True )
            print(len(df['ticker']))
            break

            