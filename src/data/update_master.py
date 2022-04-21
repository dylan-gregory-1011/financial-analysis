##!/usr/bin/env python
"""
Update Stock Master Data: 

This class should be utilized once every month to help account for new companies and companies that have ticker's that have changed. This will also update the sectors that are being used the adequate cik numbers that are to be used

    def getActiveStocks() -> None : Run once the nasdaq and nyse files have been updated, this updates the stock master table
    def getCIKFromSEC() -> None : updating the correct cik values from the sec's website

Need to ensure that we have all listed companies' full history. Go through each companies' listed page and see whether there is a previously listed company.  Extract that value and look through the index values to find the previous cik

    - Figure out which features to store.
    - Data Validation for features
    - Calculations for all the Simply Wall Street formulas
    - Figure out how to connect companies' that change their cik formatting.
"""

#Imports
import pandas as pd
import time
from requests import get
import logging
import re

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2019 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

# Constants
HEADERS = {
    'User-Agent': 'dylans-app/0.0.1'
}

class UpdateMaster(object):
    def __init__(self, proc_path):
        """ 
        Object that downloads SEC Filings and archives them into the correct data directory
        ...
        Parameters
        ----------
        proc_path: directory where the csv for all companies in the market are stored as well as the output directory
        sql: A SQL object to connect to
        """
        self.logger = logging.getLogger('filings.MasterUpdate')
        self.proc_path = proc_path
        self.logger.info(" Object Instantiated Succesfully")

    def getActiveStocks(self):
        """ 
        Update the active stock list.  Download the current NYSE and Nasdaq stock lists from the Nasdaq website and reformat the data to match the Stock Master table and then only take companies that are listed on the market forms.
        ...
        Parameters
        ----------
        path: The path where the recently downloaded csv files are stored (A Pathlib path)
        """
        # Read the file into a dataframe and get all old data
        stocks_DF = pd.read_csv(self.proc_path.joinpath('stocks_master.csv.gz'),
                                compression='gzip')
        active_stocks = pd.DataFrame()
        logging.info("Downloading newest file for listed companies on NYSE and NASDAQ")
        for mrkt in ['nyse', 'nasdaq']:
            # Read the data into a dataframe
            df = pd.read_csv(self.proc_path.joinpath('stocks_master','%s.csv' % mrkt),
                             header = 0)

            # Merge the two files togeter
            active_stocks = pd.concat([df, active_stocks])

        logging.info("Downloaded listed companies currently on the NASDAQ and NYSE. Formatting to DB")
        active_stocks.drop(['Last Sale','Market Cap'], axis = 1, inplace = True)
        active_stocks.rename({'Symbol':'ticker', 'Name':'name', 'Sector': 'sector','IPO Year':'ipoyear', 'Industry': 'industry'}, axis = 1, inplace = True)
        active_stocks['ticker'] = active_stocks['ticker'].str.replace('^','.', regex=True)
        active_stocks.drop_duplicates(inplace = True)

        index_DF = stocks_DF[stocks_DF['industry'] == 'Index']
        new_stocks = pd.merge(active_stocks[['ticker','name','ipoyear','sector','industry']],
                              stocks_DF[['ticker','last_update']], how = 'left', on = ['ticker'])

        #insert final values into the table
        df_final = pd.concat([index_DF, new_stocks])
        df_final.sort_values(['ticker'], inplace = True)
        out_cols = ['ticker','name','ipoyear','sector','industry','last_update']
        df_final[out_cols].to_csv(self.proc_path.joinpath('stocks_master.csv.gz'),
                                  index = False,
                                  compression='gzip')

    def getCIKFromSEC(self):
        """ 
        Update the tickers with the correct cik value so that attributes with multiple CIK's can remove duplicate Companies or erroneously attributed values.  This scans the SEC website for the proper attribute
        """
        # Read the tickers 
        df_tickers = pd.read_csv(self.proc_path.joinpath('stocks_master.csv.gz'),
                                compression='gzip')
        
        # Get all tyhe unique tickers to output
        tickers = df_tickers['ticker'].tolist()
        tickers.sort()
        CIK_RE = re.compile(r'.*CIK=(\d{10}).*')
        count, len_t = 1, len(tickers)
        # instantiate the lists for the object
        ticker_out, cik_out = [], []
        for tick in tickers:
            url = 'http://www.sec.gov/cgi-bin/browse-edgar?CIK=%s&Find=Search&owner=exclude&action=getcompany' % tick
            try:
                cik = CIK_RE.findall(get(url, headers = HEADERS).text)[0]

                # Add the ticker and cik to the arrays
                ticker_out.append(tick)
                cik_out.append(cik)

                self.logger.info("CIK for %s Downloaded: %.2f" % (tick, (count / len_t)* 100))
            except:
                self.logger.info("CIK Failed for %s Downloaded: %.2f" % (tick, (count / len_t)* 100))
                pass

            count+=1
            time.sleep(0.14)
        
        # Write these values to an output csv
        df_out = pd.DataFrame({'cik': cik_out, 'ticker': ticker_out})
        df_out.to_csv(self.proc_path.joinpath('ticker_cik.csv.gz'),
                      index = False,
                      compression='gzip')