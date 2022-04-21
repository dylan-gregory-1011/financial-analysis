##!/usr/bin/env python
"""
Financial Analysis project downloader.  This allows for the download of the different datasets (SEC, Equities, external data, etc) and
is run on my raspberry pi to aggregate all of the data
    
    def WikipediaData() -> Old function that downloads all of the Wikipedia visits for each listed company
    def downloadExternalData() -> Function that downloads external data from FRED's api as well as news articles from NYT
    def updateCompanyList() -> Master data function that downloads all of the currently listed companies from the exchange websites
    def downloadStockHistory() -> Downloads all EOD stock prices from TD Ameritrade's website.
    def downloadAndFormatSECData() -> Download and format the SEC data from EDGAR
"""

#Imports
from pathlib import Path
import pandas as pd
import sys
import numpy as np
from data import SECFilingDownload, TDClient, SECFilingFormatter, UpdateMaster, FredClient, GuardianClient, NYTClient, WikipediaScraper
from datetime import datetime, date
from os.path import exists
import json
import logging

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2019 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

#constants
PROJ = Path(__file__).resolve().parent.parent
logger = logging.getLogger(__name__)
DATA_DIR = PROJ.joinpath('data')
NEWS_DIR = DATA_DIR.joinpath('news')
FRED_DIR = DATA_DIR.joinpath('fred')
WIKI_DIR = DATA_DIR.joinpath('wiki')
SEC_DIR = DATA_DIR.joinpath('sec')
EQUITIES_DIR = DATA_DIR.joinpath('equities')
MASTER_DIR = DATA_DIR.joinpath('master')
CURR_DT = datetime.today().strftime('%Y-%m-%d')

def WikipediaData(run_type):
    """ 
    Function that gets Wikipedia lookup terms as well as the pagecount's for the associated terms
    ...
    Parameters
    ----------
    run_type: How to get the wikipedia data 
    """
    wiki = WikipediaScraper(proc_path = WIKI_DIR)
    if run_type == 'get_terms':
        print('Getting Lookup Terms')
        wiki.getLookupTerms()
    else:
        print('Getting PageView Counts')
        #wiki.downloadPageviewCount()

def downloadExternalData():
    """ 
        Download external datasets such as news sources, indicators from Fred and 
    """
    logging.basicConfig(level=logging.INFO, 
                        format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt= '%m-%d %H:%M', 
                        filename=PROJ.joinpath('logs',CURR_DT + '_external_dwnld.log'), 
                        filemode = 'w')
    
    # Download the news sources
    for src in ['guardian','nyt']:
        logger.info('Beginning Download Of News Data for %s' % src)
        with open(PROJ.joinpath('keys','%sconfig.json' % src), "r") as file:
            api = json.load(file)
        if src == 'guardian':
            dwnld = GuardianClient(guardian_api = api, data_path = NEWS_DIR)
        elif src == 'nyt':
            dwnld = NYTClient(nyt_api= api, data_path= NEWS_DIR)
        dwnld.downloadNewsSeries()
        logger.info('Download for %s completed succesfully' % src)

    # Download the indicator data from Fred
    logger.info('Beginning Download Of External Datasets')
    fred = FredClient(fred_info= PROJ.joinpath('keys','fredconfig.json'), data_path = FRED_DIR)
    fred.updateExternalSeries()
    logger.info('Finished Download Of External Datasets')


def updateListedCompanies():
    """ 
    Function that updates the company master table in the database to allow for adequate analysis
    """
    logging.basicConfig(level=logging.INFO, format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt= '%m-%d %H:%M', 
                        filename=PROJ.joinpath('logs',CURR_DT + '_updt_mstr.log'), 
                        filemode = 'w')
    update_mstr = UpdateMaster(proc_path = MASTER_DIR)
    update_mstr.getActiveStocks()
    logging.info("Finished updating active stocks. Moving to get Ticker and CIK values")
    update_mstr.getCIKFromSEC()
    logging.info("Finished updating tickers-cik, formatting values")

def downloadStockHistory():
    """ 
    Function that updates the company master table in the database to allow for adequate analysis
    """
    #Function that downloads the stock data and updates it in the database
    logging.basicConfig(level=logging.INFO, 
                        format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt= '%m-%d %H:%M', 
                        filename=PROJ.joinpath('logs',CURR_DT + '_stocks.log'), 
                        filemode = 'w')

    logger.info('Initializing Stock Client')
    downloader = TDClient(PROJ.joinpath('keys','tdconfig.json'), proc_path = EQUITIES_DIR)
    logger.info("Downloading Stock Data")
    downloader.updateStockHistory()
    #SQL_EQUITIES.executeSQL("INSERT OR IGNORE INTO dly_stock_hist VALUES (%s)", df_updt)
    logger.info("Finished Downloading Stock Data")

def downloadAndFormatSECData(full_load = False):
    """ 
    Function that downloads the SEC data and then passes the files that are to be formatted
    ...
    Parameters
    ----------
    full_load: A boolean, if a full load, go through all zip files, if not, just get the updated files
    """
    logging.basicConfig(level=logging.INFO, 
                        format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt= '%m-%d %H:%M:%S', 
                        filename=PROJ.joinpath('logs',CURR_DT + '_sec_filings.log'), 
                        filemode = 'w')
    logger.info("Instantiating SEC Filing Objects")
    sec_downloader = SECFilingDownload(sec_data = SEC_DIR)
    sec_formatter = SECFilingFormatter(sec_data = SEC_DIR, master_data = MASTER_DIR)

    # Get the date and the new zip files
    today = date.today()
    qtr, year = ((today.month - 1) // 3) + 1, today.year
    zip_name = "%sQTR%s.zip" % (year, qtr)
    zip_files = [(zip_name, year, qtr)]

    if not SEC_DIR.joinpath('raw-filings', zip_name).exists():
        if qtr == 1:
            year -= 1
            qtr = 4
        else:
            qtr -=1
        zip_files.append(("%sQTR%s.zip" % (year, qtr), year, qtr))

    if full_load:
        # If we want a full load, iterate over all of the zipped files
        for year in range(2010, 2022):
            for qtr in range(1, 5):
                zip_name = "%sQTR%s.zip" % (year, qtr)
                logging.info(" Download succesful, formatting files from %i, QTR %i" %(year, qtr))
                sec_formatter.formatFilings(dir = zip_name, files_to_format =  None)
                logging.info("Formatting Succesful for %i QTR %i" % (year, qtr))
    else:
        # Iterate through the zip files that we have found
        for (zip_name, year, qtr) in zip_files:
            logging.info(" Downloading SEC Filings for %i, QTR %i" %(year, qtr))
            new_files = sec_downloader.updateFilings(year = year, qtr = "QTR%s" % qtr)
            logging.info(" Download succesful, formatting files from %i, QTR %i" %(year, qtr))
            sec_formatter.formatFilings(dir = zip_name, files_to_format =  new_files)
            logging.info("Formatting Succesful for %i QTR %i" % (year, qtr))

def downloadQuarterlyFundamentalData():
    '''
    Archived Quarterly SEC download using the Excel data made available through EDGAR
    
    '''
    to_run = 'format'
    """if to_run == 'download':
        sec_dwnld = SECFilingXLSDownload(PROC_DATA,INDX_DATA)
        df_cik = pd.read_csv(PROC_DATA.joinpath('ticker_cik.csv.gz'),
                             compression='gzip')
        # Apple Q4, Boeing Filter out no date headers
        #['AAPL','MSFT','JPM','QCOM','HD','K','BYND','BA','DIS']
        for cik in df_cik[df_cik['ticker'].isin(['BA'])]['cik'].tolist():
            for yr in range(2019,2020):
                for qtr in range(1,5):
                    print("Downloading Fundamentals for company: %i" % cik)
                    print('download for -> %i : qtr %i' % (yr, qtr))
                    sec_dwnld.updateFundamentalData(yr,qtr, cik)
            return
    else:
        raw_review_files = [x for x in PROC_DATA.joinpath('new_filings').glob('**/*') if x.is_file()]
        for file in raw_review_files:
            ticker = file.name.split('.')[0]
            xlformat = SECFilingXLSFormatter(PROC_DATA.joinpath('new_filings'))
            xlformat.formatCompaniesFilings(ticker)
            break"""

def main(download_to_run= None):
    if download_to_run == 'external':
        downloadExternalData()
    elif download_to_run == 'stocks':
        downloadStockHistory()
    elif download_to_run == 'sec':
        downloadAndFormatSECData(full_load = False)
    elif download_to_run == 'update_master':
        updateListedCompanies()

if __name__ == '__main__':
    #import the process to run
    main(download_to_run = sys.argv[1])
