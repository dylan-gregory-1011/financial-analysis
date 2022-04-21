##!/usr/bin/env python
"""
Fred External Data API Download: Brings down data from the Fred API

object TDClient:
    def _headers() -> dict: function that returns the header necessary for the TD Ameritrade API
    def updateExternalSeries() -> date: Update the series data for the external datasets
    def api_call() -> Calls the api with the appropriate parameters and returns the function

     "This product uses the FRED® API but is not endorsed or certified by the Federal Reserve Bank of St. Louis."

Guardian Data API Download: Brings down data from the Guardian API

object GuardianClient:
    def downloadNewsSeries() -> date: Update the series data for the external datasets
    def api_call() -> Calls the api with the appropriate parameters and returns the function

New York TImes Data API Download: Brings down data from the NYT API, Rate Limit = 4,000 per day and 10 per minute

object NYTClient:
    def downloadNewsSeries() -> date: Update the series data for the external datasets
    def api_call() -> Calls the api with the appropriate parameters and returns the function

"""

#Imports
import json
import pandas as pd
import time
import logging
from .api_call import api_call
from datetime import datetime, timedelta, date
from os.path import exists
import os
import requests

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2019 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

# URL & Request Constants
BASE_FRED = 'https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&observation_start=2008-01-01'
BASE_GUARDIAN = 'https://content.guardianapis.com/search?order-date=published&page-size=50&order-by=newest&show-fields=all&from-date=%s&to-date=%s&section=%s&api-key=%s'
BASE_WIKI = 'https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/%s/daily/%s/%s'
BASE_NYT = 'https://api.nytimes.com/svc/archive/v1/%i/%i.json?api-key=%s'
HEADERS = {'User-agent': 'Dylan/independent-financial-analysis' ,'Accept': 'application/json'}
CURR_DT = datetime.today().strftime('%Y-%m-%d')
TODAY = date.today()

# Wiki Constants
SEARCH = 'https://en.wikipedia.org/wiki/%s'
PREV_DT = (datetime.today() - timedelta(days = 1)).strftime('%Y%m%d') + '00'
BEGINNING_DT = '2015070100'
FIELDS = ['article','timestamp','views']
REPLACE = {"AAL":"American_Airlines", "AAN":"Aaron's%2C_Inc.",
            "AAPL":"Apple_Inc.", "ALL":"Allstate", "ALLT":"Allot", "AMAG":"AMAG_Pharmaceuticals",
            "AMC":"AMC_Theatres", 'AMTD':"TD_Ameritrade", "AQ":"Aquantia_Corporation", "BAC":"Bank_of_America"}
STR_CLEAN = ['_Inc.', '_Inc','n.a.', '_Plc', '._Ltd.','_Ltd.','_P.L.C.','_(The)','_Sa','_L.P.','_plc','_Resources','_LP','_Systems'\
            ,'_&_Co','_LLC','_SA','_L.L.C.','_Corporation','(The)','_Holdings','_Corp.','_Corp','_Limited','_Incorporated','_Group']

########################################################################################################################################
####################################################              FRED          #########################################################
########################################################################################################################################

class FredClient(object):

    def __init__(self, fred_info, data_path):
        """ 
        An object that downloads economic data from the fred API
        ...
        Parameters
        ----------
        fred_info: The path to the api token file with the current refresh token and time series data
        data_path: The folder where the data is saved
        """
        self.logger = logging.getLogger('stocks.FredClient')
        with open(fred_info, "r") as file:
            fred_api = json.load(file)
        self.api_token = fred_api['apikey']
        self.series = fred_api['series']
        self.data_path = data_path

    def updateExternalSeries(self):
        """ 
        Iterate through all the time series in the json driver file and download the data into the external_data database
        """
        for key, value in self.series.items():
            self.logger.info("Downloading Data for %s" % key)
            url = BASE_FRED % (value, self.api_token)
            x = api_call(url, HEADERS, 'json')
            df = pd.DataFrame(x['observations'])
            # Write the data to an output
            df[['date','value']].to_csv(self.data_path.joinpath('%s.csv.gz' % key),
                                        index = False,
                                        compression='gzip')
            time.sleep(3)
            self.logger.info("Downloaded Data for %s" % key)

########################################################################################################################################
####################################################         Guardian          #########################################################
########################################################################################################################################

class GuardianClient(object):

    def __init__(self, guardian_api, data_path):
        """ 
        An object that downloads data from the Guardian API
        ...
        Parameters
        ----------
        guardian_info: The path to the api token file with the current refresh token and time series data
        data_path: The folder where the data is saved
        """
        self.logger = logging.getLogger('news.GuardianClient')
        self.api_token = guardian_api['apikey']
        self.sections = guardian_api['sections']
        self.news_data = data_path.joinpath('guardian')
        logging.info("Loaded API Key for the Guardian")

    def downloadNewsSeries(self):
        """ 
        Iterate through all the time series in the json driver file and download the data into the external_data database
        """
        for section in self.sections:
            logging.info("Downloading Guardian Articles for section %s" % section)
            out_file = self.news_data.joinpath('%s.tsv.gz' % section)
            min_dt = '2005-01-01'
            if out_file.exists():
                iter_pd = pd.read_csv(out_file,
                                    compression = 'gzip',
                                    sep = '\t',
                                    encoding = 'utf-8',
                                    index_col = False,
                                    chunksize = 10000)
                chunk_list = []
                for chunk in iter_pd:
                    if max(chunk['date']) > min_dt:
                        min_dt = max(chunk['date'])
                    chunk_list.append(chunk)

                df_final = pd.concat([chunk[chunk['date'] < min_dt] for chunk in chunk_list])
            else:
                df_final = pd.DataFrame()

            max_dt, prev_max_dt = CURR_DT, CURR_DT

            while max_dt != min_dt:
                url = BASE_GUARDIAN % (min_dt, max_dt ,section, self.api_token)
                x = api_call(url, HEADERS, 'json')

                news_results = x['response']['results']
                dict_ = { 'headline': [], 'sectionID': [], 'country': [], 'date': [], 'full_text': []}

                for article in news_results:
                    dict_['headline'].append(article['fields']['headline'])
                    dict_['sectionID'].append(article['sectionId'])
                    dict_['date'].append(article['webPublicationDate'].split('T')[0])
                    dict_['full_text'].append(article['fields']['bodyText'])
                    try:
                        dict_['country'].append(article['fields']['productionOffice'])
                    except:
                        dict_['country'].append('')

                df = pd.DataFrame(dict_)
                #if the values returned are empty then exit
                try:
                    max_dt = min(df['date'])
                except:
                    break

                if max_dt == prev_max_dt:
                    max_dt = datetime.strftime(datetime.strptime(prev_max_dt, '%Y-%m-%d') - timedelta(1), '%Y-%m-%d')

                prev_max_dt = max_dt

                df_final = pd.concat([df, df_final])
                time.sleep(0.1)
                logging.info("Downloaded Guardian %s Articles from date %s" % (section, max_dt))

            df_final.sort_values(by=['date'], inplace = True, ascending = False)
            df_final.to_csv(out_file,
                        compression = 'gzip',
                        mode = 'w',
                        sep='\t',
                        index = False,
                        encoding='utf-8',
                        line_terminator = '\n')

            logging.info("Finished Downloading Guardian %s Articles" % section)

########################################################################################################################################
####################################################           NYT             #########################################################
########################################################################################################################################

class NYTClient(object):

    def __init__(self, nyt_api, data_path):
        """ 
        An object that downloads news articles from the New York Times API
        ...
        Parameters
        ----------
        guardian_info: The path to the api token file with the current refresh token and time series data
        raw_data: The folder where the data is saved
        """
        self.logger = logging.getLogger('news.GuardianClient')
        self.api_token = nyt_api['apikey']
        self.news_data = data_path.joinpath('nyt')
        logging.info("Loaded API Key for the NYT")

    def downloadNewsSeries(self):
        """ 
        Iterate through all the time series in the json driver file and download the data into the external_data database
        """
        try:
            os.makedirs(self.news_data.joinpath(str(year)))
        except:
            pass

        #Month and year date currently
        month, year = TODAY.month , TODAY.year

        if not self.news_data.joinpath(str(year), 'M%i.csv.gz' %  month).exists():
            if month == 1:
                yr_prev, mn_prev = year - 1, 12
            else:
                yr_prev, mn_prev = year, month - 1
            logging.info("Downloading New York Times Articles from %i month %i" % (yr_prev, mn_prev))
            self.outputAndFormat(yr_prev, mn_prev)

        logging.info("Downloading New York Times Articles from %i month %i" % (year, month))
        self.outputAndFormat(year, month)

    def outputAndFormat(self, year, month):
        """ 
        Output the articles and write the data into the correct file
        ...
        Parameters
        ----------
        year: The year the articles were written
        month: The month the articles were written
        """
        out_file = self.news_data.joinpath(str(year), 'M%i.csv.gz' %  month)
        url = BASE_NYT % (year, month, self.api_token)
        dict_ = {'headline': [], 'source': [], 'section_name': [], 'subsection_name': [], 'pub_date': [], 'web_url': [], 'keywords': [], 'lead_paragraph':[]}

        x = api_call(url, HEADERS, 'json')
        articles = x['response']['docs']

        for article in articles:
            if not isinstance(article['headline'], dict):
                continue

            for field in ['section_name', 'subsection_name', 'lead_paragraph','source','pub_date','web_url','keywords','headline']:
                if field == 'headline':
                    try:
                        dict_[field].append(article[field]['main'])
                    except:
                        dict_[field].append('None')
                else:
                    try:
                        dict_[field].append(article[field])
                    except:
                        dict_[field].append('None')

        df = pd.DataFrame(dict_)
        df.drop_duplicates(subset = 'headline', inplace = True)
        df.sort_values(by=['pub_date'], inplace = True, ascending = False)
        df.to_csv(out_file,
                compression = 'gzip',
                mode = 'w',
                sep='\t',
                index = False,
                encoding='utf-8',
                line_terminator = '\n')

########################################################################################################################################
##############################################           Wikipedia             #########################################################
########################################################################################################################################

class WikipediaScraper(object):

    def __init__(self, data_path):
        """
        This function downloads current Wikipedia data to feed the analytics model
        ...
        Parameters
        ----------
        proc_path: The directory where the master data as well as the data output is to be dropped
        """
        self.data_path = data_path

    def getLookupTerms(self):
        """ 
          A Function that gets the associated link to the Wikipedia archive for all the companies
        """
        # Read the data
        df_mstr = pd.read_csv(self.data_path.joinpath('stocks_master.csv.gz'),
                              compression='gzip')

        # Select the proper columns
        df_mstr = df_mstr[['ticker','name']]
        df_mstr['url_search'] = df_mstr['name'].apply(lambda x: x.strip().replace(',','')\
                                            .replace(' ', '_').replace('__','_').replace('&#39;', '\''))

        url_found, url_redi = [], []
        for index, row in df_mstr.iterrows():
            time.sleep(0.05)
            search = row['url_search']
            print(search)
            title = self.checkWikipediaURL(search)

            for str_cln in STR_CLEAN:
                if title != 'NONE':
                    break
                if str_cln in search:
                    search = search.replace(str_cln,'')
                    title = self.checkWikipediaURL(search)
            #append the title to the array of cleansed values
            df_mstr.loc[index, 'Term_Redirect'] = title

        # Write the data to a flat file
        df_mstr.to_csv(self.proc_path.joinpath('comp_search.csv.gz'),
                        index = False,
                        compression='gzip')

    def checkWikipediaURL(self, search):
        """ A function that check's the Wikipedia URL that was requested and gets the final re-direct
            ::param searc: The term being searched on Wikipedia
        """
        try:
            query = requests.get(r'https://en.wikipedia.org/w/api.php?action=query&titles={}&&redirects&format=json'.format(search))
        except:
            return 'None'
        if query is None:
                return 'NONE'
        data = json.loads(query.text)['query']['pages']

        for key, val in data.items():
            title = data[key]['title']
            if '-1' in data.keys():
                title = 'NONE'
            break

        return title

    def downloadPageviewCount(self, df_mstr):
        url_found, url_redi = [], []
        for index, row in df_mstr.iterrows():
            search = row['url_search']
            results = api_call(BASE_WIKI % (search, BEGINNING_DT, PREV_DT), HEADERS, 'full')
            pg_cnt, url_redirect = results.json(), results.url
            print(results.url)
            if pg_cnt is None:
                for str_cln in STR_CLEAN:
                    if str_cln in search:
                        search = search.replace(str_cln,'')

                        results = api_call(BASE_WIKI % (search, BEGINNING_DT, PREV_DT), HEADERS, 'full')
                        pg_cnt, url_redirect = results.json(), results.url
                        if pg_cnt is not None:
                            break

                if pg_cnt is None:
                    url_redi.append('')
                    url_found.append(False)
                    continue
                print(url_redirect)
                print(search)

            df = pd.DataFrame(pg_cnt['items'])[FIELDS]
            df.to_csv(self.data_path.joinpath('wiki_results.csv.gz'),
                    compression = 'gzip',
                    mode = 'a',
                    sep='\t',
                    header = False,
                    encoding='utf-8')

            df['timestamp'] = df['timestamp'].str.slice(stop=8)
            url_found.append(True)
            url_redi.append(url_redirect)
            time.sleep(0.1)
            break

        df_mstr['url_found'] = url_found
        df_mstr['Term_Redirect'] = url_redi
        return df_mstr