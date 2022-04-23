##!/usr/bin/env python
"""
Financial Analysis Project:  This file downloads the data from SEC 10-Q and 10-K filings and builds a table that allows for the specific attribute to be analyzed in isolation from the other attributes.

    def formatFilings() -> None: Driver function that iterates through all the filings and inserts the data into the correct output
    def getDatesAndPeriods() -> Dict: returns a dictionary of date tags as well as the period length and end date
    def getValuesFromFile() -> Dict: A dictionary of all the tags in an sec filing
    def buildFilingDataset() -> None: Function that outputs a file to a table and calculates qtr and yr pct change

    Version 2 Updates:
        - Final function writes only companies that are listed on the stock market into the table
        - Goes through the compressed csv to get the most accurate quarterly values in each date and estimate the
            values as a lest resort
        - Takes the raw data and consolidates it into a compressed csv representing each company.
        - Calculated the Abs value for
        - How do we want to calculate pct diff?  yr / yr or qtr / yr prv qtr
"""

from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import zipfile
import logging
pd.options.mode.chained_assignment = None

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2019 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "2.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

class SECFilingFormatter(object):


    def __init__(self, sec_data, master_data):
        """ 
        Object that downloads SEC Filings and archives them into the correct data directory
        ...
        Parameters
        ----------
        raw_data: directory where the raw data has been saved
        proc_data: directory that the cleansed SEC output is saved
        """
        self.logger = logging.getLogger('filings.SECFilingFormatter')
        self.raw_data = sec_data.joinpath('raw-filings')
        self.proc_data = sec_data.joinpath('processed-filings')
        self.agg_data = sec_data.joinpath('aggregated-filings')
        self.cik_df = pd.read_csv(master_data.joinpath('ticker_cik.csv.gz'),
                                  compression='gzip')
        self.ciks = self.cik_df['cik'].tolist()
        self.logger.info(" Object Instantiated Succesfully")

    def formatFilings(self, yr_qtr, files_to_format = None):
        """ 
        Cleanses and formats the filing data in the final reporting table as well as a processed file for each cik
        ...
        Parameters
        ----------
        yr_qtr: A tuple with the year and qtr that the data was submitted (year, quarter)
        files_to_format: a list of files to format for delta loads
        """
        # Get the year and qtr values out of the variable name and define the zip name
        year = yr_qtr[0]
        qtr = yr_qtr[1]
        zip_name = "%sQTR%s.zip" % (year, qtr)
        filing_dir = self.raw_data.joinpath(zip_name)
        
        # Get the files to format from the list
        if files_to_format == None:
            with zipfile.ZipFile(filing_dir, 'r') as zip:
                self.files = zip.namelist()
        else:
            self.files = files_to_format

        for file in self.files:
            #self.logger.info("%s: Formatting and compiling" % file)
            with zipfile.ZipFile(filing_dir, 'r') as zip:
                soup = BeautifulSoup(zip.read(file).decode("utf-8", "ignore"), 'lxml')

            #get the taglist from the objects, derive dates
            tries, self.dates = 0, {}
            while not bool(self.dates) and tries < 5:
                tag_list = soup.find_all()
                self.dates = self.getDatesAndPeriods(tag_list, 'xbrli:')
                if not bool(self.dates):
                    self.dates = self.getDatesAndPeriods(tag_list, '')
                tries +=1

            #if dates are None, move onto next file or get the filings from the documents and format the symbol as well as the file name
            if not bool(self.dates):
                continue

            #get the cik and symbol from the file name
            try:
                cik, symbol = file.split('_')[0].split('-')
            except:
                cik, symbol = file.split('_')[0], 'MISSING'

            if len(symbol) > 5:
                try:
                    symbol = self.cik_df[self.cik['cik'] == int(cik)]['ticker'][0]
                except:
                    continue
            
            #Create the proper dataframe and append the values together (Work through formatting)
            df_qtr = self.getValuesFromFile(tag_list)
            df_qtr.insert(0, 'qtr_sub', qtr)
            df_qtr.insert(0, 'year_sub', year)
            df_qtr.insert(0, 'ticker', symbol.upper())
            df_qtr.insert(0, 'cik', cik)
            df_qtr['pds'] = df_qtr['pds'].astype(int)
            df_qtr['cik'] = df_qtr['cik'].astype(int)

            # name the file to pull, and if it exists, read the data in from the prior writing
            comp_file = self.proc_data.joinpath('%s.tsv.gz' % cik)
            
            # Write the new data to an output file so that we can use this to calculate values moving forward
            if comp_file.exists():
                mode = 'a'
                header = False 
            else:
                mode = 'w'
                header = True
            
            # Write to the file
            df_qtr.to_csv(comp_file,
                        compression = 'gzip',
                        mode = mode,
                        header= header,
                        sep='\t',
                        index = False,
                        encoding='utf-8',
                        line_terminator = '\n')

    def buildAggregatedDataset(self, files_to_format = None):
        """ 
        Output the filing data to a table to be used by the models.  Also, calculate the qtr and yr pct change
        ...
        Parameters
        ----------
        inpt: input file to analyze (file name)
        df: The aggregated dataframe to use for the output
        """
        if files_to_format is None:
            out_files = [x for x in self.proc_data.glob('**/*') if x.is_file()]
        else:
            out_files = []
            for file in out_files:
                # Get the CIK and ticker from the file 
                cik, symbol = str(file).split('_')[0].split('-')
                comp_file = self.proc_data.joinpath('%s.tsv.gz' % cik)
                out_files.append(comp_file)
        
        print(len(out_files))
        for comp_file in out_files:
            df = pd.read_csv(comp_file,
                            compression = 'gzip',
                            sep = '\t',
                            index_col = False,
                            encoding = 'utf-8',
                            lineterminator = '\n',
                            dtype = {'pds': 'int'})

            # Drop all duplicates
            df = df.drop_duplicates(keep = "first").reset_index(drop= True)

            #get the minimum periods per acct_typ, attribute and date
            df['pds'].fillna(0, inplace = True)
            df = df.loc[df.groupby(['acct_typ', 'attr', 'date'])['pds'].idxmin(), :]

            cik = int(str(comp_file.name).split('.')[0])
            #if the company doesn't have a associated stock, skip (in order to reduce values)
            if cik not in self.ciks:
                print('Skipping cik %i due to no listed stock price' % cik)
                continue

            try:
                curr_ticker = self.cik_df.loc[self.cik_df['cik'] == cik]['ticker'].values[0]
            except:
                curr_ticker = ''
                pass

            df['ticker'] = curr_ticker
            df = df[((df['acct_typ'] == 'dei') & (df['attr'] == 'entitycommonstocksharesoutstanding')) | (df['acct_typ'] != 'dei')]
            df.sort_values(by = ['acct_typ', 'attr', 'date'], inplace = True)
            df.reset_index(inplace = True, drop = True)

            df_unique = df.groupby(['attr'])['pds'].min().reset_index(level=0)
            attrs = list(df_unique[df_unique['pds'] == 4]['attr'])

            df.loc[:,'nxt_attr'] = df['attr'].shift(1)
            cond_match = (df['attr'] == df['nxt_attr'])
            try:
                df.loc[cond_match,'dt_diff'] = abs(round((((df['date'].apply(pd.to_datetime)).shift(1)) - df['date'].apply(pd.to_datetime)).dt.days * 4 / 365.25 )).fillna(0).astype(int)
            except:
                print('Skipping agg for cik %i' % cik)
                continue

            #divide values where there is no way to subtract previous periods
            df.loc[:,'prev_val'] = df['value'].shift(1)
            cond_init = (df['pds'] > 1) & ((df['attr'] != df['nxt_attr']) | (df['dt_diff'] >= df['pds'])) & (~df['attr'].isin(attrs))
            df.loc[cond_init, ['value', 'pds']] = df.loc[cond_init,'value'] / df.loc[cond_init,'pds'], 1

            #For all values that have one quarter difference, subtract previous values
            df.loc[cond_match,'prev_val'] = df['value'].shift(1)
            df.loc[cond_match,'pd_diff_1'] = df['pds'].shift(1)
            cond_sub = (df['pds'] - df['pd_diff_1'] == 1) & (df['dt_diff'] == 1) & cond_match
            df.loc[cond_sub, ['value', 'pds']] = pd.DataFrame({"value": df.loc[cond_sub,'value'] - df.loc[cond_sub,'prev_val'],
                                                            "pds": df.loc[cond_sub,'pds'] - df.loc[cond_sub,'pd_diff_1']})

            #set 3 qtr values to their closest average
            df.loc[:,'prev_val'] = df['value'].shift(1)
            df.loc[:,'prev_val_1'] = df['prev_val'].shift(1)
            df.loc[:,'dt_diff_1'] = df['dt_diff'].shift(1)
            cond_3a = (df['pds'] == 3) & (df['dt_diff'] == 1) & (df['dt_diff_1'] == 1) & cond_match
            df.loc[cond_3a, ['value', 'pds']] = df.loc[cond_3a,'value'] - df.loc[cond_3a,'prev_val'] - df.loc[cond_3a,'prev_val_1'], 1

            cond_3b = (((df['pds'] == 4) & (df['dt_diff'] == 3)) |
                        ((df['pds'] == 3) & (df['dt_diff'].isin([1,2])))) & cond_match
            df.loc[cond_3b, ['value', 'pds']] = (df.loc[cond_3b,'value'] - df.loc[cond_3b,'prev_val']) / (df.loc[cond_3b,'pds'] - 1), 1

            #Pds == 4 value subtraction
            df.loc[:,'prev_val_2'] = df['prev_val_1'].shift(1)
            df.loc[:,'dt_diff_2'] = df['dt_diff_1'].shift(1)
            cond_4a = (df['pds'] == 4) & (df['dt_diff'] == 1) & (df['dt_diff_1'] == 1) & (df['dt_diff_2'] == 1) & cond_match
            df.loc[cond_4a, ['value', 'pds']] = df.loc[cond_4a,'value'] - df.loc[cond_4a,'prev_val'] - df.loc[cond_4a,'prev_val_1'] -  df.loc[cond_4a,'prev_val_2'], 1
            cond_4b = (df['pds'] == 4) & (df['dt_diff'] + df['dt_diff_1'] <= 3) & cond_match
            df.loc[cond_4b, ['value', 'pds']] = (df.loc[cond_4b,'value'] - df.loc[cond_4b,'prev_val'] - df.loc[cond_4b,'prev_val_1']) / 2 , 1

            #qtd compare
            cond_qtr = (df['pds'] == 1) & (df['dt_diff'] == 1) & cond_match
            df.loc[cond_qtr, 'qtr_diff_pct'] = ((df.loc[cond_qtr,'value'] - df.loc[cond_qtr,'prev_val'] ) / abs(df.loc[cond_qtr,'prev_val'])) * 100

            #ytd compare
            df.loc[:,'prev_val'] = df['value'].shift(1)
            df.loc[:,'prev_val_1'] = df['prev_val'].shift(1)
            df.loc[:,'prev_val_2'] = df['prev_val_1'].shift(1)
            df.loc[:,'prev_val_3'] = df['prev_val_2'].shift(1)
            df.loc[:,'dt_diff_3'] = df['dt_diff_2'].shift(1)

            c_yr1 = (df['dt_diff'] == 4) & cond_match
            c_yr2 = (df['dt_diff'] + df['dt_diff_1'] == 4) & cond_match
            c_yr3 = (df['dt_diff'] + df['dt_diff_1'] + df['dt_diff_2']== 4) & cond_match
            c_yr4 = (df['dt_diff'] + df['dt_diff_1'] + df['dt_diff_2'] + df['dt_diff_3'] == 4) & cond_match
            c_yr5 = (df['attr'].isin(attrs)) & (df['dt_diff'] == 4) & (df['attr'] == df['nxt_attr'])

            for cond, val in [(c_yr1, 'prev_val'),(c_yr2, 'prev_val_1'),(c_yr3, 'prev_val_2'),(c_yr4, 'prev_val_3'),(c_yr5, 'prev_val')]:
                df.loc[cond, 'yr_diff_pct'] = ((df.loc[cond, 'value'] - df.loc[cond, val]) / abs(df.loc[cond, val])) * 100

            out_attr = ['cik','ticker','date','acct_typ','attr','value','qtr_diff_pct','yr_diff_pct']
            
            # Try to delete the directory, if it doesnt exist, pass
            df[out_attr].to_csv(self.agg_data.joinpath(comp_file.name),
                                compression = 'gzip',
                                mode = 'w',
                                sep='\t',
                                index = False,
                                encoding='utf-8',
                                line_terminator = '\n')

    def getDatesAndPeriods(self, tag_list, prefix):
        """ 
        Goes through the tag list to find the period/range of the file and extract the appropriate date tags and end date
        ...
        Parameters
        ----------
        tag_list: a list of the the tags in the document
        prefix: formats for the different possible HTML tags
        ...
        Returns
        ----------
         > a dictionary of the dates and contexts
        """
        contexts, final_dts = {'instant': {}, 'period':{}}, {}
        max_inst_len, max_pd_len = 100, 100

        for tag in tag_list:
            if tag.name == prefix + 'context':
                #find the start date of context
                start_date_tag = tag.find(name= prefix + 'startdate')
                if start_date_tag != None:
                    try:
                        strt_dt = datetime.strptime(start_date_tag.text.replace('\n',''),"%Y-%m-%d")
                    except:
                        strt_dt = datetime.strptime(start_date_tag.text.replace('\n','').split('T')[0],"%Y-%m-%d")
                #get the end date as well as the quarter length factor
                end_date_tag = tag.find(name = prefix + 'enddate')
                if end_date_tag != None:
                    end_date = end_date_tag.text.replace('\n','')
                    try:
                        end_dt = datetime.strptime(end_date,"%Y-%m-%d")
                    except:
                        end_dt = datetime.strptime(end_date.split('T')[0],"%Y-%m-%d")

                    qtrs = int(round(((end_dt - strt_dt).days / 365.25) * 4))
                    contexts['period'][tag['id']] = (qtrs, end_date)
                    if len(tag['id']) < max_pd_len:
                        max_pd_len = len(tag['id'])
                
                #if the tag is an instant, archive the end date
                instant_date_tag = tag.find(name= prefix + 'instant')
                if instant_date_tag != None:
                    end_date = instant_date_tag.text
                    contexts['instant'][tag['id']] = (1, end_date)
                    if len(tag['id']) < max_inst_len:
                        max_inst_len = len(tag['id'])

        for type, max_val in [('instant',max_inst_len), ('period', max_pd_len)]:
            if max_val*1.5 < 12:
                limit = 12
            else:
                limit = max_val*1.5

            tmp_dict = {k:v for k ,v in contexts[type].items() if len(k) < limit and v[0] != 0}
            final_dts = {**tmp_dict, **final_dts}

        return final_dts

    def getValuesFromFile(self, tag_list):
        """ 
        Returns all quarterly values for each of the gaap attributes in the filing.  Goes through and finds the most accurate QTD value for each filing aspect. Only uses the basic values that were outlined by the dates highlighted in the getDatesAndPeriods function.  Other attributes are more specific than we desire at this time
        ...
        Parameters
        ----------
        tag_list: A list of tags from the filing sheet
        ...
        Returns
        ----------
        > a dict with the format (attribute_name, filing_end_date): quarterly_value
        """
        filing_attrs = {}
        for tag in tag_list:
            if ':' in tag.name and 'xbrl' not in tag.name and 'link:' not in tag.name:
                try:
                    tag_dt = tag['contextref']
                except:
                    continue

                if tag_dt in self.dates.keys():
                    try:
                        tag_val = float(tag.text)
                    except:
                        """
                        Skip the textblocks for the time being
                        """
                        continue
                        #tag_text = re.sub("<table.*/table>","",tag.text)
                        #tag_val = re.sub(re.compile('<.*?>'),'' ,tag_text).replace('\n', '').replace('\t','').replace('&#xA0;','')
                    #find the minimum for each date (QTD vs YTD) and find the factor
                    pd_div, pd_end_dt = self.dates[tag_dt]
                    typ, attr = tag.name.split(':')
                    if isinstance(tag_val, float):
                        if (typ, attr, pd_end_dt) not in filing_attrs.keys() or filing_attrs[(typ, attr, pd_end_dt)][0] > pd_div:
                             filing_attrs[(typ, attr,pd_end_dt)] = (pd_div, tag_val)
                    else:
                        filing_attrs[(typ, attr, pd_end_dt)] = (0, tag_val)
        #format the object divisors and insert each date value into a nested dictionary
        return pd.DataFrame([list(k) + list(v) for k,v in filing_attrs.items()],
                            columns = ['acct_typ', 'attr', 'date', 'pds', 'value'])
