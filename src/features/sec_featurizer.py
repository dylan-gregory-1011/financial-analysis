##!/usr/bin/env python
"""
Data Featurizer for the financial analysis dataset. This function goes through the sec features that have been downloaded
from EDGAR and creates calculated key figures from the values that are going to be used in our analytical models.

    def stgFeaturesForTransform() -> Takes a lookup dictionary with all of the known alias' for each companies' reported attribute
                                    and massages all the data into one field
    def calcTTM() -> Calculate the twelve trailing month aggregations for the specific fields
    def buildFinalFundamentals() -> using the fundamentals reported by each company, build commonly used calculations for each company for each quarter.
    def buildFinalEquityFeatures() -> Blend the equity data with the fundamental data for calculations that take stock close into account
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from scipy import stats
import json

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2018 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

PROJ = Path(__file__).resolve().parent.parent.parent
EQUITY_PROJ = PROJ.joinpath('data','processed','equities_final')
REPL_NEG = -1000

class SECFeaturizer(object):
    def __init__(self, sql_sec, sql_analysis, sql_master, conn):
        """ An object that creates features off of data provided by the connection
            ::param db_connection: A database connection to the object that you hope to featurize
        """
        self.sql_sec = sql_sec
        self.sql_analysis = sql_analysis
        self.conn = conn
        self.sql_master = sql_master
        self.logger = logging.getLogger('sec.Features')

    def stgFeaturesForTransform(self, features):
        """ This feature utilizes the features.json file to calculate the different attributes based upon the different
            gaap line items that we have found.
            ::param features: a dictionary of values where the keys are the aggregated values and the values are the different
                             gaap values
        """
        f1 = [f for feat in features.values() for f in feat if isinstance(f,str)]
        f2 = [f for feat in features.values() for f2 in feat if isinstance(f2,list) for f in f2]
        df_raw = self.sql_sec.readSQL("SELECT ticker, attribute, date, value FROM sec_filings where attribute in (\'%s\') and ticker != \'NONE\'" % "\',\'".join(f1 + f2))

        #pivot table and associate all values correctly
        df = df_raw.pivot_table(index = ['ticker', 'date'], columns = 'attribute', values = 'value').reset_index()

        #build the features based on the file in ~/docs/features.json
        for key, value in features.items():
            if key == "dividend":
                #use the first term in the array, or else use the second phrase
                df[key] = df[value[2]].fillna(df[value[0]] / df["shares"]).fillna(df[value[1]] / df["shares"])
            else:
                df[key] = df[value[0]]
                for ix in range(1,len(value)):
                    new_fld = value[ix]
                    if isinstance(new_fld, str):
                        df[key].fillna(df[new_fld], inplace = True)
                    else:
                        df[key].fillna(df[new_fld].sum(axis = 1), inplace = True)
                        df[key].replace(0,None)

        key_calcs = [key for key in features.keys()]
        key_calcs.sort()
        final_columns = ['ticker', 'date'] + key_calcs
        #create a temp file with the calculated fields
        self.sql_sec.executeSQL('CREATE TABLE IF NOT EXISTS sec_filings_fmt (ticker TEXT, date TEXT, %s)' % ', '.join(['%s BIGINT ' % key for key in key_calcs]))
        self.sql_sec.executeSQL("DELETE FROM sec_filings_fmt")
        self.sql_sec.executeSQL("INSERT INTO sec_filings_fmt VALUES (%s)" , df[final_columns])

    def calcTTM(self):
        """ The function calculates the Twelve Trailing Month values for all of the attributes.  These values will be used to
            calculate different financial attributes
        """
        df_sec_fmt = self.sql_sec.readSQL("SELECT * FROM sec_filings_fmt")
        df_sec_fmt.sort_values(by= ['ticker', 'date'], inplace = True)
        df_sec_fmt['date'] = df_sec_fmt['date'].str.strip('\n').str.split('T').str[0]
        #create the previous date fields to calculate change and fill in share prices
        df_sec_fmt.reset_index(inplace = True)
        df_sec_fmt.fillna(0, inplace = True)
        df_sec_fmt['sums'] = (df_sec_fmt != 0).sum(1)
        df_sec_fmt['prev_dt'] = df_sec_fmt['date'].shift(1)
        df_sec_fmt['prev_dt_diff'] = ((pd.to_datetime(df_sec_fmt['date'],infer_datetime_format=True) - pd.to_datetime(df_sec_fmt['prev_dt'],infer_datetime_format=True))/np.timedelta64(1, 'M')).fillna(0).astype(int)

        ix_remove = []
        #iterate through the dataset and sum up the previous 3 quarterly values for each company
        for ix, row in df_sec_fmt.iterrows():
            if ix < 3:
                continue
            if abs(row['prev_dt_diff']) <= 1:
                #this is to make sure that the right value is overwritten
                prev_row = df_sec_fmt.loc[ix - 1,:]
                if row['sums'] > prev_row['sums']:
                    if (prev_row['shares'] + 0.1) / (row['shares'] + 0.1) > 0.5:
                         df_sec_fmt.loc[ix,'shares'] = prev_row['shares']
                    ix_remove.append(ix - 1)
                elif row['sums'] < prev_row['sums']:
                    if (row['shares'] + 0.1) / (prev_row['shares'] + 0.1) > 0.5:
                        df_sec_fmt.loc[ix - 1,'shares'] = row['shares']
                    ix_remove.append(ix)
                elif row['sums'] == prev_row['sums']:
                    if round((prev_row['shares'] + 0.1) / (row['shares'] + 0.1)) > 0.5:
                         df_sec_fmt.loc[ix,'shares'] = prev_row['shares']
                         ix_remove.append(ix - 1)
                    if round((row['shares'] + 0.1) / (prev_row['shares'] + 0.1)) > 0.5:
                        df_sec_fmt.loc[ix - 1,'shares'] = prev_row['shares']
                        ix_remove.append(ix)

        df_sec_fmt.drop(ix_remove, inplace = True)
        df_sec_fmt.reset_index(inplace = True)
        df_sec_fmt['prev_yr'] = df_sec_fmt['date'].shift(3)
        df_sec_fmt['prev_yr_diff'] = round(((pd.to_datetime(df_sec_fmt['date'],infer_datetime_format=True) - pd.to_datetime(df_sec_fmt['prev_yr'],infer_datetime_format=True))/np.timedelta64(1, 'M'))).fillna(0).astype(int)

        for ix, row in df_sec_fmt.iterrows():
            if ix < 3:
                continue
            if df_sec_fmt.loc[ix - 3, 'ticker'] == row['ticker'] and row['prev_yr_diff'] == 9:
                for attr in ['costofrevenue', 'cogs', 'taxes', 'netincome','operatingincome','eps', 'depreciationamortization','revenue']:
                    df_sec_fmt.loc[ix, '%s_ttm' % attr] = sum(df_sec_fmt.loc[ix-3:ix, attr])

        df_sec_fmt.drop(columns = ['index','level_0', 'sums','prev_dt','prev_dt_diff','prev_yr','prev_yr_diff'], inplace = True)
        self.sql_sec.executeSQL('CREATE TABLE IF NOT EXISTS sec_filings_ttm (ticker TEXT, date TEXT, %s)' % ', '.join(['%s DECIMAL(16,2) ' % key for key in list(df_sec_fmt)[2:]]))
        self.sql_sec.executeSQL("DELETE FROM sec_filings_ttm")
        self.sql_sec.executeSQL("INSERT INTO sec_filings_ttm VALUES (%s)" , df_sec_fmt)

    def buildFinalFundamentals(self):
        """ Using the correctly calculated TTM, This function blends together the SEC values with the closest equities close
            to calculate a wide range of fiancial key figures
        """
        df_ = self.sql_sec.readSQL("SELECT * FROM sec_filings_ttm")

        df_.fillna(0, inplace = True)
        df_['shareholderequity'] = df_['assets'] - df_['liabilities']
        df_['ebit'] = df_['taxes'] + df_['netincome']
        df_['ebit_ttm'] = df_['taxes_ttm'] + df_['netincome_ttm']
        df_['ebitda'] = df_['ebit']  + df_['depreciationamortization']
        df_['ebitda_ttm'] = df_['ebit_ttm']  + df_['depreciationamortization_ttm']
        df_['taxrate'] = df_['taxes'] / df_['ebit']
        df_['nopat'] = df_['ebit'] * (1 - df_['taxrate'])
        df_['investedcapital'] = df_['debtlongterm'] + df_['debtshortterm'] + df_['minorityInterest'] + df_['shareholderequity'] - df_['cash']
        df_['roce'] = df_['nopat'] / df_['investedcapital']
        df_['roe'] = df_['netincome_ttm'] / df_['shareholderequity']
        df_['roa'] = df_['netincome_ttm'] / df_['assets']
        df_['eps'] = df_['netincome_ttm'] / df_['shares']
        df_['debttoequity'] = (df_['debtlongterm'] + df_['debtshortterm']) / df_['shareholderequity']
        df_['bps'] = (df_['shareholderequity'] - df_['preferredstock']) / df_['shares']
        df_['operatingmargin'] =  df_['operatingincome_ttm'] / df_['revenue_ttm']
        df_['grossprofit'] = df_['revenue'] - df_['cogs']
        df_['debttoequity'] = df_['debtlongterm'] / df_['shareholderequity']
        df_['grossprofit_ttm'] = df_['revenue_ttm'] - df_['cogs_ttm']
        df_['grossmargin_ttm'] = df_['grossprofit_ttm'] / df_['revenue_ttm']
        df_['inventoryturnover'] = df_['cogs'] / df_['inventory']
        for col in ['netincome', 'grossprofit', 'operatingmargin', 'ebitda']:
            df_['col_prev'] = df_.groupby(['ticker'])[col].shift(4)
            df_['%s_grwth_rt' % col] = (df_[col] - df_['col_prev']) / df_['col_prev'] * 100
            df_.drop(['col_prev'], axis = 1, inplace = True)

        df_.replace(to_replace= [np.inf,np.nan,np.NINF],value= None, inplace = True)
        #write data to the final table
        self.sql_analysis.executeSQL("drop table fundamentals")
        self.sql_analysis.executeSQL("CREATE TABLE IF NOT EXISTS fundamentals (ticker TEXT, date TEXT, %s)" % ', '.join(['%s DECIMAL(16,2) ' % x for x in df_.columns[2:]]))
        self.sql_analysis.executeSQL("INSERT INTO fundamentals values (%s)", df_)

    def buildFinalEquityFeatures(self):
        """ Build market value and other features and assign the values to the
        """
        # Delete all previously existing files
        files = list(x for x in EQUITY_PROJ.iterdir() if x.is_file())
        for file in files:
            file.unlink()

        #download all fundamental data as well as the near_dt lookup
        df_ = self.sql_analysis.readSQL("SELECT * FROM fundamentals order by ticker, date")

        df_sec_dts = df_.drop_duplicates(subset = ['date'])[['date', 'ticker']]
        all_dts = pd.read_sql("SELECT distinct date FROM dly_stock_hist", self.conn)['date'].tolist()

        #rename the column in the lookup table and merge it with the fundamentals table
        df_sec_dts['near_dt'] = df_sec_dts['date'].apply(self.nearestDt,args = ([x for x in all_dts if x > '2008-12-20'],))
        df_ = df_.merge(df_sec_dts[['date','near_dt']], how = 'left', on = ['date'])
        df_.drop(columns = 'date', inplace = True)
        df_.rename(columns = {'near_dt':'date'} , inplace = True)

        chunks = pd.read_sql("SELECT * FROM dly_stock_hist order by ticker, date", self.conn, chunksize = 50000)
        df_mstr = self.sql_master.readSQL("SELECT ticker, sector from stocks_master where industry != \'Index\'")
        df_cont = pd.DataFrame()

        parq_objs = {}
        for df_equity in chunks:
            df_e = pd.merge(df_equity, df_, how = 'left', on = ['date','ticker'])
            df_e = pd.concat([df_cont,df_e])
            df_e.sort_values(by = ['ticker','date'], inplace = True)
            for col in ['open','high','low','close','volume']:
                df_e[col] = df_e[col].astype('float')

            tickers = df_e['ticker']
            df_e = df_e.groupby('ticker').fillna(method='bfill')
            df_e.insert(0,'ticker', tickers)
            df_e = df_e.groupby('ticker').fillna(method='ffill')
            df_e.insert(0,'ticker', tickers)

            tickers = df_e['ticker'].unique().tolist()
            lst_ticker = tickers[len(tickers) - 1]

            df_e['pb'] = df_e['close'] / df_e['bps']
            df_e['pe'] = df_e['close'] / df_e['eps_ttm']
            df_e['peg'] = df_e['pe'] / df_e['netincome_grwth_rt']
            df_e['marketcap'] = df_e['close'] * df_e['shares']
            df_e['dividendyield'] = df_e['close'] / df_e['dividend']
            df_e['ev'] = df_e['marketcap'] + df_e['debtlongterm'] + df_e['debtshortterm'] + df_e['minorityInterest'] - df_e['cash']
            df_e['em'] = df_e['ev'] / df_e['ebitda']

            #move onto next table
            df_cont = df_e[df_e['ticker'] == lst_ticker]
            df_out = df_e[df_e['ticker'] != lst_ticker]

            df_out.replace(to_replace= [np.inf,np.nan,np.NINF],value= None, inplace = True)
            tickers = df_out['ticker'].unique().tolist()
            df_ms = df_mstr[df_mstr['ticker'].isin(tickers)]
            #for feature in ['close']:
            #    for mv in [5,21,50,100,200]:
            #        df["{0}_{1}_Mvng_Avg".format(feature, mv)] = df.groupby('ticker')[feature].transform(lambda x: x.rolling(window=mv).mean())
            #    for mv in [1,5,21,63,253]:
            #        df["{0}_{1}_PctChg".format(feature, mv)] = df.groupby('ticker')[feature].transform(lambda x: x.pct_change(periods=mv) * 100)

            for sector in df_ms['sector'].unique().tolist():
                if sector == 'n/a':
                    sector_out = 'No_Sector'
                else:
                    sector_out = sector.replace(' ', '_')
                table = pa.Table.from_pandas(df_out[df_out['ticker'].isin(df_ms[df_ms['sector'] == sector]['ticker'].tolist())]
                                            , preserve_index=False)
                file = EQUITY_PROJ.joinpath('%s.parquet.snappy' % sector_out)

                if not file.exists():
                    parq_objs[sector_out] = pq.ParquetWriter(file, table.schema, compression = 'snappy')

                parq_objs[sector_out].write_table(table)

    def calculateIndustryAveragesAndRankings(self):
        """ Function that compares calculates the median value across each industry. (For each day)
            Write the industry averages into a parquet file to be used accross each value. Iterate through each company
            for the most current date to find the averages for each ratio as well as the display the most up to date data
        """
        out_vals = {}
        for file in list(x for x in EQUITY_PROJ.iterdir() if x.is_file()):
            df = pd.read_parquet(file, engine='pyarrow')
            df_out = df[['date']].drop_duplicates()
            df_out.sort_values(by = ['date'], inplace = True)
            industry = file.name.split('.')[0]
            df_out['industry'] = industry
            out_vals[industry] = {"averages": {}}

            df_mx = df[df['date'] == max(df['date'])]
            first = True
            for attribute in list(df)[7:]:
                #merge the output dataframe with the new median average value
                df_out = pd.merge(df_out, df[~df[attribute].isnull()][['date',attribute]].groupby('date').median(),
                            how = 'left', on = 'date')
                #include the average for the most recent day in the output file
                out_vals[industry]["averages"][attribute] = float("{:.2f}".format(df_mx[df_mx[attribute] > 0.0][attribute].median()))

                #input the value into the dictionary and calculate the percentile for each company
                list_vals = df_mx[~df_mx[attribute].isnull()][attribute].tolist()
                for tick in df_mx['ticker'].unique().tolist():
                    comp_val = df_mx.loc[df_mx['ticker'] == tick, attribute].values[0]
                    if comp_val is None:
                        prct = 0
                    else:
                        comp_val = float("{:.2f}".format(comp_val))
                        prct = float("{:.2f}".format(stats.percentileofscore(list_vals, comp_val)))

                    new_updt = {"curr_value" : comp_val, "industry_percentile": prct}
                    if first:
                        out_vals[industry][tick] = {attribute: new_updt}
                    else:
                        out_vals[industry][tick][attribute] = new_updt
                first = False

            #replace the null values with 0's.  Prepare the dataframe for writing to a parquet and update the industry average
            df_out.replace([np.inf,np.nan,np.NINF], None, inplace = True)
            table = pa.Table.from_pandas(df_out, preserve_index=False)
            file = EQUITY_PROJ.joinpath('Industry_Averages.parquet.snappy')
            if not file.exists():
                pq_w = pq.ParquetWriter(file, table.schema, compression = 'snappy')

            pq_w.write_table(table)

        with open(EQUITY_PROJ.joinpath('Current_Rankings.json'), 'w') as fp:
            json.dump(out_vals, fp)

    def nearestDt(self, dt, all_dates):
        """ Returns the nearest date to the supplied date
            ::param dt: the date to be compared
            ::param all_dates: the list of dates to compare to

            return: returns the closest date to the time period
        """
        return min(all_dates, key=lambda x :abs(datetime.strptime(x.strip('\n'),'%Y-%m-%d') - datetime.strptime(dt.strip('\n'),'%Y-%m-%d')))
