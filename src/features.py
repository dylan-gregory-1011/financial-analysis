##!/usr/bin/env python
"""
Financial Analysis project.  This file drives the creation of financial features to be used in the modeling portion of the analysis.

    def run_delta() -> Function that transfers the newly downloaded financial information from the raspberrypi and updates the files
    def build_sec_features() -> Takes the newly downloaded quarterly reports and calculates the associated financial figures
    def output_data_for_dashboard() -> Outputs the data to be used by the Tableau reports

"""
#Imports
from pathlib import Path
from data import SQLCommands
import sys
import numpy as np
from features import SECFeaturizer, TextFeatures, MongoClient
import pandas as pd
from datetime import datetime, date
import json
import pymongo
import re
import sqlite3 as db
import logging
import pantab
from tableauhyperapi import TableName

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2019 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

#constants and logging
PROJ = Path(__file__).resolve().parent.parent
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logger = logging.getLogger(__name__)
RAW_DATA = PROJ.joinpath('data', 'raw')
PROC_DATA = PROJ.joinpath('data','processed')
SQL_MSTR = SQLCommands(db_conn = db.connect(str(PROC_DATA.joinpath('master.db'))))
SQL_EXT = SQLCommands(db_conn = db.connect(str(PROC_DATA.joinpath('external_data.db'))))
SQL_SEC = SQLCommands(db_conn = db.connect(str(PROC_DATA.joinpath('sec.db'))))
SQL_EQUITIES = SQLCommands(db_conn = db.connect(str(PROC_DATA.joinpath('equities.db'))))
SQL_ANALYSIS = SQLCommands(db_conn = db.connect(str(PROC_DATA.joinpath('analysis.db'))))
CURR_DT = datetime.today().strftime('%Y-%m-%d')
MONGO = pymongo.MongoClient("mongodb://127.0.0.1:27017")['finance']

def run_delta():
    """ 
    Function that runs the delta data into the Historical data so that the entire dataset doesnt have to be transferred each time
    """
    logging.basicConfig(level=logging.INFO, format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt= '%m-%d %H:%M', filename=PROJ.joinpath('logs',CURR_DT + '_delta.log'), filemode = 'w')
    logger.info('Starting Delta Transfer For Equities')
    SQL_DELTA = SQLCommands(db_conn = db.connect(str(PROC_DATA.joinpath('delta.db'))))
    df = SQL_DELTA.readSQL("SELECT * FROM dly_stock_hist")
    SQL_EQUITIES.executeSQL("INSERT OR IGNORE INTO dly_stock_hist VALUES (%s)", df)
    logger.info('Finished Delta for Equities')
    logger.info('Starting Delta for SEC Filings')
    df = SQL_DELTA.readSQL("SELECT * FROM sec_filings")
    ciks = list(set(df['cik'].unique()))
    SQL_SEC.executeSQL("DELETE FROM sec_filings where cik in (%s)" % ",".join([str(v) for v in ciks]))
    SQL_SEC.executeSQL("INSERT INTO sec_filings VALUES (%s)", df)
    logger.info('Finished Delta for SEC Filings')

def build_features():
    """ 
    Function that builds the SEC Features Table and calculates the TTM factors for all the values
    """
    logging.basicConfig(level=logging.INFO, format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt= '%m-%d %H:%M', filename=PROJ.joinpath('logs',CURR_DT + '_sec_features.log'), filemode = 'w')

    with open(PROJ.joinpath('docs', 'features.json'), 'r') as f:
        features = json.load(f)

    sec = SECFeaturizer(sql_sec= SQL_SEC, sql_analysis= SQL_ANALYSIS, sql_master = SQL_MSTR
                         ,conn= db.connect(str(PROC_DATA.joinpath('equities.db'))))
    logging.info("Starting SEC Feature Formatting")
    sec.stgFeaturesForTransform(features)
    logging.info("Calculating TTM and reformatting values")
    sec.calcTTM()
    logging.info("Finalizing feature list for Fundamentals")
    sec.buildFinalFundamentals()
    logging.info("Finished building fundamentals for Data")
    sec.buildFinalEquityFeatures()
    logging.info("Finished building equity datasets for each industry.  Now comparing across industry.")
    sec.calculateIndustryAveragesAndRankings()

def output_data_for_dashboard():
    """ 
    Function that runs output data to a hyper files
    """
    files = list(x for x in PROJ.joinpath('reports').iterdir() if x.is_file())
    for file in files:
        file.unlink()

    # insert the equity data hyper table
    for file in list(x for x in PROC_DATA.joinpath('equities_final').iterdir() if x.is_file()):
        sector = file.name.split('.')[0]
        if file.name.split('.')[1] == 'json' or sector == 'Industry_Averages':
            continue
        df = pd.read_parquet(file, engine='pyarrow')
        df = df.iloc[:, np.r_[:7,57:64]]
        df.replace(to_replace = [np.inf,np.nan,np.NINF],value = None, inplace = True)
        pantab.frame_to_hyper(df, PROJ.joinpath('reports','equities.hyper'), table = 'daily_prices', table_mode = "a")

    # Get fundamental data and insert the group into the data
    with open(PROC_DATA.joinpath('equities_final', 'Current_Rankings.json'), 'r') as f:
        rankings = json.load(f)

    vals = []
    for key in rankings.keys():
        for tick in rankings[key].keys():
            if tick == 'averages':
                continue
            for attr in rankings[key][tick]:
                row = [tick, attr, rankings[key][tick][attr]['curr_value'], rankings[key][tick][attr]['industry_percentile']]
                vals.append(row)

    df_curr = pd.DataFrame(vals, columns = ['ticker','attribute','curr_value','industry_percentile'])
    df_curr.replace(to_replace = [np.inf,np.nan,np.NINF], value = None, inplace = True)
    pantab.frame_to_hyper(df_curr, PROJ.joinpath('reports','rankings.hyper'), table = "curr_rank", table_mode = "a")

    # Get fundamental data and insert the group into the data
    with open(PROJ.joinpath('docs', 'fundamental_hier.json'), 'r') as f:
        fundamentals = json.load(f)

    df_ = SQL_ANALYSIS.readSQL("SELECT * FROM fundamentals")
    df_.replace(to_replace = [np.inf,np.nan,np.NINF],value = None, inplace = True)
    df_ = pd.melt(df_, id_vars=['ticker','date'], value_name= 'attribute_value', var_name = 'attribute')
    val = df_['attribute'].apply(lambda x: ''.join([grp if x in attr else '' for grp, attr in fundamentals.items()]))
    df_.insert(2,'attribute_grp',val)
    df_['date'] = pd.to_datetime(df_['date'])
    df_['date'] = df_['date'].apply(lambda dt: dt -  pd.Timedelta(5, unit='D') if dt.day < 3 else dt)
    pantab.frame_to_hyper(df_[df_['attribute_grp'] != ''], PROJ.joinpath('reports','fundamentals.hyper'), table = "fundamentals", table_mode = "a")

    # Get master data to join with the final report
    dfm = SQL_MSTR.readSQL("SELECT * FROM stocks_master")
    pantab.frame_to_hyper(dfm, PROJ.joinpath('reports','master.hyper'), table = "stocks_master", table_mode = "a")

def updateMongoAndCleanseText():
    """ 
    Function that updates news data and risk factors in the mongo db database
    """
    logging.basicConfig(level=logging.INFO, format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt= '%m-%d %H:%M:%S', filename=PROJ.joinpath('logs',CURR_DT + '_text_upload.log'), filemode = 'w')
    logger.info("Instantiating MongoDB Connection")
    mongo = MongoClient(MONGO, SQL_MSTR)
    logging.info(" Starting Download for Risk Factors ")
    #mongo.insertData(RAW_DATA.joinpath('riskfactors'))
    logging.info(" Finished Download for Risk Factors.  Starting upload of News articles ")
    #mongo.insertNewsData(RAW_DATA.joinpath('news'))
    logging.info(" Finished Mongodb Upload ")

    logging.info(" Run Risk Factor Analysis ")
    rf_obj = TextFeatures(MONGO)
    rf_obj.featurizeRiskFactors(PROC_DATA)

def main(feat_to_run = None):
    if feat_to_run == 'delta':
        run_delta()
    elif feat_to_run == 'featurize':
        build_features()
    elif feat_to_run == 'write_output':
        output_data_for_dashboard()
    elif feat_to_run == 'text':
        updateMongoAndCleanseText()

if __name__ == '__main__':
    #import the process to run
    main(feat_to_run = sys.argv[1])
