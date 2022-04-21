##!/usr/bin/env python
""" Company Metrics Featurizer
"""

import pandas as pd

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2020 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

class CalculateCompanyMetrics(object):

    def __init__(self, sql_analytics, sql_mstr):
        """ Mongo Client used to connect to the MongoDB
        """
        self.analytics = sql_analytics
        self.sql_mstr = sql_mstr

    def calcFairValue():
    """
    DCF ------------------------------
    if sector = 'Finance':
        twoStageDiscountedFlow()
    else:
        if not data:
            dividendDiscountModel()
        else:
            if sub_sector in ['Insurance','Bank']:
                excessReturnsMethod()
            else:
                affoDiscountedCashFlow()
    Relative Valuation ----------------------
    PE Ratio, PEG Ratio, PB Ratio


    """
