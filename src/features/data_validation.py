##!/usr/bin/env python
"""
Tool to expidite the Data Validation process

"""

import pandas as pd
from datetime import datetime
import logging
import numpy as np

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2018 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

class DataValidation(object):
    def __init__(self, sql_sec, sql_equities, sql_analysis):
        """ An object that creates features off of data provided by the connection
            ::param db_connection: A database connection to the object that you hope to featurize
        """
        self.sql_sec = sql_sec
        self.sql_analysis = sql_analysis
        self.sql_equities = sql_equities
        self.logger = logging.getLogger('features.validation')

    def findIrregularities(self):
        """ Function that looks through the sec_fmt data for missing data or data that

        """
        #Iterate through the different fields
            # Get the first date with any data in any field and create a lookup table

        #iterate through the different fields
            #Go through the fields and isolate rows after that data where there is a 0 in the field
            #find the average of the two closest values


    def outcomePossibilitie(self):
        """ Based Upon the table above, look through the sec_filings table and output potential fillings that could fit the
            values that are missing

        """
        #come up with a lookup table for each of the values that has the base phrase to lookup
        #iterate over these phrases
            #download the entire table of values from sec_filings that match these terms
            #join this table to the values that are missing from the function that are mentioned above.
            #output this to a table (and use this to lookup the term)

        #for each value, try to find the attribute that matches closest to the value
