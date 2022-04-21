##!/usr/bin/env python
"""
API Function that is used to call each API and handle the responses appropriately.
"""

#Imports
import requests
import time

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2019 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

def api_call(url, headers, return_typ):
    """ 
    The function that calls the api for both the history and fundamental information
    ...
    Parameters
    ----------
    url: The url to be called for the api
    ...
    Returns
    -----------
      > The text from an api call
    """
    while True:
        try: #iterate through and if a timeout exception is reached, try again
            api_call = requests.get(url, headers= headers, timeout=(10, 30))
            break
        except requests.exceptions.Timeout:
            pass
        except requests.exceptions.ConnectionError:
            pass

    while api_call.status_code != 200:
        if api_call.status_code == 400:
            return 'Bad Call'
        if api_call.status_code == 404:
            #print("Error webpage not found")
            return None
        elif api_call.status_code == 429:
            print('RateLimit')
            time.sleep(20)
        #recall the api request
        time.sleep(2)
        api_call = requests.get(url, headers= headers, timeout=(10, 30))
    if return_typ == 'json':
        return api_call.json()
    elif return_typ== 'text':
        return api_call.text
    else:
        return api_call
