##!/usr/bin/env python
""" Financial Analysis Project:  This file downloads the data from SEC 10-Q and 10-K filings and builds a table
that allows for the specific attribute to be analyzed in isolation from the other attributes.

    def updateFilings() -> None: The driver function that downloads the SEC Filing data for a quarter
    def getRiskFactor() -> None: Write the risk factor to the raw file output
    def getFilingLinks() -> name, xml: returns an the links for the xbrl file as well as the risk factor
    def getLinkFromHTML() -> Finds the correct link from an HTML file (Updated in version 2 to use better text parsing)
    def _append_html_version() -> append the index portion of an HTML file
"""

#Imports
import requests
import os
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
import urllib
from datetime import datetime, timedelta
import re
import tempfile
import zipfile
import time
#from os.path import exists
import logging
from .api_call import api_call

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2019 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

HEADERS = {
    'User-Agent': 'dylans-app/0.0.1'
}

class SECFilingDownload(object):

    def __init__(self, sec_data):
        """ 
        An object that downlads sec filings from the website 
        ...
        Parameters
        ----------
        sec_data: The directory that holds the sec data
        """
        self.logger = logging.getLogger('sec.SECFilingDownload')
        self.raw_dir = sec_data.joinpath('raw-filings')
        self.ind_dir = sec_data.joinpath('index')
        self.logger.info(" Object Instantiated Succesfully")

    def updateFilings(self, year, qtr):
        """ 
        Function that downloads the SEC 10-K and 10-Q filings for the most recent period.  First downloads the most recent copy of the index file and then calculates the files that still need to be download based on previous downloads
        ...
        Parameters
        ----------
        year: The year to download
        qtr: the qtr to download
        """
        url, ind_file = ("https://www.sec.gov/Archives/edgar/full-index/%s/%s/master.zip") % (year, qtr), "%s-%s.tsv" % (year, qtr)
        qtr_dwnld = ind_file.split('.')[0].replace("-",'')
        self.out_dir = self.raw_dir.joinpath('filings',qtr_dwnld + '.zip')
        self.risk_files = set(os.listdir(self.raw_dir.joinpath('riskfactors')))
        out_files = []

        if self.out_dir.exists():
            df_ind = pd.read_csv(self.ind_dir.joinpath(ind_file), sep = '|', names = ['cik', 'nm','type','dt_submitted','text','url'])
            min_dt = max(df_ind['dt_submitted'])
        else:
            min_dt = '1900-01-01'
            with zipfile.ZipFile(self.out_dir, 'w') as z:
                pass

        self.logger.info('Min Date for this load is %s' % min_dt)
        with tempfile.TemporaryFile(mode='w+b') as tmp:
            # set up new request, add modified headers to the request
            req = urllib.request.Request(url, headers = HEADERS)
            content = urllib.request.urlopen(req).read()
            tmp.write(content)
            with zipfile.ZipFile(tmp).open("master.idx") as z_in:
                with open(self.ind_dir.joinpath(ind_file), 'w+') as idxfile:
                    for _ in range(11):
                        next(z_in)
                    lines = z_in.read()
                    lines = lines.decode('utf-8', errors= 'ignore')
                    lines = map(lambda line: self._append_html_version(line), lines.splitlines())
                    idxfile.write('\n'.join(lines))

        #read the index file into a dataframe and structure the data for 10-Q and 10-K
        df_ind = pd.read_csv(self.ind_dir.joinpath(ind_file), sep = '|', names = ['cik', 'nm','type','dt_submitted','text','url'])
        max_dt = datetime.strftime(datetime.today() - timedelta(days = 1), '%Y-%m-%d')

        df_qtrly = df_ind[((df_ind['type'] == '10-Q') | (df_ind['type'] == '10-K')) & (df_ind['dt_submitted'] > min_dt)  & (df_ind['dt_submitted'] <= max_dt)]
        df_final = df_qtrly[['cik','type','dt_submitted']].groupby(['cik', 'type'], sort = True)['dt_submitted']\
                                                .max().reset_index(name = 'dt_submitted')

        df_final = pd.merge(df_final, df_qtrly, how = 'inner', on = ['cik', 'type', 'dt_submitted'])\
                     .groupby(['cik','nm','type','dt_submitted'], sort = True)['url'] \
                     .max() \
                     .reset_index(name = 'url')

        #iterate through all the files and download the data
        count, total_records = 0, len(df_final['cik'])
        for index, row in df_final.iterrows():
            count+=1
            self.row = row
            try:
                time.sleep(0.15)
                xbrl_url, html_url = self.getFilingLinks()
                self.getRiskFactor(html_url)
                doc_text = api_call(xbrl_url, HEADERS, 'text')
                url_obj = xbrl_url.split('/')
                symbol = url_obj[len(url_obj) - 1].split('-')[0]
                file_name = '%s-%s_%s_%s.%s' % (self.row['cik'], symbol, self.row['type'], self.row['dt_submitted'], 'xml')

                out_files.append(file_name)
                with zipfile.ZipFile(self.out_dir,'a',zipfile.ZIP_DEFLATED) as z:
                    z.writestr(file_name,doc_text)
            except:
                pass
            self.logger.info(" %s Completed: %.2f Pct" % (row['cik'] ,float(count)/float(total_records)*100))
        return out_files

    def getRiskFactor(self, url):
        """ 
        Get the Risk Factor from each file and write it to the correct folder
        ...
        Paramaters
        ----------
        url: the html url from to analyze
        """
        #setup constants to be used in text parsing
        search_a, search_b, search_c, search_d, splt_txt_spec, out_txt = '', '', '', '', '', ''
        ix_a, ix_b, ix_c, ix_d, ix_prev, t1, countA = 0, 0, 0, 0, 0, 0, 0
        idx_found = -1
        brk_found = False
        #start parsing
        response = api_call(url, HEADERS, 'text')
        soup = BeautifulSoup(response, "lxml")
        for script in soup(["script", "style"]):
            script.extract()

        #break into lines and remove leading and trailing space on each
        text = soup.get_text('\n', strip = True)
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip().encode('ascii','ignore').decode('utf-8') for line in lines for phrase in line.split("  "))
        chunks  = [chunk for chunk in chunks if (chunk and chunk.upper() not in ['INDEX','TABLE OF CONTENTS','X','.'] and not chunk.isdigit() and not re.match( 'I-[0-9]*', chunk))]
        len_file = len(chunks)

        for idx, txt in enumerate(chunks):
            if idx == len_file - 2:
                break
            splt_txt = "".join(txt.upper().replace('.','').split())
            combo_txt = splt_txt + "".join(chunks[idx + 1].upper().replace('.','').split())
            combo_txt_2 = combo_txt + "".join(chunks[idx + 2].upper().replace('.','').split())

            if (search_a != '' and search_b != '') and idx not in [ix_a, ix_b] and countA > 1 and ((search_a in splt_txt and len(splt_txt) < 25 and 'BUSINESS' not in splt_txt)
                    or (search_b in splt_txt and len(splt_txt) < 25 and 'BUSINESS' not in splt_txt) or (search_a + search_b == splt_txt
                    or search_a.strip('ITEM') + search_b == splt_txt or 'ITEM' + search_a + search_b == splt_txt)):
                break

            if splt_txt in ['RISKFACTORS', 'ITEM1A', 'ITEM1ARISKFACTORS','RISKFACTOR','ARISKFACTORS'] and search_a == '':
                if 'RISKFACTOR' in "".join(chunks[idx + 1].upper().replace('.','').split()):
                    search_a, ix_a = "".join(chunks[idx + 2].upper().replace('.','').split()), idx + 2
                    search_b, ix_b = "".join(chunks[idx + 3].upper().replace('.','').split()) , idx + 3
                    search_c, ix_c = "".join(chunks[idx + 4].upper().replace('.','').split()), idx + 4
                    search_d, ix_d = "".join(chunks[idx + 5].upper().replace('.','').split()) , idx + 5
                    idx_found = idx + 1
                else:
                    search_a, ix_a = "".join(chunks[idx + 1].upper().replace('.','').split()), idx + 1
                    search_b, ix_b = "".join(chunks[idx + 2].upper().replace('.','').split()), idx + 2
                    search_c, ix_c = "".join(chunks[idx + 3].upper().replace('.','').split()), idx + 3
                    search_d, ix_d = "".join(chunks[idx + 4].upper().replace('.','').split()) , idx + 4
                    idx_found = idx

                #for weird formatting with the break statement
                if search_a == 'ITEM' and search_b in ['1B', '2','3','4','6']:
                    search_a = search_a + search_b
                    search_b = search_c

            if (('ITEM1A' in splt_txt or '1A' in splt_txt) and 'RISKFACTOR' in splt_txt and len(splt_txt) < 19) \
                    or ('RISKFACTOR' in combo_txt and '1A' in combo_txt and len(combo_txt) < 19) \
                    or ('RISKFACTOR' in combo_txt_2 and '1A' in combo_txt_2 and len(combo_txt_2) < 19):
                if idx != ix_prev + 1:
                    countA +=1
                ix_prev = idx

            if idx == idx_found and countA == 0:
                splt_txt_spec = splt_txt
                countA = 2
        ######################################################################################
        #Reformat for files that have weird formatting for values that have a header
        if countA > 5:
            countA = 2
        #if the items are on the same line, break apart
        if 'ITEM' in search_a and 'ITEM' in search_b and search_a != 'ITEM' and search_b != 'ITEM':
            if 'ITEM1B' in search_a and search_a != 'ITEM1B' :
                search_b = search_a[6:]
                search_a = search_a[:6]
            else:
                search_b = search_a[5:]
                search_a = search_a[:5]

        #if the break is not found, bad txt
        if not brk_found:
            search_b = search_b[:20]

        #if the table of contents has weird formatting, ensure break points are set
        if countA == 1:
            brk_found = False
            search_a = 'ITEM2'
            search_b = 'UNREGISTEREDSALESOF'
            search_c = 'ITEM1B'
            search_d = 'UNRESOLVEDSTAFF'
        elif countA == 0:
            return
        ######################################################################################
        ix_prev = 0
        for idx, txt in enumerate(chunks):
            if idx == len_file - 2:
                break
            splt_txt = "".join(txt.upper().replace('.','').split())
            combo_txt = splt_txt + "".join(chunks[idx + 1].upper().replace('.','').split())
            combo_txt_2 = combo_txt + "".join(chunks[idx + 2].upper().replace('.','').split())

            if t1 >= countA and ((search_a in splt_txt and len(splt_txt) < 30 and 'BUSINESS' not in splt_txt) or
                (search_b in splt_txt and len(splt_txt) < 30 and 'BUSINESS' not in splt_txt) or (search_a + search_b == splt_txt or search_a.strip('ITEM') + search_b == splt_txt
                or 'ITEM' + search_a + search_b == splt_txt) or search_a == splt_txt or search_b == splt_txt or (search_a + search_b in splt_txt )):
                break

            if not brk_found and t1 >= countA and ((search_c in splt_txt and len(splt_txt) < 30 and 'BUSINESS' not in splt_txt) or
                (search_d in splt_txt and len(splt_txt) < 30 and 'BUSINESS' not in splt_txt) or (search_c + search_d == splt_txt)
                or search_c == splt_txt or search_d == splt_txt or (search_c + search_d in splt_txt)):
                break

            if (('ITEM1A' in splt_txt or '1A' in splt_txt) and 'RISKFACTORS' in splt_txt and len(splt_txt) < 19) \
                    or ('RISKFACTORS' in combo_txt and '1A' in combo_txt and len(combo_txt) < 19) \
                    or ('RISKFACTOR' in combo_txt_2 and '1A' in combo_txt_2 and len(combo_txt_2) < 19) or (splt_txt_spec == splt_txt and splt_txt_spec != ''):
                if idx != ix_prev + 1:
                    t1 +=1
                    ix_prev = idx
                continue

            if t1 >= countA:
                out_txt = ' '.join([out_txt, txt])

        if len(out_txt) == 0:
            return

        rf_f = self.raw_dir.joinpath('riskfactors', str(self.row['cik']) + '.csv.gz')
        df_raw = pd.DataFrame(columns = ['risktext'], index = [self.row['dt_submitted']])

        if '%s.csv.gz' % self.row['cik'] in self.risk_files:
            df_raw = pd.read_csv(rf_f,
                                compression = 'gzip',
                                index_col = 0,
                                sep = '\t',
                                encoding = 'utf-8')

        df_raw.loc[self.row['dt_submitted']] = [out_txt]
        df_raw.to_csv(rf_f,
                compression = 'gzip',
                mode = 'w',
                sep='\t',
                encoding='utf-8')

    def getFilingLinks(self):
        """ 
        Get the XBRL link from the index file
        ...
        Returns
        ----------
         > A tuple of the full url for the files in question (xml link, html link)
        """
        response = api_call('https://www.sec.gov/Archives/' + self.row['url'], HEADERS, 'text')
        filing_html = BeautifulSoup(response, "lxml")

        htm_tds = filing_html.findAll('td', text= self.row['type'])
        html_link = self.getLinkFromHTML(htm_tds, 'htm')

        xml_tds = filing_html.findAll('td', text='EX-101.INS')

        if len(xml_tds) == 0:
            xml_tds = filing_html.findAll('td', text='XML')

        xbrl_link = self.getLinkFromHTML(xml_tds, 'xml')

        return urljoin("https://www.sec.gov", xbrl_link), urljoin("https://www.sec.gov", html_link)

    def getLinkFromHTML(self, tds, output):
        """ Get the correct link from the webpage and return the link based on the output
        ... 
        Parameters
        ----------
        tds: The embedded table structure to analyze
        output: the type of output we are searching for (XML, htm)
        ...
        Returns
        ----------
         > returns a link to the correct file
        """
        for td in tds:
            try:
                link = td.findPrevious('a', text=re.compile('\.%s$' % output))['href']
                if not link.startswith('/Archives/'):
                    link = '/Archives/' + link.split('/Archives/')[1]
            except :
                continue
            else:
                if not re.match(pattern='\d\.%s$' % output, string= link):
                    continue
                else:
                    break
        return link

    def _append_html_version(self, line):
        """ 
        Used to format the index location for all SEC filings
        ...
        Parameters
        ----------
        line: the location of the index file
        ...
        Returns
        ----------
          > The correctly formatted index line
        """
        chunks = line.split("|")
        return line + "|" + chunks[-1].replace(".txt", "-index.html")
