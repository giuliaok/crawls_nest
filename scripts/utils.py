import pandas as pd 
import re
import pandas as pd 
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Point
import seaborn
import glob
import utils
import requests
from requests.adapters import HTTPAdapter
from requests.packages import urllib3
from requests.packages.urllib3.util.retry import Retry
from requests.exceptions import ConnectionError
from tqdm import tqdm 
import warcio 
from warcio import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed
from warcio.statusandheaders import StatusAndHeadersParserException
import time
import re
import json 

def warc_getter(df):
    """
    Get /wet/ file (containing url's text) from /warc/
    """
    df['needed_warc'] = df['needed_warc'].str.replace('/warc/', '/wet/').replace('warc.gz', 'warc.wet.gz') 
    return df

def wat_getter(df):
    """
    Get /wet/ file (containing url's text) from /warc/
    """
    df['needed_wat'] = df['needed_warc'].str.replace('/warc/', '/wat/')
    df['needed_wat'] = df['needed_wat'].str.replace('wet.gz', 'wat.gz') 
    return df

def lambda_getter(df): 
    """
    Get text from warc files  
    """

    df['text'] = df.apply(lambda x: text_getter(x['needed_warc'], x['url']), axis = 1) 
    return df

def postcode_finder(text):
    """
    Find postocdes in text. Long regex was retrieved from ONS 
    """
    postcodes = re.findall(r'([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\s?[0-9][A-Za-z]{2})', text)
    return list(postcodes) 

def postcode_counter(text):
    """
    Extract postcodes from regex, count how many times each postcode appears in a webpage and store the postcode + count in a dictionary 
    """
    postcodes = postcode_finder(text)
    webpage_postcodes = [postcode[1] for postcode in postcodes]
    dictionary_with_counts = {x:webpage_postcodes.count(x) for x in webpage_postcodes}
    return dictionary_with_counts

def postcode_counter_webpage(df):
    """
    Get dictionaries of postcodes + counts for webpage  
    """
    df['postcode'] = df['text'].apply(postcode_counter)
    return df

def website_aggregator(df):
    df_group = df.groupby(['url_host_name']).agg({'postcode': list}).reset_index()
    df_group['website_postcodes'] = df_group['postcode'].apply(lambda x: {k: v for d in x for k, v in d.items()})
    df_group = df_group.drop(['postcode'], axis = 1)
    return df_group 

def text_getter(wet_file, url):
    session = requests.Session()
    retryer = Retry(
        total=5, read=5, connect=5, backoff_factor=0.2, status=1, redirect=1, status_forcelist=None) 
    adapter = HTTPAdapter(max_retries=retryer)
    adapter.max_retries.respect_retry_after_header = False 
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    try:
        r = session.get(wet_file, stream = True, timeout= (5, 2.5)) 
    except OSError as exc: 
        if exc.errno == 55:
            print('[Errno 55] No buffer space available')
            time.sleep(0.1)
            
    try: 
        for record in ArchiveIterator(r.raw):       
            if record.rec_headers.get_header('WARC-Target-URI') == url: 
                if record.rec_type == 'conversion': 
                    print(record.rec_type)
                    print(multiprocessing.current_process())
                    text = record.content_stream().read() 
                    text = text.decode('utf-8')
                    print('done')
                    return text
    except Exception as e:
        """
        The actual exceptions that we would like to catch here are: ArchiveLoadFailed, StatusAndHeadersParserException
        """
        #print(e, r)
        time.sleep(0.5)
        """
        Find way to append these exceptioins so they can be reused! 
        """
    session.close()

def lambda_arefs(df):
    df['arefs'] = df['html'].apply(get_arefs)
    return df 

def lambda_getter_html(df): 
    """
    Get html from wat files  
    """

    df['html'] = df.apply(lambda x: html_getter(x['needed_wat'], x['url']), axis = 1) 
    return df


def get_arefs(wat_content) -> list:
    """
    Extract A@/href urls from wat files 
    """
    arefs = []
    contents = wat_content['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links']
    for dictionary in contents:
        if 'path' in dictionary.keys():
            if dictionary['path'] == 'A@/href':
                arefs.append(dictionary['url'])
    return arefs


def html_getter(wat_file, url):
    session = requests.Session()
    retryer = Retry(
        total=5, read=5, connect=5, backoff_factor=0.2, status=1, redirect=1, status_forcelist=None) #levi had put the backoff_factor at 0.2...
    adapter = HTTPAdapter(max_retries=retryer)
    adapter.max_retries.respect_retry_after_header = False #MOST IMPORTANT LINE!!!!
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    try:
        r = session.get(wat_file, stream = True) #the problem might be in the downloaded file here!!!!!!!!!!! thats y it might changeeeee !!!!!!
    except OSError as exc: # dunno ig this makes it move to the next one? This part needs to be tested 
        if exc.errno == 55:
            print('[Errno 55] No buffer space available')
            time.sleep(0.1)
    try: 
        for record in ArchiveIterator(r.raw):  
            if record.rec_headers.get_header('WARC-Target-URI') == url: 
                if record.rec_type == 'metadata':
                    a = record.content_stream().read()
                    data = json.loads(a.decode('utf-8'))
                    if data['Envelope']['WARC-Header-Metadata']['WARC-Type'] == 'response': 
                        return data
    except Exception as e:
        """
        The actual exceptions that we would like to catch here are: ArchiveLoadFailed, StatusAndHeadersParserException
        """
        print(e, r)
        time.sleep(0.5)
        """
        Find way to append these exceptioins so they can be reused! 
        """
    session.close()
