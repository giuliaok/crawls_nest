import pandas as pd 
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

def wat_getter(df):
    """
    Get /wet/ file (containing url's text) from /warc/
    """
    df['needed_wat'] = df['needed_warc'].str.replace('/warc/', '/wat/')
    df['needed_wat'] = df['needed_wat'].str.replace('wet.gz', 'wat.gz') 
    return df 

def html_getter(wat_file, url):
    session = requests.Session()
    retryer = Retry(
        total=5, read=5, connect=5, backoff_factor=0.2, status=1, redirect=1, status_forcelist=None) 
    adapter = HTTPAdapter(max_retries=retryer)
    adapter.max_retries.respect_retry_after_header = False #MOST IMPORTANT LINE!!!!
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    try:
        r = session.get(wat_file, stream = True) 
    except OSError as exc:  
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