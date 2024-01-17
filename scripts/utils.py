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
from tqdm import tqdm  


def warc_getter(df):
    """
    Get /wet/ file (containing url's text) from /warc/
    """
    df['needed_warc'] = df['needed_warc'].str.replace('/warc/', '/wet/').replace('warc.gz', 'warc.wet.gz') 
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

def postcode_counter(text, geography = None): #REWRITE
    """
    Extract postcodes from regex, count how many times each postcode appears in a webpage and store the postcode + count in a dictionary 
    """
    postcodes = postcode_finder(text)
    if geography:
        webpage_postcodes = [i for i in postcodes if i.startswith(geography)] 
        dictionary_with_counts = {x:webpage_postcodes.count(x) for x in webpage_postcodes}
        return dictionary_with_counts
    else:
        webpage_postcodes = [postcode[1] for postcode in postcodes]
        dictionary_with_counts = {x:webpage_postcodes.count(x) for x in webpage_postcodes} 
        return dictionary_with_counts

def postcode_counter_webpage(df, geography = None):
    """
    Get dictionaries of postcodes + counts for webpage  
    """
    df['postcode'] = df['text'].apply(postcode_counter, geography = geography)
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
                    #print(multiprocessing.current_process())
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

def parallelize_df(df, function):
    n_cores = os.cpu_count()
    df_split = np.array_split(df, n_cores)
    with get_context('fork').Pool(processes = n_cores) as pool:
        df = pd.concat(pool.map(function, df_split))
        pool.close()
        pool.join()
    return df





