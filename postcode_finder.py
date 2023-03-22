from functools import total_ordering
import pandas as pd
import requests
from warcio import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed
from warcio.statusandheaders import StatusAndHeadersParserException
import os 
import numpy as np 
from multiprocessing import set_start_method
from multiprocessing import get_context
from multiprocessing import pool 
import multiprocessing
from requests.adapters import HTTPAdapter
from requests.packages import urllib3
from requests.packages.urllib3.util.retry import Retry
from requests.exceptions import ConnectionError
from tqdm import tqdm 
import pickle 
import sys
import argparse 
import time
import re

df = pd.read_pickle('/Users/gocchini/Desktop/CC project/tutorial_following/results01-02.pkl')
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

    df['text'] = df.apply(lambda x: text_getter(x['needed_warc'], x['url']), axis = 1) #lol i am not calling this function.. 
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
        total=5, read=5, connect=5, backoff_factor=0.2, status=1, redirect=1, status_forcelist=None) #levi had put the backoff_factor at 0.2...
    adapter = HTTPAdapter(max_retries=retryer)
    adapter.max_retries.respect_retry_after_header = False #MOST IMPORTANT LINE!!!!
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    try:
        r = session.get(wet_file, stream = True, timeout= (5, 0.5)) #the problem might be in the downloaded file here!!!!!!!!!!! thats y it might changeeeee !!!!!!
    except OSError as exc: # dunno ig this makes it move to the next one? This part needs to be tested 
        if exc.errno == 55:
            print('[Errno 55] No buffer space available')
            time.sleep(0.1)
            # shall I break here? No cause I want to continue. And no raise. 
    "insert code for retrial...it does not look to me like it is rretrying? talk about iit with ry"
    """
    try here???? per il record, prova. Ma se non riconosce il record e inutile! Potrebbe darsi che parse come record il rsultato di r.rraw. Se r .raw e ugualew a xml, allora e tutto da buttare! 
    forse la soluzione e cercare di capire: 
    il problema e che non gli eriesce proprio a loadare quel record perche non lo sa interpretare!!!quindi il try dovcrebbe essere qui sopra!!!
    prova ad aprire il record, a meno che tu non lo riconosca!! ultima cosa che dico 23.43 credo proprio vada quii su. 
    23.53, anzi, magar non riesce proprio a fare ArchiiveIt(r,raw?) ha fallito a loadare l'archive, ma non credo sia il r....che e solo un respmnse...
    """
    try: 
        for record in ArchiveIterator(r.raw):        #cannot parse record header ,,,c;e qlc probl
            if record.rec_headers.get_header('WARC-Target-URI') == url: #the problem is when it parses the headers..could it be after this? 
                if record.rec_type == 'conversion': #so that this is the bad record type?
                    # with raw_stream it does not work, I get error 'LimitReader' object is not callable
                    text = record.content_stream().read() 
                    text = text.decode('utf-8')
                    print('done')
                    return text
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
    

def lambda_postcode_getter(df): 
    """
    Applying function to find texts and postcodes within the text. 
    As some text might be empty, we drop them or they would break the code
    """
    df = lambda_getter(df)
    df.dropna(subset = ['text'])
    df_text = df.mask(df.eq('None')).dropna(subset = 'text') #linea che rompe tutto eheh 
    results_df = postcode_counter_webpage(df_text)
    return results_df

def parallelize_df(df, function):
    n_cores = os.cpu_count()
    df_split = np.array_split(df, n_cores)
    with get_context('fork').Pool(processes = n_cores) as pool:
        df = pd.concat(pool.map(function, df_split))
        pool.close()
        pool.join()
    return df

def getter_and_saver(df, n_iterations): # TO DO: list comprehension for faster results ...should be fine? 
    # heavy on memory 
    results = pd.DataFrame()
    df_split = np.array_split(df, n_iterations)
    for df in tqdm(df_split):
        results = pd.concat([results, parallelize_df(df, lambda_postcode_getter)]) #GOOD LINE! 
        #delete extra stuff from here :)
        #to-do call aggr here 
        results_to_pickle = website_aggregator(results) 
        results_to_pickle.to_pickle('results_22_03.pkl')
    return results_to_pickle, results #WHY THE FUCK WAS I JUST RETURNING THE HEAD???? 

if __name__ == "__main__":


    trial_df = df[0:100]
    trial_df = warc_getter(trial_df) 
    results = getter_and_saver(trial_df, 5)
    print(results) #when number is low ValueError: Cannot set a DataFrame with multiple columns to the single column text. its becvause if i break again i have to do like at least 1000 columns 

    # parser = argparse.ArgumentParser()
    # parser.add_argument('--dataset_path', type=str, default='./')
