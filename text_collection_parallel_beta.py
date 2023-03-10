
from functools import total_ordering
import pandas as pd
import json
import requests
from warcio import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed
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

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

df = pd.read_pickle('/Users/gocchini/Desktop/CC project/tutorial_following/results01-02.pkl')

def warc_getter(df):
    df['needed_warc'] = df['needed_warc'].str.replace('/warc/', '/wet/').replace('warc.gz', 'warc.wet.gz') 
    return df

def text_getter(wet_file, url):
    try:
        r = requests.get(wet_file, stream = True)
    except OSError as exc: # dunno ig this makes it move to the next one? This part needs to be tested 
        if exc.errno == 55:
            print('[Errno 55] No buffer space available')
            time.sleep(0.1)
            # shall I break here? No cause I want to continue. And no raise. 
    # print('got it')
    try: 
        for record in ArchiveIterator(r.raw):        
            if record.rec_headers.get_header('WARC-Target-URI') == url:
                if record.rec_type == 'conversion':
                    # with raw_stream it does not work, I get error 'LimitReader' object is not callable
                    text = record.content_stream().read() 
                    text = text.decode('utf-8')
                    print('done')
                    return text
    except ArchiveLoadFailed:
            print('Encountered a bad WARC record.', file=sys.stderr) # not completely sure this is working yet? 
    # no text is found so raise error
    # raise ValueError("text not found") #this breaks everything 


def lambda_getter(df): 
    df['text'] = df.apply(lambda x: text_getter(x['needed_warc'], x['url']), axis = 1)
    return df

# if you want to avoid the lambda function, use function below (not that I would)

def text_getter_non_lambda(df):
    session = requests.Session()
    retryer = Retry(
        total=5, read=5, connect=5, backoff_factor=0.2, status=1, redirect=1, status_forcelist=None)
    for index in df.index:
        wet_file = df.loc[index, 'needed_warc']
        url = df.loc[index, 'url']
        print(url)
        r = session.get(wet_file, stream = True)
        print('got it')
        for record in ArchiveIterator(r.raw):
            if record.rec_headers.get_header('WARC-Target-URI') == url:
                if record.rec_type == 'conversion':
                    # with raw_stream it does not work, I get error 'LimitReader' object is not callable
                    text = record.content_stream().read() 
                    text = text.decode('utf-8')
                    df.loc[index,'text'] = text
                    # need to add some break statements around here
    session.close()
    print('completed chunk')
    return df

def parallelize_df(df, function):
    n_cores = os.cpu_count()
    df_split = np.array_split(df, n_cores)
    with get_context('fork').Pool(processes = n_cores) as pool:
        df = pd.concat(pool.map(function, df_split))
        pool.close()
        pool.join()
    return df

# write code to catch warcio.exceptions.ArchiveLoadFailed: Unknown archive format, first line: ['<?xml', 'version="1.0"', 'encoding="UTF-8"?>'] - seems to happpen randomly 

'''
'''

# write code to pickle every n iterations, if you want to save the data. Not that I thnk you should 

def getter_and_saver(df, n_iterations): # TO DO: list comprehension for faster results 
    # heavy on memory 
    results_to_pickle = pd.DataFrame()
    df_split = np.array_split(df, n_iterations)
    for df in tqdm(df_split):
        results_to_pickle = pd.concat([results_to_pickle, parallelize_df(df, lambda_getter)])
        results_to_pickle.to_pickle('results_10_03.pkl')
    return results_to_pickle.head()

# add parameter to get out the NaN --> in vue of possible packaging 

# results_website = postcode_counter_website(df)

# use this to monitor the size of the dataframe! So that it does not get out of hand. 
def bytesto(bytes, to, bsize=1024): 
    a = {'k' : 1, 'm': 2, 'g' : 3, 't' : 4, 'p' : 5, 'e' : 6 }
    r = float(bytes)
    return bytes / (bsize ** a[to])
# would need to apply this after a certain number of iterations, using a line looking like 

def get_size(df):
    size = sys.getsizeof(df)
    size_in_gb = bytesto(size, 'g')
    return size_in_gb



if __name__ == '__main__':

    trial_df = df[0:1000]
    trial_df = warc_getter(trial_df) 
    print(trial_df)
    results = getter_and_saver(trial_df, 20)
    print(results) #when number is low ValueError: Cannot set a DataFrame with multiple columns to the single column text. its becvause if i break again i have to do like at least 1000 columns 
'''

    #set_start_method("fork")
    #res = text_getter_parallel(trial_df)

    #res = parallelize_df(trial_df, lambda_getter)
    #print(res)

'''
