
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
    r = requests.get(wet_file, stream = True)
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
        results_to_pickle.to_pickle('trial_28_02.pkl')
    return results_to_pickle.head()

#still saves just the last one, write better for loop. But in generak it works heheh. Also nicely with tqdm

def postcode_finder(text):
    postcodes = re.findall(r'([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\s?[0-9][A-Za-z]{2})', text)
    return list(postcodes) 

def lambda_postcode(df):
    df['postcode'] = df.apply(lambda x: postcode_finder(x['text']), axis = 1)
    return df 


if __name__ == '__main__':

    trial_df = df[0:100]
    trial_df = warc_getter(trial_df)
    print(trial_df)
    results = getter_and_saver(trial_df, 2)
    print(results)
    #set_start_method("fork")
    #res = text_getter_parallel(trial_df)

    #res = parallelize_df(trial_df, lambda_getter)
    #print(res)


