
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
import argparse 
import re

df = pd.read_pickle('/Users/gocchini/Desktop/CC project/tutorial_following/results01-02.pkl')

def warc_getter(df):
    df['needed_warc'] = df['needed_warc'].str.replace('/warc/', '/wet/').replace('warc.gz', 'warc.wet.gz') 
    return df

def lambda_getter(df): 
    df['text'] = df.apply(lambda x: text_getter(x['needed_warc'], x['url']), axis = 1)
    return df

def postcode_finder(text):
    postcodes = re.findall(r'([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\s?[0-9][A-Za-z]{2})', text)
    return list(postcodes) 

def postcode_counter(text):
    postcodes = postcode_finder(text)
    webpage_postcodes = [postcode[1] for postcode in postcodes]
    dictionary_with_counts = {x:webpage_postcodes.count(x) for x in webpage_postcodes}
    return dictionary_with_counts

def counter_webpage(df):
    df['postcode'] = df['text'].apply(postcode_counter)
    return df

def postcode_counter_website(df):
    df['postcode'] = df['text'].apply(postcode_counter)
    df = df.groupby(['url_host_name']).agg({'postcode': list}).reset_index()
    df['website_postcodes'] = df['postcode'].apply(lambda x: {k: v for d in x for k, v in d.items()})
    df = df.drop(['postcode'], axis = 1)
    return df 

def text_postcode_getter(wet_file, url):
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

def lambda_getter(df): 
    df['text'] = df.apply(lambda x: text_postcode_getter(x['needed_warc'], x['url']), axis = 1)
    #df = df.dropna(['text'])
    #df = postcode_counter_website(df)
    return df

def parallelize_df(df, function):
    n_cores = os.cpu_count()
    df_split = np.array_split(df, n_cores)
    with get_context('fork').Pool(processes = n_cores) as pool:
        df = pd.concat(pool.map(function, df_split))
        pool.close()
        pool.join()
    return df

def getter_and_saver(df, n_iterations): # TO DO: list comprehension for faster results 
    # heavy on memory 
    results_to_pickle = pd.DataFrame()
    df_split = np.array_split(df, n_iterations)
    for df in tqdm(df_split):
        results_to_pickle = pd.concat([results_to_pickle, parallelize_df(df, lambda_getter)])
        results_to_pickle.to_pickle('results_10_03.pkl')
    return results_to_pickle.head()

if __name__ == "__main__":

    trial_df = df[0:1000]
    trial_df = warc_getter(trial_df) 
    results = getter_and_saver(trial_df, 10)
    print(results) #when number is low ValueError: Cannot set a DataFrame with multiple columns to the single column text. its becvause if i break again i have to do like at least 1000 columns 

    # parser = argparse.ArgumentParser()
    # parser.add_argument('--dataset_path', type=str, default='./')