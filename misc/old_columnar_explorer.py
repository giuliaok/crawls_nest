import pandas as pd
import json
import requests
from warcio import ArchiveIterator
import gzip
import re
import os 
import ast
from tqdm import tqdm 
import numpy
import requests
from requests.adapters import HTTPAdapter
from requests.packages import urllib3
from requests.packages.urllib3.util.retry import Retry
import time
import os
import numpy
import multiprocessing
from multiprocessing import set_start_method
from multiprocessing import get_context
from functools import reduce
from multiprocessing import Pool, cpu_count
import pickle



def get_monthly_indices(link):
    index_paths = requests.get(link)
    open('monthly_index.gz', 'wb').write(index_paths.content)
    links_for_download_cdx = []
    with gzip.open('monthly_index.gz', 'rb') as f:
        for line in f:
            links_for_download_cdx.append(line)
    os.remove('monthly_index.gz')
    links_for_download_cdx_strings = [str(byte_string, 'UTF-8').rstrip('\n') for byte_string in links_for_download_cdx]
    links_to_get_indices = ['https://data.commoncrawl.org/' + x for x in links_for_download_cdx_strings] 
    return(links_to_get_indices)

def get_couk(links_to_get_indices_for_script):
    co_uk = pd.DataFrame()
    for link_of_indices in tqdm(links_to_get_indices_for_script):
        link_to_download = 'curl -o columnar.gz.parquet ' + ''.join(link_of_indices) 
        os.system(link_to_download)
        df = pd.read_parquet('columnar.gz.parquet', engine='fastparquet')
        co_uk_data = df.loc[df['url_host_private_suffix'] == 'co.uk'] #funzia con .com!'
        print('quanti?')
        print(len(co_uk_data))
        co_uk = pd.concat([co_uk, co_uk_data], axis=0).reset_index(drop=True)
        print(len(co_uk))
        os.remove('columnar.gz.parquet')
    return(co_uk)


results = pd.DataFrame()
august = 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-33/cc-index-table.paths.gz'
indices = get_monthly_indices(august)
chunks_of_indices = numpy.array_split(indices, 9)
my_chunks = chunks_of_indices[6:9]
print(my_chunks)
for chunk in my_chunks:
    print(len(chunk))
    results = pd.concat([results, get_couk(chunk)])
    results = results.drop(['url_surtkey', 'url_host_tld', 'url_host_2nd_last_part', 'url_host_3rd_last_part', 
                            'url_host_4th_last_part', 'url_host_5th_last_part', 'url_host_name_reversed', 'url_protocol', 
                            'url_port', 'url_path', 'url_query', 'content_digest', 'content_mime_detected'], axis = 1) 	
    results = results.loc[(results['fetch_status'] == 200)]
    print('slow_part - remove txt')
    #also maybe try to understand y these two lines below give me warnings? thanks, Giulia from the past
    results['needed_warc'] = 'https://data.commoncrawl.org/' + results['warc_filename'].astype(str)
    results['needed_warc'] = results['needed_warc'].str.replace('/warc/', '/wet/').replace('warc.gz', 'warc.wet.gz') 
    results = results[~results.warc_filename.str.contains('robotstxt')] 
    results.to_pickle('/Users/gocchini/Desktop/CC project/tutorial_following/results01-02.pkl')
    print(results.tail())
    print(len(results))

#commit