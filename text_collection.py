import pandas as pd
import json
import requests
from warcio import ArchiveIterator
import smart_open

df = pd.read_pickle('/Users/gocchini/Desktop/CC project/tutorial_following/results01-02.pkl')

def text_getter(wet_file, url):

    r = requests.get(wet_file, stream = True)
    texts = []
    for record in ArchiveIterator(r.raw):
        if record.rec_headers.get_header('WARC-Target-URI') == url:
            if record.rec_type == 'conversion':
                text = record.content_stream().read() #with raw_stream it does not work, I get error 'LimitReader' object is not callable
                text = text.decode('utf-8')
                texts.append(text)

df['text'] = trial_df.apply(lambda x: text_getter(x['needed_warc_2'], x['url']), axis = 1)

#commit