import pandas as pd 
import re

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
