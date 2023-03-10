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